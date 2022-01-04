#define _GNU_SOURCE

#include <dirent.h>
#include <dirent.h>
#include <fcntl.h>
#include <linux/stat.h>
#include <pthread.h>
#include <stdlib.h>
#include <string.h>
#include <sys/eventfd.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>
#include "wiredtiger.h"
#include "wiredtiger_ext.h"
#include "liburing.h"

static const char *home;
static const char *config;

#define EVENT_TYPE_SHUTDOWN 0
#define EVENT_TYPE_NORMAL   1
#define EVENT_TYPE_LINKED   2
#define CQE_BATCH_SIZE      16

/*
* A wrapper struct to be used with io_uring SQEs and CQEs.
*/
typedef struct __ring_event_user_data {
    int event_type;
    int lock_flag; // indicator to main thread to unblock

    // might just use posix cond vars ... or copy over WT's wr_condvar.
    // tbh just need something that looks like a latch for now ...
    pthread_mutex_t mutex;
    pthread_cond_t condvar;

    // cqe->res value
    int ret_code;
} RING_EVENT_USER_DATA;


/* ! [JEB :: FILE_SYSTEM] */
typedef struct __jeb_file_handle {
    WT_FILE_SYSTEM iface;

    // one ring to rule them all ...
    struct io_uring ring;
    
    // eventfd used in conjunction with the uring
    int efd;
    
    // a background thread that reads CQEs off the uring and notifies blocked callers
    pthread_t uring_consumer; 

    WT_EXTENSION_API *wtext;
} JEB_FILE_SYSTEM;


typedef struct jeb_file_handle {
    WT_FILE_HANDLE iface;

    /* pointer to the enclosing file system */
    JEB_FILE_SYSTEM *fs;

    int fd;

} JEB_FILE_HANDLE;

/* 
* initialization function; must be invoked at WT_CONNECTION creation, 
* via the extensions sxtring config
*/
int create_custom_file_system(WT_CONNECTION *, WT_CONFIG_ARG *);

/*
* Forward function declarations for file system API.
*/
static int jeb_fs_open(
  WT_FILE_SYSTEM *, WT_SESSION *, const char *, WT_FS_OPEN_FILE_TYPE, uint32_t, WT_FILE_HANDLE **);
static int jeb_fs_directory_list(
  WT_FILE_SYSTEM *, WT_SESSION *, const char *, const char *, char ***, uint32_t *);
static int jeb_fs_directory_list_free(WT_FILE_SYSTEM *, WT_SESSION *, char **, uint32_t);
static int jeb_fs_exist(WT_FILE_SYSTEM *, WT_SESSION *, const char *, bool *);
static int jeb_fs_remove(WT_FILE_SYSTEM *, WT_SESSION *, const char *, uint32_t);
static int jeb_fs_rename(WT_FILE_SYSTEM *, WT_SESSION *, const char *, const char *, uint32_t);
static int jeb_fs_size(WT_FILE_SYSTEM *, WT_SESSION *, const char *, wt_off_t *);
static int jeb_fs_terminate(WT_FILE_SYSTEM *, WT_SESSION *);

/*
* Forward function declarations for file handle API.
*/
static int jeb_fh_close(WT_FILE_HANDLE *, WT_SESSION *);
static int jeb_fh_extend(WT_FILE_HANDLE *, WT_SESSION *, wt_off_t offset);
static int jeb_fh_extend_nolock(WT_FILE_HANDLE *, WT_SESSION *, wt_off_t offset);
static int jeb_fh_lock(WT_FILE_HANDLE *, WT_SESSION *, bool);
static int jeb_fh_read(WT_FILE_HANDLE *, WT_SESSION *, wt_off_t, size_t, void *);
static int jeb_fh_size(WT_FILE_HANDLE *, WT_SESSION *, wt_off_t *);
static int jeb_fh_sync(WT_FILE_HANDLE *, WT_SESSION *);
static int jeb_fh_sync_nowait(WT_FILE_HANDLE *, WT_SESSION *);
static int jeb_fh_truncate(WT_FILE_HANDLE *, WT_SESSION *, wt_off_t);
static int jeb_fh_write(WT_FILE_HANDLE *, WT_SESSION *, wt_off_t, size_t, const void *);

static int jeb_fh_map(WT_FILE_HANDLE *, WT_SESSION *, void *, size_t *, void *);
static int jeb_fh_map_discard(WT_FILE_HANDLE *, WT_SESSION *, void *, size_t, void *);
static int jeb_fh_map_preload(WT_FILE_HANDLE *, WT_SESSION *, const void *, size_t, void *);
static int jeb_fh_unmap(WT_FILE_HANDLE *, WT_SESSION *, void *, size_t, void *);

/*
FILE_HANDLE functions not currently defined:

fh_advise
fh_extend
fh_extend_nolock
*/

int init_io_uring(struct io_uring *ring, int efd) {
    struct io_uring_params params;

    if (geteuid()) {
        fprintf(stderr, "You need root privileges to run this program.\n");
        return 1;
    }
    memset(&params, 0, sizeof(struct io_uring_params));
    params.flags |= IORING_SETUP_SQPOLL;

    // TODO: make idle time a config option?
    params.sq_thread_idle = 120000; // 2 minutes in ms;

    // TODO: make queue depth a config option?
    int ret = io_uring_queue_init_params(16, ring, &params);
    if (ret) {
        fprintf(stderr, "unable to setup uring: %s\n", strerror(-ret));
        return 1;
    }
    io_uring_register_eventfd(ring, efd);

    return 0;
}

/*
* function exxecuted by a bg thread to block on the eventfd, and when it awakens,
* check the ring for CQEs. For each CQE available, poke the "lock"
* in the user_data to awaken the blocked read/write thread.
*
* This thread will not free any memory.
*/
void *ring_consumer(void *data) {
    struct io_uring_cqe *cqes[CQE_BATCH_SIZE];
    struct io_uring_cqe *cqe;
    JEB_FILE_SYSTEM *fs = (JEB_FILE_SYSTEM *) data;
    RING_EVENT_USER_DATA *ud;
    eventfd_t v;

    bool must_exit = false;

    while (!must_exit) {
        // this blocks forever ... i think :(
        int ret = eventfd_read(fs->efd, &v);
        if (ret < 0)
            // TODO: find some better way to handle this error
            exit(1);

        // TODO: there's also io_uring_for_each_cqe()

        // make sure we get all the CQEs that are ready, else we won't get 
        // re-notified from the eventfd blocking
        while (1) {
            int cnt = io_uring_peek_batch_cqe(&fs->ring, cqes, CQE_BATCH_SIZE);
            if (!cnt) {
                // not sure when we'd get 0 count if the eventfd triggered, unless spurious :shrug:
                break;
            }
//            printf("JEB::ring_consumer - next batch count = %d\n", cnt);
            
            cqe = *cqes;
            for (int i = 0; i < cnt; i++, cqe++) {
                ud = io_uring_cqe_get_data(cqe);
                // NOTE: there could be some nasty thread blocks here if the timing is unlucky 
                // (if the producing thread hasn't called pthread_cond_wait() yet).
                pthread_mutex_lock(&ud->mutex); // <- i think i need to do this, but not sure yet (or do it at submission??)
                ud->ret_code = cqe->res;
                pthread_cond_signal(&ud->condvar);
                pthread_mutex_unlock(&ud->mutex);

                if (ud->event_type == EVENT_TYPE_SHUTDOWN) {
                    must_exit = true;
                }

                // TODO: see if there's a way to batch update the pointer here, instead of doing it one at a time.
                // io_uring_for_each_cqe() has a helper function to do that, i think ....
                io_uring_cqe_seen(&fs->ring, cqe);
            }
        }
    }

    return (NULL);
}

/*
* Initialization function for the custom file system/handle.
*/
int create_custom_file_system(WT_CONNECTION *conn, WT_CONFIG_ARG *config) {
    JEB_FILE_SYSTEM *fs;
    WT_EXTENSION_API *wtext;
    WT_FILE_SYSTEM *file_system;
    // struct io_uring *ring = NULL;
    int ret = 0;
    int efd;

    wtext = conn->get_extension_api(conn);

    if ((fs = calloc(1, sizeof(JEB_FILE_SYSTEM))) == NULL) {
        (void)wtext->err_printf(wtext, NULL, "failed to allocate custom file system: %s",
                wtext->strerror(wtext, NULL, ENOMEM));
        return (ENOMEM);
    }

    fs->wtext = wtext;
    file_system = (WT_FILE_SYSTEM *)fs;

    // NOTE: this is where we can parse the config string to set values into the custom FS,
    // but I'm omitting for now (not sure if i need ....)

    file_system->fs_directory_list = jeb_fs_directory_list;
    file_system->fs_directory_list_free = jeb_fs_directory_list_free;
    file_system->fs_exist = jeb_fs_exist;
    file_system->fs_open_file = jeb_fs_open;
    file_system->fs_remove = jeb_fs_remove;
    file_system->fs_rename = jeb_fs_rename;
    file_system->fs_size = jeb_fs_size;
    file_system->terminate = jeb_fs_terminate;

    // now, set up the uring
    efd = eventfd(0, 0);
    if (efd < 0) {
        (void)wtext->err_printf(wtext, NULL, "failed to create eventfd: %s",
                wtext->strerror(wtext, NULL, efd));
        free(fs);
        exit (1);
    }
    fs->efd = efd;
    if ((ret = init_io_uring(&fs->ring, efd)) != 0) {
        (void)wtext->err_printf(wtext, NULL, "failed to create uring: %s",
                wtext->strerror(wtext, NULL, ret));
        free(fs);
        exit(1);
    }

    if ((ret = pthread_create(&fs->uring_consumer, NULL, ring_consumer, (void *)fs)) != 0) {
        (void)wtext->err_printf(wtext, NULL, "failed to create uring: %s",
                wtext->strerror(wtext, NULL, ret));
        // TODO: probably need better clean up code, esp after init'ing the uring
        free(fs);
        exit(1);
    }

    printf("JEB::create_custom_file_system about to set FS into the connection\n");
    if ((ret = conn->set_file_system(conn, file_system, NULL)) != 0) {
        (void)wtext->err_printf(wtext, NULL, "WT_CONNECTION.set_file_system: %s", 
                wtext->strerror(wtext, NULL, ret));
        free(fs);
        exit(1);
    }

    printf("JEB::create_custom_file_system successfully set up custom file system!\n");
    return (0);
}

static int 
jeb_fs_open(WT_FILE_SYSTEM *fs, WT_SESSION *session, const char *name , 
    WT_FS_OPEN_FILE_TYPE file_type, uint32_t flags , WT_FILE_HANDLE **file_handlep) {
    JEB_FILE_HANDLE *jeb_file_handle;
    JEB_FILE_SYSTEM *jeb_fs;
    WT_FILE_HANDLE *file_handle;
    struct io_uring_sqe *sqe;
    int ret = 0;
    int open_flags = 0, fd = 0, mode = 0;

    printf("JEB::jeb_fs_open %s\n", name);

    (void)flags; /* ignored for now */
    (void)file_type; /* unused */

    *file_handlep = NULL;
    jeb_fs = (JEB_FILE_SYSTEM *)fs;
    jeb_file_handle = NULL;

    if (file_type == WT_FS_OPEN_FILE_TYPE_DIRECTORY) {
        mode = 0444;
        open_flags = O_RDONLY | O_CLOEXEC;
    } else {
        // assume regular file ....
        open_flags = flags & WT_FS_OPEN_READONLY ? O_RDONLY : O_RDWR;
        open_flags |= O_CLOEXEC;
        if (flags & WT_FS_OPEN_CREATE) {
            open_flags |= O_CREAT;
            if (flags | WT_FS_OPEN_EXCLUSIVE) 
                open_flags |= O_EXCL;
            mode = 0644;
        } else {
            mode = 0;
        }
    }

    // NOTE: WT sets the O_DSYNC flag on log (WAL) files. not sure if that's totally cool with io_uring,
    // but I didn't look very hard: https://lore.kernel.org/all/CAF-ewDoqyx5knsnd_qgfRXE+CxK==PO1zF+RE=oEuv9NQq+48g@mail.gmail.com/T/

    sqe = io_uring_get_sqe(&jeb_fs->ring);
    io_uring_prep_openat(sqe, 0, name, open_flags, mode);
    RING_EVENT_USER_DATA *ud = malloc(sizeof(RING_EVENT_USER_DATA));
    ud->event_type = EVENT_TYPE_NORMAL;
    
    /* Initialize mutex and condition variable objects */
    pthread_mutex_init(&ud->mutex, NULL);
    pthread_cond_init (&ud->condvar, NULL);
    pthread_mutex_lock(&ud->mutex);

    io_uring_sqe_set_data(sqe, ud);
    io_uring_submit(&jeb_fs->ring);
    
    // this should be in a loop to deal with spurious wakeups.
    // should probably use pthread_cond_timedwait().
    pthread_cond_wait(&ud->condvar, &ud->mutex);
    pthread_mutex_unlock(&ud->mutex);
    pthread_mutex_destroy(&ud->mutex);
    pthread_cond_destroy(&ud->condvar);

    fd = ud->ret_code;
    free(ud);

    if (fd < 0) {
        fprintf(stderr, "failed to create a new file: %s, err: %s\n", name, strerror(ret));
        return ret;
    }

    // WT does some fadvise stuff, as well. ignoring for now

    if ((jeb_file_handle = calloc(1, sizeof(JEB_FILE_HANDLE))) == NULL) {
        ret = ENOMEM;
        // goto err?????
    }

    jeb_file_handle->fs = jeb_fs;
    jeb_file_handle->fd = fd;

    file_handle = (WT_FILE_HANDLE *)jeb_file_handle;
    file_handle->file_system = fs;
    if ((file_handle->name = strdup(name)) == NULL) {
        ret = ENOMEM;
        // goto err;
    }

    // TODO: when we support mmap, update the function pointers below
    file_handle->close = jeb_fh_close;
    file_handle->fh_advise = NULL;
    file_handle->fh_extend = jeb_fh_extend;
    file_handle->fh_extend_nolock = jeb_fh_extend_nolock;
    file_handle->fh_lock = jeb_fh_lock;
    file_handle->fh_map = jeb_fh_map;
    file_handle->fh_map_discard = jeb_fh_map_discard;
    file_handle->fh_map_preload = jeb_fh_map_preload;
    file_handle->fh_read = jeb_fh_read;
    file_handle->fh_size = jeb_fh_size;
    file_handle->fh_sync = jeb_fh_sync;
    file_handle->fh_sync_nowait = jeb_fh_sync_nowait;
    file_handle->fh_truncate = jeb_fh_truncate;
    file_handle->fh_unmap = jeb_fh_unmap;
    file_handle->fh_write = jeb_fh_write;

    *file_handlep = file_handle;

    return (ret);
}

/* return if file exists */
static int 
jeb_fs_exist(WT_FILE_SYSTEM *fs, WT_SESSION *session, const char *name, bool *existp) {
    JEB_FILE_SYSTEM *jeb_fs;
    struct statx statx;
    struct io_uring_sqe *sqe;
    int ret = 0;

    jeb_fs = (JEB_FILE_SYSTEM *)fs;
    sqe = io_uring_get_sqe(&jeb_fs->ring);
    io_uring_prep_statx(sqe, 0, name, 0, 0, &statx);
    RING_EVENT_USER_DATA *ud = malloc(sizeof(RING_EVENT_USER_DATA));
    ud->event_type = EVENT_TYPE_NORMAL;
    
    /* Initialize mutex and condition variable objects */
    pthread_mutex_init(&ud->mutex, NULL);
    pthread_cond_init (&ud->condvar, NULL);
    pthread_mutex_lock(&ud->mutex);

    io_uring_sqe_set_data(sqe, ud);
    io_uring_submit(&jeb_fs->ring);

    // this should be in a loop to deal with spurious wakeups.
    // should probably use pthread_cond_timedwait().
    pthread_cond_wait(&ud->condvar, &ud->mutex);
    pthread_mutex_unlock(&ud->mutex);
    pthread_mutex_destroy(&ud->mutex);
    pthread_cond_destroy(&ud->condvar);
    ret = ud->ret_code;

    free(ud);
    if (ret == 0) {
        *existp = true;
        return (0);
    }
    if (-ret == ENOENT) {
        *existp = false;
        return (0);
    }

    return ret;
}

/* POSIX remove */
static int 
jeb_fs_remove(WT_FILE_SYSTEM *fs, WT_SESSION *session, const char *name, uint32_t flags) {
    // io_uring doesn't support unlink(), so copying WT's __posix_fs_remove().
    // NOTE: completely ignoring that we'd need to flush the parent directory(ies), as well,
    // cuz this is PoC.

    printf("JEB::jeb_fs_remove %s\n", name);
    int ret = 0;

    /*
     * ISO C doesn't require rename return -1 on failure or set errno (note POSIX 1003.1 extends C
     * with those requirements). Be cautious, force any non-zero return to -1 so we'll check errno.
     * We can still end up with the wrong errno (if errno is garbage), or the generic WT_ERROR
     * return (if errno is 0), but we've done the best we can.
     */
    if ((ret = unlink(name)) != 0) {
        fprintf(stderr, "failed remove (delete) %s, err: %s\n", name, strerror(ret));
        return ret;
    }
    return 0;
}

static int 
jeb_fs_rename(WT_FILE_SYSTEM *fs , WT_SESSION *session, const char *from, const char *to, uint32_t flags) {
    int ret = 0;
    
    printf("JEB::jeb_fs_rename - from: %s => %s\n", from, to);
    // io_uring doesn't support rename(), so copying WT's __posix_fs_rename().
    // NOTE: completely ignoring that we'd need to flush the parent directory(ies), as well,
    // cuz this is PoC.
    /*
     * ISO C doesn't require rename return -1 on failure or set errno (note POSIX 1003.1 extends C
     * with those requirements). Be cautious, force any non-zero return to -1 so we'll check errno.
     * We can still end up with the wrong errno (if errno is garbage), or the generic WT_ERROR
     * return (if errno is 0), but we've done the best we can.
     */
    if ((ret = rename(from, to)) != 0) {
        fprintf(stderr, "failed rename %s to %s, err: %s\n", from, to, strerror(ret));
        return ret;
    }
    return 0;
}

/* get the size of file in bytes */
static int 
jeb_fs_size(WT_FILE_SYSTEM *fs, WT_SESSION *session, const char *name, wt_off_t *sizep) {
    // NOTE: almost exactly the same as jeb_fh_size() - only diff is args to stax()
    JEB_FILE_SYSTEM *jeb_fs;
    struct statx statx;
    struct io_uring_sqe *sqe;
    int ret = 0;

    printf("JEB::jeb_fs_size %s\n", name);
    jeb_fs = (JEB_FILE_SYSTEM *)fs;
    sqe = io_uring_get_sqe(&jeb_fs->ring);
    io_uring_prep_statx(sqe, 0, name, 0, 0, &statx);
    RING_EVENT_USER_DATA *ud = malloc(sizeof(RING_EVENT_USER_DATA));
    ud->event_type = EVENT_TYPE_NORMAL;
    
    /* Initialize mutex and condition variable objects */
    pthread_mutex_init(&ud->mutex, NULL);
    pthread_cond_init (&ud->condvar, NULL);
    pthread_mutex_lock(&ud->mutex);

    io_uring_sqe_set_data(sqe, ud);
    io_uring_submit(&jeb_fs->ring);

    // this should be in a loop to deal with spurious wakeups.
    // should probably use pthread_cond_timedwait().
    pthread_cond_wait(&ud->condvar, &ud->mutex);
    pthread_mutex_unlock(&ud->mutex);
    pthread_mutex_destroy(&ud->mutex);
    pthread_cond_destroy(&ud->condvar);
    ret = ud->ret_code;

    free(ud);
    if (ret == 0) {
        *sizep = statx.stx_size;
        return (0);
    }

    return ret;
}

/* Check if a string matches a prefix. */
#define JEB_PREFIX_MATCH(str, pfx) \
    (((const char *)(str))[0] == ((const char *)(pfx))[0] && strncmp(str, pfx, strlen(pfx)) == 0)

/* return a list of files in a given sub-directory */
static int 
jeb_fs_directory_list(WT_FILE_SYSTEM *fs, WT_SESSION *session, const char *directory, 
    const char *prefix, char ***dirlistp, uint32_t *countp) {
    // io_uring doesn't support syscalls to list a directory.
    // thus, copying WT's__directory_list_worker()

    struct dirent *dp;
    DIR *dirp;
    // size_t dirallocsz;
    uint32_t count;
    char **entries;
    int ret = 0;

    *dirlistp = NULL;
    *countp = 0;
    dirp = NULL;
    // dirallocsz = 0;
    entries = NULL;

    printf("JEB::jeb_fs_directory_list - dir: %s, prefix: %s\n", directory, prefix);

    /*
     * If opendir fails, we should have a NULL pointer with an error value, but various static
     * analysis programs remain unconvinced, check both.
     */
    if ((dirp = opendir(directory)) == NULL || errno != 0) {
        ret = EINVAL;
    }

    for (count = 0; (dp = readdir(dirp)) != NULL;) {
        /*
         * Skip . and ..
         */
        if (strcmp(dp->d_name, ".") == 0 || strcmp(dp->d_name, "..") == 0)
            continue;

        /* The list of files is optionally filtered by a prefix. */
        if (prefix != NULL && !JEB_PREFIX_MATCH(dp->d_name, prefix))
            continue;

        // TODO: implement me

        // WT_ERR(__wt_realloc_def(session, &dirallocsz, count + 1, &entries));
        // WT_ERR(__wt_strdup(session, dp->d_name, &entries[count]));
        // ++count;
    }

    *dirlistp = entries;
    *countp = count;
    return ret;

// err:
//     WT_SYSCALL(closedir(dirp), tret);
//     if (tret != 0) {
//         __wt_err(session, tret, "%s: directory-list: closedir", directory);
//         if (ret == 0)
//             ret = tret;
//     }

//     if (ret == 0)
//         return (0);

//     WT_TRET(__wt_posix_directory_list_free(file_system, wt_session, entries, count));

//     WT_RET_MSG(
//       session, ret, "%s: directory-list, prefix \"%s\"", directory, prefix == NULL ? "" : prefix);
}

/* free memory allocated by jeb_fs_directory_list */
static int 
jeb_fs_directory_list_free(WT_FILE_SYSTEM *fs, WT_SESSION *session, char **dirlist, uint32_t count) {
    printf("JEB::jeb_fs_directory_list_free\n");

    // TODO: implement me

    if (dirlist != NULL) {
        while (count > 0) {
        //     __wt_free(session, dirlist[--count]);
        }
        // __wt_free(session, dirlist);
    }
    return 0;
}

/* 
* discard any resources on termination.
* send the ring_consumer thread a special message to tell it to 
* stop blocking on the uring. then shutdown the uring.
*/
static int 
jeb_fs_terminate(WT_FILE_SYSTEM *fs, WT_SESSION *session) {
    JEB_FILE_SYSTEM *jeb_fs;
    struct io_uring_sqe *sqe;
    int ret = 0;

    jeb_fs = (JEB_FILE_SYSTEM *)fs;

    printf("JEB::jeb_fs_terminate HEAD\n");
    sqe = io_uring_get_sqe(&jeb_fs->ring);
    io_uring_prep_nop(sqe);
    RING_EVENT_USER_DATA *ud = malloc(sizeof(RING_EVENT_USER_DATA));
    ud->event_type = EVENT_TYPE_SHUTDOWN;

    /* Initialize mutex and condition variable objects */
    pthread_mutex_init(&ud->mutex, NULL);
    pthread_cond_init (&ud->condvar, NULL);
    pthread_mutex_lock(&ud->mutex);

    io_uring_sqe_set_data(sqe, ud);
    io_uring_submit(&jeb_fs->ring);

    // this should be in a loop to deal with spurious wakeups.
    // should probably use pthread_cond_timedwait().
    pthread_cond_wait(&ud->condvar, &ud->mutex);
    pthread_mutex_unlock(&ud->mutex);
    pthread_mutex_destroy(&ud->mutex);
    pthread_cond_destroy(&ud->condvar);
    free(ud);

    io_uring_queue_exit(&jeb_fs->ring);

    if ((ret = close(jeb_fs->efd)) != 0) {
        fprintf(stderr, "problem closing eventd used with io_uring. ignoring but error is %s\n", strerror(ret));
    }

    return (0);
}
/* ! [JEB :: FILE_SYSTEM] */

/* ! [JEB :: FILE HANDLE] */
static int 
jeb_fh_close(WT_FILE_HANDLE *file_handle, WT_SESSION *session) {
    JEB_FILE_HANDLE *jeb_file_handle;
    JEB_FILE_SYSTEM *jeb_fs;
    struct io_uring_sqe *sqe;
    int ret = 0;

    jeb_file_handle = (JEB_FILE_HANDLE *)file_handle;
    jeb_fs = jeb_file_handle->fs;

    printf("JEB::jeb_fh_close - %s\n", file_handle->name);
    sqe = io_uring_get_sqe(&jeb_fs->ring);
    io_uring_prep_close(sqe, jeb_file_handle->fd);
    RING_EVENT_USER_DATA *ud = malloc(sizeof(RING_EVENT_USER_DATA));
    ud->event_type = EVENT_TYPE_NORMAL;

    /* Initialize mutex and condition variable objects */
    pthread_mutex_init(&ud->mutex, NULL);
    pthread_cond_init (&ud->condvar, NULL);
    pthread_mutex_lock(&ud->mutex);

    io_uring_sqe_set_data(sqe, ud);
    io_uring_submit(&jeb_fs->ring);

    // this should be in a loop to deal with spurious wakeups.
    // should probably use pthread_cond_timedwait().
    pthread_cond_wait(&ud->condvar, &ud->mutex);
    pthread_mutex_unlock(&ud->mutex);
    pthread_mutex_destroy(&ud->mutex);
    pthread_cond_destroy(&ud->condvar);
    ret = ud->ret_code;

    free(ud);
    return ret;
}

static int 
jeb_fh_extend(WT_FILE_HANDLE *file_handle, WT_SESSION *session, wt_off_t offset) {
    JEB_FILE_HANDLE *jeb_file_handle;
    JEB_FILE_SYSTEM *jeb_fs;
    struct io_uring_sqe *sqe;
    int ret = 0;

    jeb_file_handle = (JEB_FILE_HANDLE *)file_handle;
    jeb_fs = jeb_file_handle->fs;

    // TODO: there's a bunch of extra logic around the extend() functions in WT
    // wrt mapping the file (to prevent races). will need to account for that  ...

    printf("JEB::jeb_fh_extend - %s\n", file_handle->name);
    sqe = io_uring_get_sqe(&jeb_fs->ring);
    io_uring_prep_fallocate(sqe, jeb_file_handle->fd, 0, (wt_off_t)0, offset);
    RING_EVENT_USER_DATA *ud = malloc(sizeof(RING_EVENT_USER_DATA));
    ud->event_type = EVENT_TYPE_NORMAL;

    /* Initialize mutex and condition variable objects */
    pthread_mutex_init(&ud->mutex, NULL);
    pthread_cond_init (&ud->condvar, NULL);
    pthread_mutex_lock(&ud->mutex);

    io_uring_sqe_set_data(sqe, ud);
    io_uring_submit(&jeb_fs->ring);

    // this should be in a loop to deal with spurious wakeups.
    // should probably use pthread_cond_timedwait().
    pthread_cond_wait(&ud->condvar, &ud->mutex);
    pthread_mutex_unlock(&ud->mutex);
    pthread_mutex_destroy(&ud->mutex);
    pthread_cond_destroy(&ud->condvar);
    ret = ud->ret_code;

    free(ud);
    return ret;
}

static int 
jeb_fh_extend_nolock(WT_FILE_HANDLE *file_handle, WT_SESSION *session, wt_off_t offset) {
    return jeb_fh_extend(file_handle, session, offset);
}

/* lock/unlock a file */
static int 
jeb_fh_lock(WT_FILE_HANDLE *file_handle, WT_SESSION *session, bool lock) {
    // WT uses linux flock & fcntl, which isn't supported by liburing yet.
    // thus, just copying WT's __posix_file_lock()
    struct flock fl;
    int ret = 0;
    JEB_FILE_HANDLE *pfh;
    pfh = (JEB_FILE_HANDLE *)file_handle;

    /*
     * WiredTiger requires this function be able to acquire locks past the end of file.
     *
     * Note we're using fcntl(2) locking: all fcntl locks associated with a file for a given process
     * are removed when any file descriptor for the file is closed by the process, even if a lock
     * was never requested for that file descriptor.
     */
    fl.l_start = 0;
    fl.l_len = 1;
    fl.l_type = lock ? F_WRLCK : F_UNLCK;
    fl.l_whence = SEEK_SET;

    if ((ret = fcntl(pfh->fd, F_SETLK, &fl)) != 0) {
        fprintf(stderr, "failed to lock/unlock file: %d\n", ret);
    }

    return ret;
}

/* POSIX read */
static int 
jeb_fh_read(WT_FILE_HANDLE *file_handle, WT_SESSION *session, wt_off_t offset, 
    size_t len, void *buf) {
    printf("JEB::jeb_fh_read - %s\n", file_handle->name);
    JEB_FILE_HANDLE *jeb_file_handle;
    JEB_FILE_SYSTEM *jeb_fs;
    struct io_uring_sqe *sqe;
    int ret = 0;

    jeb_file_handle = (JEB_FILE_HANDLE *)file_handle;
    jeb_fs = jeb_file_handle->fs;

    // TODO: depending on the size of the incoming buffer, might want to break this 
    // up into multiple SQEs. That is what WT does in __posix_file_read().

    sqe = io_uring_get_sqe(&jeb_fs->ring);
    io_uring_prep_read(sqe, jeb_file_handle->fd, buf, len, offset);
    RING_EVENT_USER_DATA *ud = malloc(sizeof(RING_EVENT_USER_DATA));
    ud->event_type = EVENT_TYPE_NORMAL;
    
    /* Initialize mutex and condition variable objects */
    pthread_mutex_init(&ud->mutex, NULL);
    pthread_cond_init (&ud->condvar, NULL);
    pthread_mutex_lock(&ud->mutex);

    io_uring_sqe_set_data(sqe, ud);
    io_uring_submit(&jeb_fs->ring);
    
    // this should be in a loop to deal with spurious wakeups.
    // should probably use pthread_cond_timedwait().
    pthread_cond_wait(&ud->condvar, &ud->mutex);
    pthread_mutex_unlock(&ud->mutex);
    pthread_mutex_destroy(&ud->mutex);
    pthread_cond_destroy(&ud->condvar);
    ret = ud->ret_code;
    free(ud);

    if (ret < 0) {
        fprintf(stderr, "failure reading from file: %s\n", strerror(ret));
        return ret;
    }

    return 0;
}


static int 
jeb_fh_size(WT_FILE_HANDLE *file_handle, WT_SESSION *session, wt_off_t *sizep) {
    // NOTE: almost exactly the same as jeb_fs_size() - only diff is args to statx()
    JEB_FILE_HANDLE *jeb_file_handle;
    JEB_FILE_SYSTEM *jeb_fs;
    struct statx statx;
    struct io_uring_sqe *sqe;
    int ret = 0;
    int flags = 0;

    printf("JEB::jeb_fh_size %s\n", file_handle->name);
    jeb_file_handle = (JEB_FILE_HANDLE *)file_handle;
    jeb_fs = jeb_file_handle->fs;
    flags |= AT_EMPTY_PATH;
    sqe = io_uring_get_sqe(&jeb_fs->ring);
    io_uring_prep_statx(sqe, jeb_file_handle->fd, "", flags, 0, &statx);
    RING_EVENT_USER_DATA *ud = malloc(sizeof(RING_EVENT_USER_DATA));
    ud->event_type = EVENT_TYPE_NORMAL;
    
    /* Initialize mutex and condition variable objects */
    pthread_mutex_init(&ud->mutex, NULL);
    pthread_cond_init (&ud->condvar, NULL);
    pthread_mutex_lock(&ud->mutex);

    io_uring_sqe_set_data(sqe, ud);
    io_uring_submit(&jeb_fs->ring);

    // this should be in a loop to deal with spurious wakeups.
    // should probably use pthread_cond_timedwait().
    pthread_cond_wait(&ud->condvar, &ud->mutex);
    pthread_mutex_unlock(&ud->mutex);
    pthread_mutex_destroy(&ud->mutex);
    pthread_cond_destroy(&ud->condvar);
    ret = ud->ret_code;

    free(ud);
    if (ret == 0) {
        *sizep = statx.stx_size;
        return (0);
    }

    return ret;
}

/* ensure file content is stable */
static int 
jeb_fh_sync(WT_FILE_HANDLE *file_handle, WT_SESSION *session) {
    JEB_FILE_HANDLE *jeb_file_handle;
    JEB_FILE_SYSTEM *jeb_fs;
    struct io_uring_sqe *sqe;
    int ret = 0;

    printf("JEB::jeb_fh_sync - %s\n", file_handle->name);

    jeb_file_handle = (JEB_FILE_HANDLE *)file_handle;
    jeb_fs = jeb_file_handle->fs;

    sqe = io_uring_get_sqe(&jeb_fs->ring);
    io_uring_prep_fsync(sqe, jeb_file_handle->fd, 0);
    RING_EVENT_USER_DATA *ud = malloc(sizeof(RING_EVENT_USER_DATA));
    ud->event_type = EVENT_TYPE_NORMAL;

    /* Initialize mutex and condition variable objects */
    pthread_mutex_init(&ud->mutex, NULL);
    pthread_cond_init (&ud->condvar, NULL);
    pthread_mutex_lock(&ud->mutex);

    io_uring_sqe_set_data(sqe, ud);
    io_uring_submit(&jeb_fs->ring);

    // this should be in a loop to deal with spurious wakeups.
    // should probably use pthread_cond_timedwait().
    pthread_cond_wait(&ud->condvar, &ud->mutex);
    pthread_mutex_unlock(&ud->mutex);
    pthread_mutex_destroy(&ud->mutex);
    pthread_cond_destroy(&ud->condvar);
    ret = ud->ret_code;

    free(ud);
    return ret;
}

/* ensure file content is stable */
static int 
jeb_fh_sync_nowait(WT_FILE_HANDLE *file_handle, WT_SESSION *session) {
    printf("JEB::jeb_fh_sync_nowait\n");
    return (ENOTSUP);
}

/* POSIX truncate */
static int 
jeb_fh_truncate(WT_FILE_HANDLE *file_handle, WT_SESSION *session, wt_off_t len) {
    JEB_FILE_HANDLE *jeb_file_handle;
    int ret = 0;

    // TODO: there's a bunch of extra logic around the truncate() functions in WT
    // wrt mapping the file (to prevent races). will need to account for that  ...

    printf("JEB::jeb_fh_truncate - %s, new len: %ld\n", file_handle->name, len);
    jeb_file_handle = (JEB_FILE_HANDLE *)file_handle;

    // TODO: re-enable this mmap stuffs
    // remap = (len != pfh->mmap_size);
    // if (remap)
    //     __wt_prepare_remap_resize_file(file_handle, wt_session);

    if ((ret = ftruncate(jeb_file_handle->fd, len)) != 0) {
        fprintf(stderr, "failed to truncate %s, err: %s\n", file_handle->name, strerror(ret));
        return ret;
    }
    // if (remap) {
    //     if (ret == 0)
    //         __wt_remap_resize_file(file_handle, wt_session);
    //     else {
    //         __wt_release_without_remap(file_handle);
    //         WT_RET_MSG(session, ret, "%s: handle-truncate: ftruncate", file_handle->name);
    //     }
    // }
    return (0);
}

/* POSIX write. return zero on success and a non-zero error code on failure */
static int 
jeb_fh_write(WT_FILE_HANDLE *file_handle, WT_SESSION *session, wt_off_t offset, 
    size_t len, const void *buf) {
    JEB_FILE_HANDLE *jeb_file_handle;
    JEB_FILE_SYSTEM *jeb_fs;
    struct io_uring_sqe *sqe;
    int ret = 0;

    jeb_file_handle = (JEB_FILE_HANDLE *)file_handle;
    jeb_fs = jeb_file_handle->fs;

    // TODO: depending on the size of the incoming buffer, might want to break this 
    // up into multiple SQEs. That is what WT does in __posix_file_write().

    sqe = io_uring_get_sqe(&jeb_fs->ring);
    io_uring_prep_write(sqe, jeb_file_handle->fd, buf, len, offset);
    RING_EVENT_USER_DATA *ud = malloc(sizeof(RING_EVENT_USER_DATA));
    ud->event_type = EVENT_TYPE_NORMAL;
    
    /* Initialize mutex and condition variable objects */
    pthread_mutex_init(&ud->mutex, NULL);
    pthread_cond_init (&ud->condvar, NULL);
    pthread_mutex_lock(&ud->mutex);

    io_uring_sqe_set_data(sqe, ud);
    io_uring_submit(&jeb_fs->ring);
    
    // this should be in a loop to deal with spurious wakeups.
    // should probably use pthread_cond_timedwait().
    pthread_cond_wait(&ud->condvar, &ud->mutex);
    pthread_mutex_unlock(&ud->mutex);
    pthread_mutex_destroy(&ud->mutex);
    pthread_cond_destroy(&ud->condvar);
    ret = ud->ret_code;
    free(ud);

    if (ret < 0) {
        fprintf(stderr, "failure writing to file: %s\n", strerror(ret));
        return ret;
    }

    return 0;
}

/* Map a file into memory */
static int 
jeb_fh_map(WT_FILE_HANDLE *file_handle, WT_SESSION *session, void *mapped_region, size_t *length, void *mapped_cookie) {
    printf("JEB::jeb_fh_map\n");
    return (ENOTSUP);
}

/* Unmap part of a memory mapped file */
static int 
jeb_fh_map_discard(WT_FILE_HANDLE *file_handle, WT_SESSION *session, void *mapped_region, size_t length, void *mapped_cookie) {
    printf("JEB::jeb_fh_map_discard\n");
    return (ENOTSUP);
}

/* Preload part of a memory mapped file */
static int 
jeb_fh_map_preload(WT_FILE_HANDLE *file_handle, WT_SESSION *session, const void *mapped_region, size_t length, void *mapped_cookie) {
    printf("JEB::jeb_fh_map_preload\n");
    return (ENOTSUP);
}

/* Unmap a memory mapped file */
static int 
jeb_fh_unmap(WT_FILE_HANDLE *file_handle, WT_SESSION *session, void *mapped_region, size_t length, void *mapped_cookie) {
    printf("JEB::jeb_fh_unmap\n");
    return (ENOTSUP);
}

/* ! [JEB :: FILE HANDLE] */



int
main(int args, char *argv[]) {
    WT_CONNECTION *conn;
    WT_SESSION *session;
    WT_CURSOR *cursor;
    int ret;
    home = "/tmp/wt_hacking";
    config = "create,session_max=10000,statistics=(all),statistics_log=(wait=1),log=(file_max=1MB,enabled=true,compressor=zstd,path=journal)," \
    "extensions=[local={entry=create_custom_file_system,early_load=true},/usr/local/lib/libwiredtiger_lz4.so,/usr/local/lib/libwiredtiger_zstd.so]," \
    "error_prefix=MSG_JEB,verbose=[recovery_progress,checkpoint_progress,compact_progress,recovery]";

    fprintf(stderr, "*** JEB::main - about to open conn\n");
    // NOTE: hit some blocking behavior when using a custom event handler, so punting for now
    // if((ret = wiredtiger_open(home, (WT_EVENT_HANDLER *)&event_handler, config, &conn)) != 0) {
    if((ret = wiredtiger_open(home, NULL, config, &conn)) != 0) {
        fprintf(stderr, "failed to open dir: %s\n", wiredtiger_strerror(ret));
        return -1;
    }
    fprintf(stderr, "*** JEB::main - about to open session\n");
    if((ret = conn->open_session(conn, NULL, NULL, &session)) != 0) {
        fprintf(stderr, "failed to open session: %s\n", wiredtiger_strerror(ret));
        return -1;
    }

    fprintf(stderr, "*** JEB::main - about create table\n");
    if ((ret = session->create(session, "table:jeb1", "key_format=S,value_format=S")) != 0) {
        fprintf(stderr, "failed to create table: %s\n", wiredtiger_strerror(ret));
        return -1;
    }

    if ((ret = session->open_cursor(session, "table:jeb1", NULL, NULL, &cursor)) != 0) {
        fprintf(stderr, "failed to open cursor: %s\n", wiredtiger_strerror(ret));
        return -1;
    }

    // FINALLY, insert some data :)
    fprintf(stderr, "*** JEB::main - about to write data\n");
    cursor->set_key(cursor, "key1");
    cursor->set_value(cursor, "val1");
    if ((ret = cursor->insert(cursor)) != 0) {
        fprintf(stderr, "failed to write data: %s\n", wiredtiger_strerror(ret));
        return -1;
    }

    const char *key, *value;
    cursor->reset(cursor);
    while((ret = cursor->next(cursor)) == 0) {
        cursor->get_key(cursor, &key);
        cursor->get_value(cursor, &value);
        printf("*** JEB::main - next record: %s : %s\n", key, value);
    }
    //scan_end_check(ret == WT_NOTFOUND);

    // the sleep is mostly to let the stats pile up in the statslog
    // fprintf(stderr, "about to sleep\n");
    // sleep(10);

    fprintf(stderr, "*** JEB::main - about to close session\n");
    if((ret = conn->close(conn, NULL)) != 0) {
        fprintf(stderr, "failed to close connection: %s\n", wiredtiger_strerror(ret));
        return -1;
    }
    return 0;
}