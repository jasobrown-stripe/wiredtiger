#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif

#include <dirent.h>
#include <dirent.h>
#include <fcntl.h>
#include <linux/stat.h>
#include <pthread.h>
#include <stdlib.h>
#include <string.h>
#include <threads.h>
#include <sys/eventfd.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>
#include "wiredtiger.h"
#include "wiredtiger_ext.h"
#include "liburing.h"
#include "wt_internal.h"

/*
* A wrapper struct to be used with io_uring SQEs and CQEs.
*/
typedef struct __ring_event_user_data {

    // DEBUG - remove
    pthread_t caller;
} RING_EVENT_USER_DATA;


/* ! [JEB :: FILE_SYSTEM] */
typedef struct __jeb_file_system {
    WT_FILE_SYSTEM iface;

    /*
     * io_uring_statx() is unsupported with _IO_URING_SQPOLL until kernel 5.13. 
     Stripe runs 5.11 on devboxes, and 5.4 on prod (as of Jan 2022).
     */
    bool enable_iouring_statx;

    WT_EXTENSION_API *wtext;
} JEB_FILE_SYSTEM;


typedef struct __jeb_file_handle {
    WT_FILE_HANDLE iface;

    /* pointer to the enclosing file system */
    JEB_FILE_SYSTEM *fs;

    int fd;

    // flag to indicate if we've registered this handle's fd
    // with the io_uring. need to unregister when closing.
    bool uring_registered;

    /* The memory buffer and variables if we use mmap for I/O */
    uint8_t *mmap_buf;
    bool mmap_file_mappable;
    int mmap_prot;
    volatile uint32_t mmap_resizing;
    wt_off_t mmap_size;
    volatile uint32_t mmap_usecount;

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
static int jeb_fs_exist_uring(WT_FILE_SYSTEM *, WT_SESSION *, const char *, bool *);
static int jeb_fs_remove(WT_FILE_SYSTEM *, WT_SESSION *, const char *, uint32_t);
static int jeb_fs_rename(WT_FILE_SYSTEM *, WT_SESSION *, const char *, const char *, uint32_t);
static int jeb_fs_size_uring(WT_FILE_SYSTEM *, WT_SESSION *, const char *, wt_off_t *);
static int jeb_fs_terminate(WT_FILE_SYSTEM *, WT_SESSION *);

/*
* Forward function declarations for file handle API.
*/
static int jeb_fh_advise(WT_FILE_HANDLE *, WT_SESSION *, wt_off_t, wt_off_t, int);
static int jeb_fh_close(WT_FILE_HANDLE *, WT_SESSION *);
static int jeb_fh_extend(WT_FILE_HANDLE *, WT_SESSION *, wt_off_t offset);
static int jeb_fh_extend_nolock(WT_FILE_HANDLE *, WT_SESSION *, wt_off_t offset);
static int jeb_fh_lock(WT_FILE_HANDLE *, WT_SESSION *, bool);
static int jeb_fh_read(WT_FILE_HANDLE *, WT_SESSION *, wt_off_t, size_t, void *);
static int jeb_fh_size_uring(WT_FILE_HANDLE *, WT_SESSION *, wt_off_t *);
static int jeb_fh_sync(WT_FILE_HANDLE *, WT_SESSION *);
static int jeb_fh_sync_nowait(WT_FILE_HANDLE *, WT_SESSION *);
static int jeb_fh_truncate(WT_FILE_HANDLE *, WT_SESSION *, wt_off_t);
static int jeb_fh_write(WT_FILE_HANDLE *, WT_SESSION *, wt_off_t, size_t, const void *);

/*
* Forward-declarations of mmap-related functions
*/
static int jeb_fh_read_mmap(WT_FILE_HANDLE *, WT_SESSION *, wt_off_t, size_t, void *);
static int jeb_fh_write_mmap(WT_FILE_HANDLE *, WT_SESSION *, wt_off_t, size_t, const void *);
static int jeb_fh_map(WT_FILE_HANDLE *, WT_SESSION *, void *, size_t *, void *);
static int jeb_fh_map_discard(WT_FILE_HANDLE *, WT_SESSION *, void *, size_t, void *);
static int jeb_fh_map_preload(WT_FILE_HANDLE *, WT_SESSION *, const void *, size_t, void *);
static int jeb_fh_unmap(WT_FILE_HANDLE *, WT_SESSION *, void *, size_t, void *);

/*
* Forward-declarations of functions copied due to io_uring_statx() and SQPOLL incompatabilities
*/
static int jeb_fs_exist_posix(WT_FILE_SYSTEM *, WT_SESSION *, const char *, bool *);
static int jeb_fs_size_posix(WT_FILE_SYSTEM *, WT_SESSION *, const char *, wt_off_t *);
static int jeb_fh_size_posix(WT_FILE_HANDLE *, WT_SESSION *, wt_off_t *);

static void __map_file(WT_FILE_HANDLE *, WT_SESSION *);
static void __unmap_file(WT_FILE_HANDLE *, WT_SESSION *);
static void __prepare_remap_resize_file(WT_FILE_HANDLE *, WT_SESSION *);
static void __release_without_remap(WT_FILE_HANDLE *);
static void __remap_resize_file(WT_FILE_HANDLE *, WT_SESSION *);

int init_io_uring(struct io_uring **ring) {
    struct io_uring_params params;
    int ret = 0;

    if (geteuid()) {
        fprintf(stderr, "You need root privileges to run this program.\n");
        return 1;
    }
    memset(&params, 0, sizeof(struct io_uring_params));
    // params.flags |= IORING_SETUP_SQPOLL;

    // TODO: make idle time a config option?
    params.sq_thread_idle = 120000; // 2 minutes in ms;

    // TODO: check out more io_uring params here: https://unixism.net/loti/ref-iouring/io_uring_setup.html
    // i like IORING_FEAT_NODROP

    // TODO: make queue depth a config option?
    ret = io_uring_queue_init_params(16, *ring, &params);
    if (ret) {
        fprintf(stderr, "unable to setup uring: %s\n", strerror(-ret));
        return 1;
    }

    return 0;
}

void _get_uring(struct io_uring** ring) {
    int ret = 0;

    _Thread_local static struct io_uring *ring_tl = NULL;
    if (ring_tl == NULL) {
        if ((ring_tl = calloc(1, sizeof(struct io_uring))) == NULL) {
            fprintf(stderr, "no mem for io_uring\n");
            exit(1);
        }

        init_io_uring(&ring_tl);
        printf("JEB::_get_uring - successfully created uring \n");
    }
    *ring = ring_tl;
}

/*
* function exxecuted by a bg thread to block on the eventfd, and when it awakens,
* check the ring for CQEs. For each CQE available, poke the "lock"
* in the user_data to awaken the blocked read/write thread.
*
* This thread will not free any memory.
*/
int consume_event(struct io_uring *ring, struct io_uring_cqe **cqe) {
    int ret = 0;

    while (1) {
        // Note: there's also io_uring_wait_cqe_timeout!
        ret = io_uring_wait_cqe(ring, cqe);
        if (ret == -EAGAIN) {
            continue;
        }

        // this shouldn't happen ???
        if (!cqe) {
            // error????
            return -1;
        }

        if (ret == 0) {
            return 0;
        }
    }
}

/*
* NOTE: io_uring_statx() is unsupported with _IO_URING_SQPOLL until kernel 5.13.
* Stripe runs 5.11 on devboxes, and 5.4 on prod (as of Jan 2022).
*/
static bool
use_iouring_statx() {
    // TODO: maybe do some fancy footwork like check a kernel version,
    // not sure what the state of the art is. Also, unclear if one of the
    // io_uring feature flags will declare if statx is supported with SQPOLL
    return false;
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
    // int efd;

    wtext = conn->get_extension_api(conn);

    if ((fs = calloc(1, sizeof(JEB_FILE_SYSTEM))) == NULL) {
        (void)wtext->err_printf(wtext, NULL, "failed to allocate custom file system: %s",
                wtext->strerror(wtext, NULL, ENOMEM));
        return (ENOMEM);
    }

    fs->wtext = wtext;
    file_system = (WT_FILE_SYSTEM *)fs;
    fs->enable_iouring_statx = use_iouring_statx();

    // NOTE: this is where we can parse the config string to set values into the custom FS,
    // but I'm omitting for now (not sure if i need ....)

    file_system->fs_directory_list = jeb_fs_directory_list;
    file_system->fs_directory_list_free = jeb_fs_directory_list_free;
    file_system->fs_open_file = jeb_fs_open;
    file_system->fs_remove = jeb_fs_remove;
    file_system->fs_rename = jeb_fs_rename;
    file_system->terminate = jeb_fs_terminate;

    if (fs->enable_iouring_statx) {
        file_system->fs_exist = jeb_fs_exist_uring;
        file_system->fs_size = jeb_fs_size_uring;
    } else {
        file_system->fs_exist = jeb_fs_exist_posix;
        file_system->fs_size = jeb_fs_size_posix;
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
jeb_fs_open(WT_FILE_SYSTEM *fs, WT_SESSION *wt_session, const char *name , 
    WT_FS_OPEN_FILE_TYPE file_type, uint32_t flags , WT_FILE_HANDLE **file_handlep) {
    JEB_FILE_HANDLE *jeb_file_handle;
    JEB_FILE_SYSTEM *jeb_fs;
    WT_FILE_HANDLE *file_handle;
    WT_CONNECTION_IMPL *conn;
    WT_SESSION_IMPL *session;
    struct io_uring *ring;
    struct io_uring_sqe *sqe = NULL;
    struct io_uring_cqe *cqe = NULL;
    int ret = 0;
    int open_flags = 0, fd = 0, mode = 0;
    bool uring_registered = false;

    printf("JEB::jeb_fs_open %s\n", name);

    *file_handlep = NULL;
    jeb_fs = (JEB_FILE_SYSTEM *)fs;
    jeb_file_handle = NULL;
    session = (WT_SESSION_IMPL *)wt_session;
    conn = S2C(session);

    if (file_type == WT_FS_OPEN_FILE_TYPE_DIRECTORY) {
        mode = 0444;
        open_flags = O_RDONLY | O_CLOEXEC;
    } else {
        // assume regular file ....
        open_flags = flags & WT_FS_OPEN_READONLY ? O_RDONLY : O_RDWR;
        open_flags |= O_CLOEXEC;
        if (flags & WT_FS_OPEN_CREATE) {
            open_flags |= O_CREAT;
            if (flags & WT_FS_OPEN_EXCLUSIVE) 
                open_flags |= O_EXCL;
            mode = 0644;
        } else {
            mode = 0;
        }
        if (file_type == WT_FS_OPEN_FILE_TYPE_LOG && FLD_ISSET(conn->txn_logsync, WT_LOG_DSYNC)) {
            open_flags |= O_DSYNC;
        }
    }

    // FIXME temp hack: if the path is relative, we need to set AT_FDCWD for openat()
    // open_flags |= AT_FDCWD;

    // NOTE: WT sets the O_DSYNC flag on log (WAL) files. not sure if that's totally cool with io_uring,
    // but I didn't look very hard: https://lore.kernel.org/all/CAF-ewDoqyx5knsnd_qgfRXE+CxK==PO1zF+RE=oEuv9NQq+48g@mail.gmail.com/T/
    _get_uring(&ring);
    sqe = io_uring_get_sqe(ring);
    io_uring_prep_openat(sqe, 0, name, open_flags, mode);

    if ((ret = io_uring_submit(ring)) != 1) {
        printf("JEB::jeb_fs_open - io_uring_submit ret: %d\n", ret);
        // err? retry?
    }
    ret = consume_event(ring, &cqe);
    // TODO check ret for error

    ret = cqe->res;
    io_uring_cqe_seen(ring, cqe);
    if (ret < 0) {
        fprintf(stderr, "failed to create a new file: %s, ret: %d, err: %s\n", name, -ret, strerror(-ret));
        return -ret;
    }
    fd = ret;
    ret = 0;

       // TODO: when I actually tried this, the syscall just hung forever or returned EBUSY :( 
    // if (file_type == WT_FS_OPEN_FILE_TYPE_DATA) {
    //     printf("JEB::jeb_fs_open %s -- registering fd %d with uring\n", name, fd);

    //    ret = io_uring_register_files(ring, &fd, 1);
	// 	if (ret) {
	// 		fprintf(stderr, "file reg failed: %d, err: %s\n", ret, strerror(-ret));
	// 	} else {
    //         uring_registered = true;
    //     }
    // }

    // WT does some fadvise stuff, as well. copying ....
    if (file_type == WT_FS_OPEN_FILE_TYPE_DATA && LF_ISSET(WT_FS_OPEN_ACCESS_RAND | WT_FS_OPEN_ACCESS_SEQ)) {
        printf("JEB::jeb_fs_open - setting posix_fadvise\n");
        int advise_flag = 0;
        if (LF_ISSET(WT_FS_OPEN_ACCESS_RAND))
            advise_flag = POSIX_FADV_RANDOM;
        if (LF_ISSET(WT_FS_OPEN_ACCESS_SEQ))
            advise_flag = POSIX_FADV_SEQUENTIAL;
        sqe = io_uring_get_sqe(ring);
        io_uring_prep_fadvise(sqe, fd, 0, 0, advise_flag);

        if ((ret = io_uring_submit(ring)) != 1) {
            printf("JEB::jeb_fs_open - failed to submit posix_fadvise: %d\n", ret);
            // err? retry?
        }
        ret = consume_event(ring, &cqe);
        // TODO check ret for error

        ret = cqe->res;
        io_uring_cqe_seen(ring, cqe);
        if (ret < 0) {
            printf("JEB::jeb_fs_open - failed to set posix_fadvise: %d\n", ret);
            // err? retry?
        }
    }

    if ((jeb_file_handle = calloc(1, sizeof(JEB_FILE_HANDLE))) == NULL) {
        ret = ENOMEM;
    }

    jeb_file_handle->fs = jeb_fs;
    jeb_file_handle->fd = fd;
    jeb_file_handle->uring_registered = uring_registered;

    file_handle = (WT_FILE_HANDLE *)jeb_file_handle;
    file_handle->file_system = fs;
    if ((file_handle->name = strdup(name)) == NULL) {
        ret = ENOMEM;
        // goto err;
    }


    file_handle->close = jeb_fh_close;
    file_handle->fh_advise = jeb_fh_advise;
    file_handle->fh_extend = jeb_fh_extend;
    file_handle->fh_extend_nolock = jeb_fh_extend_nolock;
    file_handle->fh_lock = jeb_fh_lock;
    file_handle->fh_map = jeb_fh_map;
    file_handle->fh_map_discard = jeb_fh_map_discard;
    file_handle->fh_map_preload = jeb_fh_map_preload;
    file_handle->fh_sync = jeb_fh_sync;
    file_handle->fh_sync_nowait = jeb_fh_sync_nowait;
    file_handle->fh_truncate = jeb_fh_truncate;
    file_handle->fh_unmap = jeb_fh_unmap;

    if (jeb_fs->enable_iouring_statx) {
        file_handle->fh_size = jeb_fh_size_uring;
    } else {
        file_handle->fh_size = jeb_fh_size_posix;
    }

    // this block borrowed from WT __posix_open_file
    if (conn->mmap_all) {
        /*
         * We are going to use mmap for I/O. So let's mmap the file on opening. If mmap fails, we
         * will just mark the file as not mappable (inside the mapping function) and will use system
         * calls for I/O on this file. We will not crash the database if mmap fails.
         */
        if (file_type == WT_FS_OPEN_FILE_TYPE_DATA || file_type == WT_FS_OPEN_FILE_TYPE_LOG) {
            jeb_file_handle->mmap_file_mappable = true;
            jeb_file_handle->mmap_prot = LF_ISSET(WT_FS_OPEN_READONLY) ? PROT_READ : PROT_READ | PROT_WRITE;
            __map_file(file_handle, wt_session);
        }
    }

    if (jeb_file_handle->mmap_file_mappable) {
        file_handle->fh_read = jeb_fh_read_mmap;
        file_handle->fh_write = jeb_fh_write_mmap;
    } else {
        file_handle->fh_read = jeb_fh_read;
        file_handle->fh_write = jeb_fh_write;
    }

    *file_handlep = file_handle;

    return (ret);
}

/*
* to be used when io_uring_statx cannot be used.
*/
static int
jeb_fs_exist_posix(
  WT_FILE_SYSTEM *file_system, WT_SESSION *wt_session, const char *name, bool *existp)
{
    struct stat sb;
    int ret = 0;

    printf("JEB::jeb_fs_exist_posix - name = %s\n", name);
    if ((ret = stat(name, &sb)) != 0) {
        if (errno == ENOENT) {
            *existp = false;
            return (0);
        }
        return errno;
    }

    *existp = true;
    return (0);
}

/* return if file exists */
static int 
jeb_fs_exist_uring(WT_FILE_SYSTEM *fs, WT_SESSION *session, const char *name, bool *existp) {
    JEB_FILE_SYSTEM *jeb_fs;
    struct statx statx;
    struct io_uring *ring;
    struct io_uring_sqe *sqe;
    struct io_uring_cqe *cqe;
    int ret = 0;

    jeb_fs = (JEB_FILE_SYSTEM *)fs;

    _get_uring(&ring);
    sqe = io_uring_get_sqe(ring);
    io_uring_prep_statx(sqe, 0, name, 0, 0, &statx);
    if ((ret = io_uring_submit(ring)) != 1) {
        printf("JEB::jeb_fs_exist_uring - io_uring_submit ret: %d\n", ret);
        // err? retry?
    }
    ret = consume_event(ring, &cqe);
    // TODO check ret for error

    ret = cqe->res;
    io_uring_cqe_seen(ring, cqe);

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

static int
jeb_fs_size_posix(WT_FILE_SYSTEM *file_system, WT_SESSION *wt_session, const char *name, wt_off_t *sizep) {
    struct stat sb;
    int ret = 0;

    printf("JEB::jeb_fs_size_posix %s\n", name);
    if ((ret = stat(name, &sb)) != 0) {
        return errno;
    }

    *sizep = sb.st_size;
    return (0);
}

/* get the size of file in bytes */
static int 
jeb_fs_size_uring(WT_FILE_SYSTEM *fs, WT_SESSION *session, const char *name, wt_off_t *sizep) {
    // NOTE: almost exactly the same as jeb_fh_size() - only diff is args to stax()
    JEB_FILE_SYSTEM *jeb_fs;
    struct statx statx;
    struct io_uring *ring;
    struct io_uring_sqe *sqe;
    struct io_uring_cqe *cqe;
    RING_EVENT_USER_DATA *ud;
    int ret = 0;

    printf("JEB::jeb_fs_size_uring %s\n", name);
    jeb_fs = (JEB_FILE_SYSTEM *)fs;

    _get_uring(&ring);
    sqe = io_uring_get_sqe(ring);
    io_uring_prep_statx(sqe, 0, name, 0, 0, &statx);
    if ((ret = io_uring_submit(ring)) != 1) {
        printf("JEB::jeb_fs_size_uring - io_uring_submit ret: %d\n", ret);
        // err? retry?
    }
    ret = consume_event(ring, &cqe);
    // TODO check ret for error

    ret = cqe->res;
    io_uring_cqe_seen(ring, cqe);

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
jeb_fs_directory_list(WT_FILE_SYSTEM *fs, WT_SESSION *wt_session, const char *directory, 
    const char *prefix, char ***dirlistp, uint32_t *countp) {
    // io_uring doesn't support syscalls to list a directory.
    // thus, copying WT's __directory_list_worker()

    WT_SESSION_IMPL *session;
    struct dirent *dp;
    DIR *dirp;
    size_t dirallocsz;
    uint32_t count;
    char **entries;
    int ret = 0;

    *dirlistp = NULL;
    *countp = 0;
    dirp = NULL;
    dirallocsz = 0;
    entries = NULL;

    printf("JEB::jeb_fs_directory_list - dir: %s, prefix: %s\n", directory, prefix);
    session = (WT_SESSION_IMPL *)wt_session;

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

        // TODO: ignoring any errors that might happen (for now)
        __wt_realloc_def(session, &dirallocsz, count + 1, &entries);
        if ((ret = __wt_strdup(session, dp->d_name, &entries[count])) != 0) {
            fprintf(stderr, "JEB::jeb_fs_directory_list __wt_strdup err: %d, %s", ret, strerror(ret));
            return ret;
        }
        ++count;
    }

    *dirlistp = entries;
    *countp = count;
    return ret;
}

/* free memory allocated by jeb_fs_directory_list */
static int 
jeb_fs_directory_list_free(WT_FILE_SYSTEM *fs, WT_SESSION *session, char **dirlist, uint32_t count) {
    printf("JEB::jeb_fs_directory_list_free\n");

    if (dirlist != NULL) {
        while (count > 0)
            free(dirlist[--count]);
        free(dirlist);
    }

    return (0);
}

/* 
* discard any resources on termination.
* send the ring_consumer thread a special message to tell it to 
* stop blocking on the uring. then shutdown the uring.
*/
static int 
jeb_fs_terminate(WT_FILE_SYSTEM *fs, WT_SESSION *session) {
    JEB_FILE_SYSTEM *jeb_fs;

    jeb_fs = (JEB_FILE_SYSTEM *)fs;
    free(jeb_fs);

    return (0);
}
/* ! [JEB :: FILE_SYSTEM] */

/* ! [JEB :: FILE HANDLE] */
static int
jeb_fh_advise(WT_FILE_HANDLE *file_handle, WT_SESSION *session, wt_off_t offset, wt_off_t len , int advice) {
    JEB_FILE_HANDLE *jeb_file_handle;
    struct io_uring *ring;
    struct io_uring_sqe *sqe = NULL;
    struct io_uring_cqe *cqe = NULL;
    int ret = 0;

    printf("JEB::jeb_fh_advise - file = %s\n", file_handle->name);
    jeb_file_handle = (JEB_FILE_HANDLE *)file_handle;
    _get_uring(&ring);
    sqe = io_uring_get_sqe(ring);
    io_uring_prep_fadvise(sqe, jeb_file_handle->fd, offset, len, advice);

    if ((ret = io_uring_submit(ring)) != 1) {
        printf("JEB::jeb_fh_advise - failed to submit posix_fadvise: %d\n", ret);
        // err? retry?
    }
    ret = consume_event(ring, &cqe);
    // TODO check ret for error

    ret = cqe->res;
    io_uring_cqe_seen(ring, cqe);
    if (ret < 0) {
        printf("JEB::jeb_fs_open - failed to set posix_fadvise: %d\n", ret);
        // err? retry?
    }

    return ret;
}


static int 
jeb_fh_close(WT_FILE_HANDLE *file_handle, WT_SESSION *session) {
    JEB_FILE_HANDLE *jeb_file_handle;
    JEB_FILE_SYSTEM *jeb_fs;
    struct io_uring *ring;
    struct io_uring_sqe *sqe;
    struct io_uring_cqe *cqe;
    int ret = 0;

    jeb_file_handle = (JEB_FILE_HANDLE *)file_handle;
    jeb_fs = jeb_file_handle->fs;

    if (jeb_file_handle->mmap_file_mappable && jeb_file_handle->mmap_buf != NULL)
        __unmap_file(file_handle, session);

    _get_uring(&ring);
    sqe = io_uring_get_sqe(ring);
    io_uring_prep_close(sqe, jeb_file_handle->fd);
    if ((ret = io_uring_submit(ring)) != 1) {
        printf("JEB::jeb_fh_close - io_uring_submit ret: %d\n", ret);
        // err? retry?
    }
    ret = consume_event(ring, &cqe);
    // TODO check ret for error

    ret = cqe->res;
    io_uring_cqe_seen(ring, cqe);

    if (jeb_file_handle->uring_registered) {
        // TODO: not entirely clear how to unregister a *single* file from the uring.
        // will research later .....
    }

    free(file_handle->name);
    free(jeb_file_handle);
    return ret;
}

static int 
jeb_fh_extend(WT_FILE_HANDLE *file_handle, WT_SESSION *session, wt_off_t offset) {
    JEB_FILE_HANDLE *jeb_file_handle;
    JEB_FILE_SYSTEM *jeb_fs;
    struct io_uring *ring;
    struct io_uring_sqe *sqe;
    struct io_uring_cqe *cqe;
    int ret = 0;

    jeb_file_handle = (JEB_FILE_HANDLE *)file_handle;
    jeb_fs = jeb_file_handle->fs;

    // TODO: there's a bunch of extra logic around the extend() functions in WT
    // wrt mapping the file (to prevent races). will need to account for that  ...

    printf("JEB::jeb_fh_extend - %s\n", file_handle->name);
    _get_uring(&ring);
    sqe = io_uring_get_sqe(ring);
    io_uring_prep_fallocate(sqe, jeb_file_handle->fd, 0, (wt_off_t)0, offset);
    if ((ret = io_uring_submit(ring)) != 1) {
        printf("JEB::jeb_fh_extend - io_uring_submit ret: %d\n", ret);
        // err? retry?
    }
    ret = consume_event(ring, &cqe);
    // TODO check ret for error

    ret = cqe->res;
    io_uring_cqe_seen(ring, cqe);

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

/* io_uring read */
static int 
jeb_fh_read_mmap(WT_FILE_HANDLE *file_handle, WT_SESSION *wt_session, wt_off_t offset, 
    size_t len, void *buf) {
    JEB_FILE_HANDLE *jeb_file_handle;
    bool mmap_success;
    int ret = 0;

    jeb_file_handle = (JEB_FILE_HANDLE *)file_handle;

    if(!jeb_file_handle->mmap_file_mappable || jeb_file_handle->mmap_resizing) {
        return jeb_fh_read(file_handle, wt_session, offset, len, buf);
    }

    /* Indicate that we might be using the mapped area */
    (void)__wt_atomic_addv32(&jeb_file_handle->mmap_usecount, 1);

    mmap_success = false;
    if (jeb_file_handle->mmap_buf != NULL && jeb_file_handle->mmap_size >= offset + (wt_off_t)len && !jeb_file_handle->mmap_resizing) {
        memcpy(buf, (void *)(jeb_file_handle->mmap_buf + offset), len);
        mmap_success = true;
    }

    /* Signal that we are done using the mapped buffer. */
    (void)__wt_atomic_subv32(&jeb_file_handle->mmap_usecount, 1);

    if (mmap_success)
        return 0;

    /* We couldn't use mmap for some reason, so use the system call. */
    return jeb_fh_read(file_handle, wt_session, offset, len, buf);
}

/* io_uring read */
static int 
jeb_fh_read(WT_FILE_HANDLE *file_handle, WT_SESSION *session, wt_off_t offset, 
    size_t len, void *buf) {
    // printf("JEB::jeb_fh_read - %s\n", file_handle->name);
    JEB_FILE_HANDLE *jeb_file_handle;
    struct io_uring *ring;
    struct io_uring_sqe *sqe;
    struct io_uring_cqe *cqe;
    int ret = 0;

    jeb_file_handle = (JEB_FILE_HANDLE *)file_handle;

    // TODO: depending on the size of the incoming buffer, might want to break this 
    // up into multiple SQEs. That is what WT does in __posix_file_read().

    _get_uring(&ring);
    sqe = io_uring_get_sqe(ring);
    io_uring_prep_read(sqe, jeb_file_handle->fd, buf, len, offset);
    // if (jeb_file_handle->uring_registered) {
    //         sqe->flags |= IOSQE_FIXED_FILE;
    // }
    // distinguish between <= 0 (error) and > 1 (multiple SQEs submitted)
    if ((ret = io_uring_submit(ring)) != 1) {
        printf("JEB::jeb_fh_read - io_uring_submit ret: %d\n", ret);
        // err? retry?
    }
    ret = consume_event(ring, &cqe);
    // TODO check ret for error

    ret = cqe->res;
    io_uring_cqe_seen(ring, cqe);

    if (ret < 0) {
        fprintf(stderr, "failure reading from file: %s\n", strerror(ret));
        return ret;
    } else if (ret < len) {
        printf("JEB::jeb_fh_read - SHORT READ requested len: %ld, read len %d\n", len, ret);
        return ret;
    }
    // printf("JEB::jeb_fh_read - requested len: %ld, read len %d\n", len, ret);

    return 0;
}

/*
* to be used when io_uring_statx cannot be used.
*/
static int
jeb_fh_size_posix(WT_FILE_HANDLE *file_handle, WT_SESSION *wt_session, wt_off_t *sizep) {
    struct stat sb;
    int ret = 0;
    JEB_FILE_HANDLE *jeb_file_handle;

    jeb_file_handle = (JEB_FILE_HANDLE *)file_handle;
    printf("JEB::jeb_fh_size_posix %s, fd = %d\n", file_handle->name, jeb_file_handle->fd);

    if ((ret = fstat(jeb_file_handle->fd, &sb)) != 0) {
        return errno;
    }

    *sizep = sb.st_size;
    return (0);
}

static int 
jeb_fh_size_uring(WT_FILE_HANDLE *file_handle, WT_SESSION *session, wt_off_t *sizep) {
    // NOTE: almost exactly the same as jeb_fs_size_statx() - only diff is args to statx()
    JEB_FILE_HANDLE *jeb_file_handle;
    JEB_FILE_SYSTEM *jeb_fs;
    struct statx statx;
    struct io_uring *ring;
    struct io_uring_sqe *sqe;
    struct io_uring_cqe *cqe;
    int ret = 0;
    int flags = 0;

    jeb_file_handle = (JEB_FILE_HANDLE *)file_handle;
    jeb_fs = jeb_file_handle->fs;

    printf("JEB::jeb_fh_size_uring %s, fd = %d\n", file_handle->name, jeb_file_handle->fd);
    flags |= AT_EMPTY_PATH;
    _get_uring(&ring);
    sqe = io_uring_get_sqe(ring);
    io_uring_prep_statx(sqe, jeb_file_handle->fd, "", flags, 0, &statx);
    if ((ret = io_uring_submit(ring)) != 1) {
        printf("JEB::jeb_fh_size_uring - io_uring_submit ret: %d\n", ret);
        // err? retry?
    }
    ret = consume_event(ring, &cqe);
    // TODO check ret for error

    ret = cqe->res;
    io_uring_cqe_seen(ring, cqe);

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
    struct io_uring *ring;
    struct io_uring_sqe *sqe;
    struct io_uring_cqe *cqe;
    int ret = 0;

    printf("JEB::jeb_fh_sync - %s\n", file_handle->name);

    jeb_file_handle = (JEB_FILE_HANDLE *)file_handle;
    jeb_fs = jeb_file_handle->fs;

    _get_uring(&ring);
    sqe = io_uring_get_sqe(ring);
    io_uring_prep_fsync(sqe, jeb_file_handle->fd, 0);
    if ((ret = io_uring_submit(ring)) != 1) {
        printf("JEB::jeb_fh_sync - io_uring_submit ret: %d\n", ret);
        // err? retry?
    }
    ret = consume_event(ring, &cqe);
    // TODO check ret for error

    ret = cqe->res;
    io_uring_cqe_seen(ring, cqe);

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
jeb_fh_truncate(WT_FILE_HANDLE *file_handle, WT_SESSION *wt_session, wt_off_t len) {
    JEB_FILE_HANDLE *jeb_file_handle;
    bool remap;
    int ret = 0;

    printf("JEB::jeb_fh_truncate - %s, new len: %ld\n", file_handle->name, len);
    jeb_file_handle = (JEB_FILE_HANDLE *)file_handle;

    remap = (len != jeb_file_handle->mmap_size);
    if (remap) {
        __prepare_remap_resize_file(file_handle, wt_session);
    }

    if ((ret = ftruncate(jeb_file_handle->fd, len)) != 0) {
        fprintf(stderr, "failed to truncate %s, err: %s\n", file_handle->name, strerror(ret));
        return ret;
    }

    if (remap) {
        if (ret == 0)
            __remap_resize_file(file_handle, wt_session);
        else {
            __release_without_remap(file_handle);
        }
    }

    return (0);
}

/* POSIX write. return zero on success and a non-zero error code on failure */
static int 
jeb_fh_write_mmap(WT_FILE_HANDLE *file_handle, WT_SESSION *wt_session, wt_off_t offset, 
    size_t len, const void *buf) {
    static int remap_opportunities;
    int ret = 0;

    JEB_FILE_HANDLE *jeb_file_handle;
    bool mmap_success;

    jeb_file_handle = (JEB_FILE_HANDLE *)file_handle;
    
    if (!jeb_file_handle->mmap_file_mappable || jeb_file_handle->mmap_resizing) {
        return jeb_fh_write(file_handle, wt_session, offset, len, buf);
    }

        /* Indicate that we might be using the mapped area */
    (void)__wt_atomic_addv32(&jeb_file_handle->mmap_usecount, 1);

    // try mmap write
    mmap_success = false;
    if (jeb_file_handle->mmap_buf != NULL && jeb_file_handle->mmap_size >= offset + (wt_off_t)len && !jeb_file_handle->mmap_resizing) {
        memcpy((void *)(jeb_file_handle->mmap_buf + offset), buf, len);
        mmap_success = true;
    }

    /* Signal that we are done using the mapped buffer. */
    (void)__wt_atomic_subv32(&jeb_file_handle->mmap_usecount, 1);

    if (mmap_success)
        return 0;

    /* We couldn't use mmap for some reason, so use the system call. */
    ret = jeb_fh_write(file_handle, wt_session, offset, len, buf);

/*
 * If we wrote the file via a system call, we might have extended its size. If the file is mapped,
 * remap it with the new size. If we are actively extending the file, don't remap it on every write
 * to avoid overhead.
 */
#define WT_REMAP_SKIP 10
    if (jeb_file_handle->mmap_file_mappable && !jeb_file_handle->mmap_resizing && jeb_file_handle->mmap_size < offset + (wt_off_t)len) {
        /* If we are actively extending the file, don't remap it on every write. */
        if ((remap_opportunities++) % WT_REMAP_SKIP == 0) {
            __wt_prepare_remap_resize_file(file_handle, wt_session);
            __wt_remap_resize_file(file_handle, wt_session);
        }
    }

    return (0);
}


/* POSIX write. return zero on success and a non-zero error code on failure */
static int 
jeb_fh_write(WT_FILE_HANDLE *file_handle, WT_SESSION *session, wt_off_t offset, 
    size_t len, const void *buf) {
    JEB_FILE_HANDLE *jeb_file_handle;
    struct io_uring *ring;
    struct io_uring_sqe *sqe;
    struct io_uring_cqe *cqe;
    int ret = 0;

    jeb_file_handle = (JEB_FILE_HANDLE *)file_handle;

    // TODO: depending on the size of the incoming buffer, might want to break this 
    // up into multiple SQEs. That is what WT does in __posix_file_write().

    pthread_t caller = pthread_self();
    // printf("JEB::jeb_fh_write (%s) - len: %ld, offset: %ld, threadId: %ld\n", file_handle->name, len, offset, caller);

    _get_uring(&ring);
    sqe = io_uring_get_sqe(ring);
    io_uring_prep_write(sqe, jeb_file_handle->fd, buf, len, offset);
    //  if (jeb_file_handle->uring_registered) {
    //         sqe->flags |= IOSQE_FIXED_FILE;
    // }
    if ((ret = io_uring_submit(ring)) != 1) {
        printf("JEB::jeb_fh_write - io_uring_submit ret: %d\n", ret);
        // err? retry?
    }
    ret = consume_event(ring, &cqe);
    // TODO check ret for error

    ret = cqe->res;
    io_uring_cqe_seen(ring, cqe);

    if (ret < 0) {
        fprintf(stderr, "failure writing to file: %s\n", strerror(ret));
        return ret;
    }

    return 0;
}

/* Map a file into memory */
static int 
jeb_fh_map(WT_FILE_HANDLE *file_handle, WT_SESSION *session, void *mapped_regionp, size_t *lenp, void *mapped_cookie) {
    JEB_FILE_HANDLE *jeb_file_handle;
    wt_off_t file_size;
    size_t len;
    void *map;
    int ret = 0;

    printf("JEB::jeb_fh_map %s\n", file_handle->name);
    jeb_file_handle = (JEB_FILE_HANDLE *)file_handle;    
    if ((ret = file_handle->fh_size(file_handle, session, &file_size)) != 0) {
        return ret;
    }
    len = file_size;

    if((map = mmap(NULL, len, PROT_READ, MAP_PRIVATE, jeb_file_handle->fd, (wt_off_t)0)) == MAP_FAILED) {
        printf("JEB::jeb_fh_map failed to map %s, errno: %d", file_handle->name, errno);
        return errno;
    }

    *(void **)mapped_regionp = map;
    *lenp = len;
    return (0);
}

/* Preload part of a memory mapped file */
// mostly a copy of __wt_posix_map_preload
static int 
jeb_fh_map_preload(WT_FILE_HANDLE *file_handle, WT_SESSION *wt_session, const void *map, size_t length, void *mapped_cookie) {
    WT_CONNECTION_IMPL *conn;
    WT_SESSION_IMPL *session;
    WT_BM *bm;
    JEB_FILE_HANDLE *jeb_file_handle;
    JEB_FILE_SYSTEM *jeb_fs;
    struct io_uring *ring;
    struct io_uring_sqe *sqe;
    struct io_uring_cqe *cqe;
    int ret = 0;
    void *blk;

    session = (WT_SESSION_IMPL *)wt_session;
    conn = S2C(session);
    bm = S2BT(session)->bm;

    /* Linux requires the address be aligned to a 4KB boundary. */
    blk = (void *)((uintptr_t)map & ~(uintptr_t)(conn->page_size - 1));
    length += WT_PTRDIFF(map, blk);

    /* XXX proxy for "am I doing a scan?" -- manual read-ahead */
    if (F_ISSET(session, WT_SESSION_READ_WONT_NEED)) {
        /* Read in 2MB blocks every 1MB of data. */
        if (((uintptr_t)((uint8_t *)blk + length) & (uintptr_t)((1 << 20) - 1)) < (uintptr_t)blk)
            return (0);
        length =
          WT_MIN(WT_MAX(20 * length, 2 << 20), WT_PTRDIFF((uint8_t *)bm->map + bm->maplen, blk));
    }

    /*
     * Manual pages aren't clear on whether alignment is required for the size, so we will be
     * conservative.
     */
    length &= ~(size_t)(conn->page_size - 1);
    if (length <= (size_t)conn->page_size)
        return (0);

    _get_uring(&ring);
    sqe = io_uring_get_sqe(ring);
    io_uring_prep_madvise(sqe, blk, length, POSIX_MADV_WILLNEED);
    if ((ret = io_uring_submit(ring)) != 1) {
        printf("JEB::jeb_fh_map_preload - io_uring_submit ret: %d\n", ret);
        // err? retry?
    }
    ret = consume_event(ring, &cqe);
    // TODO check ret for error

    ret = cqe->res;
    io_uring_cqe_seen(ring, cqe);

    return ret;
}

/* Unmap part of a memory mapped file */
static int 
jeb_fh_map_discard(WT_FILE_HANDLE *file_handle, WT_SESSION *wt_session, void *map, size_t length, void *mapped_cookie) {
    WT_CONNECTION_IMPL *conn;
    WT_SESSION_IMPL *session;
    JEB_FILE_HANDLE *jeb_file_handle;
    JEB_FILE_SYSTEM *jeb_fs;
    struct io_uring *ring;
    struct io_uring_sqe *sqe;
    struct io_uring_cqe *cqe;
    int ret = 0;
    void *blk;

    session = (WT_SESSION_IMPL *)wt_session;
    conn = S2C(session);

    /* Linux requires the address be aligned to a 4KB boundary. */
    blk = (void *)((uintptr_t)map & ~(uintptr_t)(conn->page_size - 1));
    length += WT_PTRDIFF(map, blk);

    _get_uring(&ring);
    sqe = io_uring_get_sqe(ring);
    io_uring_prep_madvise(sqe, blk, length, POSIX_MADV_DONTNEED);
    if ((ret = io_uring_submit(ring)) != 1) {
        printf("JEB::jeb_fh_map_discard - io_uring_submit ret: %d\n", ret);
        // err? retry?
    }
    ret = consume_event(ring, &cqe);
    // TODO check ret for error

    ret = cqe->res;
    io_uring_cqe_seen(ring, cqe);

    return ret;
}

/* Unmap a memory region */
static int 
jeb_fh_unmap(WT_FILE_HANDLE *file_handle, WT_SESSION *session, void *mapped_region, size_t length, void *mapped_cookie) {
    int ret = 0;

    printf("JEB::jeb_fh_unmap %s\n", file_handle->name);
    ret = munmap(mapped_region, length);

    if (ret != 0) {
        printf("JEB::__unmap_file failed to munmap a region of %s, ignoring\n", file_handle->name);
    }

    return ret;
}

// largely borrowed from __wt_map_file
// only called in os_fs.c in _open(), __wt_remap_resize_file() which is called from truncate()/write_mmap
static void
__map_file(WT_FILE_HANDLE *file_handle, WT_SESSION *wt_session) {
    JEB_FILE_HANDLE *jeb_file_handle;
    wt_off_t file_size;
    void *prev_addr;

    jeb_file_handle = (JEB_FILE_HANDLE *)file_handle;
    if (file_handle->fh_size(file_handle, wt_session, &file_size) != 0) {
        jeb_file_handle->mmap_file_mappable = false;
        return;
    }

    if (file_size <= 0) {
        if (jeb_file_handle->mmap_buf != NULL) {
            __unmap_file(file_handle, wt_session);
        }
        return;
    }
    prev_addr = jeb_file_handle->mmap_buf;

    if ((jeb_file_handle->mmap_buf = (uint8_t *)mmap(prev_addr, (size_t)file_size, jeb_file_handle->mmap_prot, 
            MAP_SHARED | MAP_FILE, jeb_file_handle->fd, 0)) == MAP_FAILED ) {
        printf("JEB::__map_file - couldn't mmap file %s\n", file_handle->name);
        jeb_file_handle->mmap_size = 0;
        jeb_file_handle->mmap_buf = NULL;
        jeb_file_handle->mmap_file_mappable = false;
        return;
    }

    jeb_file_handle->mmap_size = file_size;
}

/*
 * __prepare_remap_resize_file --
 *     Wait until all sessions using the mapped region for I/O are done, so it is safe to remap the
 *     file when it changes size.
 */
void
__prepare_remap_resize_file(WT_FILE_HANDLE *file_handle, WT_SESSION *wt_session) {
    JEB_FILE_HANDLE *jeb_file_handle;
    WT_SESSION_IMPL *session;
    uint64_t sleep_usec, yield_count;

    session = (WT_SESSION_IMPL *)wt_session;
    jeb_file_handle = (JEB_FILE_HANDLE *)file_handle;
    sleep_usec = 10;
    yield_count = 0;

    if (!jeb_file_handle->mmap_file_mappable)
        return;

wait:
    /* Wait until it looks like no one is resizing the region */
    while (jeb_file_handle->mmap_resizing == 1)
        __wt_spin_backoff(&yield_count, &sleep_usec);

    if (__wt_atomic_casv32(&jeb_file_handle->mmap_resizing, 0, 1) == false)
        goto wait;


    /*
     * Wait for any sessions using the region for I/O to finish. Now that we have set the resizing
     * flag, new sessions will not use the region, defaulting to system calls instead.
     */
    while (jeb_file_handle->mmap_usecount > 0)
        __wt_spin_backoff(&yield_count, &sleep_usec);

}

/*
 * __release_without_remap --
 *     Signal that we are releasing the mapped buffer we wanted to resize, but do not actually remap
 *     the file. If we set the resizing flag earlier, but the operation that tried to resize the
 *     file did not succeed, we will simply reset the flag without resizing.
 */
void
__release_without_remap(WT_FILE_HANDLE *file_handle)
{
    JEB_FILE_HANDLE *jeb_file_handle;
    jeb_file_handle = (JEB_FILE_HANDLE *)file_handle;

    if (!jeb_file_handle->mmap_file_mappable)
        return;

    /* Signal that we are done resizing the buffer */
    (void)__wt_atomic_subv32(&jeb_file_handle->mmap_resizing, 1);
}

/*
 * __remap_resize_file --
 *     After the file size has changed, unmap the file. Then remap it with the new size.
 */
static void
__remap_resize_file(WT_FILE_HANDLE *file_handle, WT_SESSION *wt_session) {
    JEB_FILE_HANDLE *jeb_file_handle;
    jeb_file_handle = (JEB_FILE_HANDLE *)file_handle;

    if (!jeb_file_handle->mmap_file_mappable)
        return;

    if (jeb_file_handle->mmap_buf != NULL) {
        __unmap_file(file_handle, wt_session);
    }

    __map_file(file_handle, wt_session);

    /* Signal that we are done resizing the buffer */
    (void)__wt_atomic_subv32(&jeb_file_handle->mmap_resizing, 1);
}

/**
 * munmap an entire file
 */
// only called from close(), __map_file on fails, __wt_remap_resize_file() which is called from truncate()/write_mmap
static void
__unmap_file(WT_FILE_HANDLE *file_handle, WT_SESSION *wt_session) {
    JEB_FILE_HANDLE *jeb_file_handle;
    int ret = 0;

    jeb_file_handle = (JEB_FILE_HANDLE *)file_handle;
    ret = munmap(jeb_file_handle->mmap_buf, jeb_file_handle->mmap_size);

    // TODO: is there a race between munmap'ing and setting the buf to NULL? does it matter?
    jeb_file_handle->mmap_buf = NULL;
    jeb_file_handle->mmap_size = 0;

    if (ret != 0) {
        printf("JEB::__unmap_file failed to munmap %s, ignoring\n", file_handle->name);
    }
}

/* ! [JEB :: FILE HANDLE] */