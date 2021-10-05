/*-
 * Public Domain 2014-present MongoDB, Inc.
 * Public Domain 2008-2014 WiredTiger, Inc.
 *
 * This is free and unencumbered software released into the public domain.
 *
 * Anyone is free to copy, modify, publish, use, compile, sell, or
 * distribute this software, either in source code form or as a compiled
 * binary, for any purpose, commercial or non-commercial, and by any
 * means.
 *
 * In jurisdictions that recognize copyright laws, the author or authors
 * of this software dedicate any and all copyright interest in the
 * software to the public domain. We make this dedication for the benefit
 * of the public at large and to the detriment of our heirs and
 * successors. We intend this dedication to be an overt act of
 * relinquishment in perpetuity of all present and future rights to this
 * software under copyright law.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 * EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
 * IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR
 * OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,
 * ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
 * OTHER DEALINGS IN THE SOFTWARE.
 */
#include "test_util.h"

/*
 *
 */

/* If you modify number of records or data content, make sure to update DATA_SIZE accordingly. */
#define NUM_RECORDS 3000000

/* Constants and variables declaration. */
/*
 * You may want to add "verbose=[compact,compact_progress]" to the connection config string to get
 * better view on what is happening.
 */
static const char conn_config[] =
  "create,cache_size=1GB,statistics=(all),statistics_log=(wait=1,json),verbose=[compact,compact_"
  "progress]";
static const char table_config[] =
  "allocation_size=4KB,leaf_page_max=4KB,key_format=i,value_format=QQQS";
static char data_str[1024] = "";
bool volatile stop_updates = false;

/* Structures definition. */
struct thread_data {
    WT_CONNECTION *conn;
    const char *uri;
    uint32_t begin, end;
    bool update;
};

/* Forward declarations. */
static void run_test(const char *home, const char *uri);
static void *test_thread_func(void *arg);
static void *thread_func_compact(void *arg);
static void populate(WT_SESSION *session, const char *uri);
static void remove_records(WT_SESSION *session, const char *uri);
static void workload(WT_SESSION *session, const char *uri, uint32_t begin, uint32_t end, bool update);
static uint64_t get_file_size(WT_SESSION *session, const char *uri);

/* Methods implementation. */
int
main(int argc, char *argv[])
{
    TEST_OPTS *opts, _opts;

    opts = &_opts;
    memset(opts, 0, sizeof(*opts));
    testutil_check(testutil_parse_opts(argc, argv, opts));

    run_test(opts->home, opts->uri);

    testutil_cleanup(opts);

    return (EXIT_SUCCESS);
}

#define THREADS_NUM 10

static void
run_test(const char *home, const char *uri)
{
    WT_CONNECTION *conn;
    WT_SESSION *session;
    pthread_t thread_compact, threads_workload[THREADS_NUM];
    uint64_t file_sz_after;
    int i;
    struct thread_data td[THREADS_NUM] = {
      {NULL, uri, 1, NUM_RECORDS / 3, true},
      {NULL, uri, (NUM_RECORDS * 2) / 3, NUM_RECORDS, false},
      {NULL, uri, 1, NUM_RECORDS / 3, false},
      {NULL, uri, (NUM_RECORDS * 2) / 3, NUM_RECORDS, true},
      {NULL, uri, 1, NUM_RECORDS / 3, true},
      {NULL, uri, (NUM_RECORDS * 2) / 3, NUM_RECORDS, false},
      {NULL, uri, 1, NUM_RECORDS / 3, false},
      {NULL, uri, (NUM_RECORDS * 2) / 3, NUM_RECORDS, true},
      {NULL, uri, 1, NUM_RECORDS / 3, true},
      {NULL, uri, (NUM_RECORDS * 2) / 3, NUM_RECORDS, false},
    };

    testutil_make_work_dir(home);
    testutil_check(wiredtiger_open(home, NULL, conn_config, &conn));

    testutil_check(conn->open_session(conn, NULL, NULL, &session));

    /* Create and populate table. Checkpoint the data after that. */
    testutil_check(session->create(session, uri, table_config));

    printf("Populating table...\n");
    populate(session, uri);
    testutil_check(session->checkpoint(session, NULL));

    /*
     * Remove 1/3 of data from the middle of the key range to let compact relocate blocks from the
     * end of the file. Checkpoint the changes after the removal.
     */
    printf("Removing records...\n");
    remove_records(session, uri);
    testutil_check(session->checkpoint(session, NULL));

    td[0].conn = conn;
    testutil_check(pthread_create(&thread_compact, NULL, thread_func_compact, &td[0]));

    for (i = 0; i < THREADS_NUM; i++) {
        td[i].conn = conn;
        testutil_check(pthread_create(&threads_workload[i], NULL, test_thread_func, &td[i]));
        //__wt_sleep(1, 0);
    }


    /* Wait for the threads to finish the work. */
    for (i = 0; i < THREADS_NUM; i++)
        (void)pthread_join(threads_workload[i], NULL);

    (void)pthread_join(thread_compact, NULL);

    testutil_check(session->checkpoint(session, NULL));
    file_sz_after = get_file_size(session, uri);

    /* Wait for stat */
    __wt_sleep(2, 0);

    testutil_check(session->close(session, NULL));
    session = NULL;

    testutil_check(conn->close(conn, NULL));
    conn = NULL;

    printf(" - Compressed file size is: %f MB\n", file_sz_after / (1024.0 * 1024));
    testutil_assert(file_sz_after / (1024.0 * 1024) < 4000);
}

static void *
test_thread_func(void *arg)
{
    struct thread_data *td;
    WT_SESSION *session;

    td = (struct thread_data *)arg;

    testutil_check(td->conn->open_session(td->conn, NULL, NULL, &session));

    WT_STAT_CONN_INCR((WT_SESSION_IMPL *)session, session_table_compact_running_updates);
    printf(td->update ? "Updating records...\n" : "Reading records...\n");
    workload(session, td->uri, td->begin, td->end, td->update);
    printf(td->update ? "Updating finished.\n" : "Reading finished.\n");
    WT_STAT_CONN_DECR((WT_SESSION_IMPL *)session, session_table_compact_running_updates);

    testutil_check(session->close(session, NULL));
    session = NULL;

    return (NULL);
}

static void *
thread_func_compact(void *arg)
{
    struct thread_data *td;
    WT_SESSION *session;

    td = (struct thread_data *)arg;

    testutil_check(td->conn->open_session(td->conn, NULL, NULL, &session));

    /* Perform compact operation. */
    printf("Compacting table...\n");
    testutil_check(session->compact(session, td->uri, NULL));
    printf("Compacting finished.\n");
    stop_updates = true;

    testutil_check(session->close(session, NULL));
    session = NULL;

    return (NULL);
}

static void
populate(WT_SESSION *session, const char *uri)
{
    WT_CURSOR *cursor;
    WT_RAND_STATE rnd;
    uint64_t val;
    int i, str_len;

    __wt_random_init_seed((WT_SESSION_IMPL *)session, &rnd);

    str_len = sizeof(data_str) / sizeof(data_str[0]);
    for (i = 0; i < str_len - 1; i++)
        data_str[i] = 'a' + __wt_random(&rnd) % 26;

    data_str[str_len - 1] = '\0';

    testutil_check(session->open_cursor(session, uri, NULL, NULL, &cursor));
    for (i = 0; i < NUM_RECORDS; i++) {
        cursor->set_key(cursor, i);
        val = (uint64_t)__wt_random(&rnd);
        cursor->set_value(cursor, val, val, val, data_str);
        testutil_check(cursor->insert(cursor));
    }

    testutil_check(cursor->close(cursor));
    cursor = NULL;
}

static void
remove_records(WT_SESSION *session, const char *uri)
{
    WT_CURSOR *cursor;
    int i;

    testutil_check(session->open_cursor(session, uri, NULL, NULL, &cursor));

    /* Remove 1/3 of the records from the middle of the key range. */
    for (i = NUM_RECORDS / 3; i < (NUM_RECORDS * 2) / 3; i++) {
        cursor->set_key(cursor, i);
        testutil_check(cursor->remove(cursor));
    }

    testutil_check(cursor->close(cursor));
    cursor = NULL;
}

static void
workload(WT_SESSION *session, const char *uri, uint32_t begin, uint32_t end, bool update)
{
    WT_CURSOR *cursor;
    size_t buf_size;
    uint32_t i;
    uint64_t val1, val2, val3;
    char *str;
    WT_RAND_STATE rs;
    int key;

    __wt_random_init_seed((WT_SESSION_IMPL*)session, &rs);

    testutil_check(session->open_cursor(session, uri, NULL, NULL, &cursor));

    buf_size = sizeof(data_str) / sizeof(data_str[0]);
    memset(data_str, 'a', buf_size - 1);
    data_str[buf_size - 1] = '\0';
    
    for (i = begin; !stop_updates && i < end; i++) {
        key = (int)(__wt_random(&rs) % (end - begin) + begin);
        cursor->set_key(cursor, key);
        
        if (update) {
            /* Update record. */
            cursor->set_value(cursor, 1, 2, 3, data_str);
            testutil_check(cursor->update(cursor));
        }
        else {
            /* Read random record. */
            testutil_check(cursor->search(cursor));
            testutil_check(cursor->get_value(cursor, &val1, &val2, &val3, &str));
        }
    }

    testutil_check(cursor->close(cursor));
    cursor = NULL;
}

static uint64_t
get_file_size(WT_SESSION *session, const char *uri)
{
    WT_CURSOR *cur_stat;
    uint64_t val;
    char *descr, *str_val, stat_uri[128];

    sprintf(stat_uri, "statistics:%s", uri);
    testutil_check(session->open_cursor(session, stat_uri, NULL, "statistics=(all)", &cur_stat));
    cur_stat->set_key(cur_stat, WT_STAT_DSRC_BLOCK_SIZE);
    testutil_check(cur_stat->search(cur_stat));
    testutil_check(cur_stat->get_value(cur_stat, &descr, &str_val, &val));
    testutil_check(cur_stat->close(cur_stat));
    cur_stat = NULL;

    return (val);
}
