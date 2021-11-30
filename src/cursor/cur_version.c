/*-
 * Copyright (c) 2014-present MongoDB, Inc.
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#include "wt_internal.h"

/*
 * __curversion_set_key --
 *     WT_CURSOR->set_key implementation for version cursors.
 */
static void
__curversion_set_key(WT_CURSOR *cursor, ...)
{
    WT_CURSOR *table_cursor;
    WT_CURSOR_VERSION *version_cursor;

    version_cursor = (WT_CURSOR_VERSION *)cursor;
    table_cursor = version_cursor->table_cursor;
    table_cursor->set_key(table_cursor);
}

/*
 * __curversion_get_key --
 *     WT_CURSOR->get_key implementation for version cursors.
 */
static int
__curversion_get_key(WT_CURSOR *cursor, ...)
{
    WT_CURSOR *table_cursor;
    WT_CURSOR_VERSION *version_cursor;
    WT_DECL_RET;
    va_list ap;

    version_cursor = (WT_CURSOR_VERSION *)cursor;
    table_cursor = version_cursor->table_cursor;
    va_start(ap, cursor);
    WT_ERR(__wt_cursor_get_keyv(cursor, cursor->flags, ap));
    WT_ERR(__wt_cursor_get_keyv(table_cursor, table_cursor->flags, ap));

err:
    va_end(ap);
    return (ret);
}

/*
 * __curversion_next --
 *     WT_CURSOR->next method for version cursors. The next function will position the cursor on the
 *     next update of the key it is positioned at. We traverse through updates on the update chain,
 *     then the ondisk value, and finally from the history store.
 */
static int
__curversion_next(WT_CURSOR *cursor)
{
    WT_BTREE *btree;
    WT_CELL *cell;
    WT_CELL_UNPACK_KV *vpack, _vpack;
    WT_COL *cip;
    WT_CURSOR *hs_cursor, *table_cursor;
    WT_CURSOR_BTREE *cbt;
    WT_CURSOR_VERSION *version_cursor;
    WT_DECL_RET;
    WT_ITEM hs_key, hs_value;
    WT_PAGE *page;
    WT_ROW *rip;
    WT_SESSION_IMPL *session;
    WT_TIME_WINDOW tw;
    WT_UPDATE *upd;
    wt_timestamp_t durable_stop_ts, stop_ts;
    uint64_t hs_counter, stop_txn;
    uint64_t recno, rle;
    uint32_t hs_btree_id, hs_upd_type;
    bool upd_found;

    upd_found = false;
    version_cursor = (WT_CURSOR_VERSION *)cursor;
    hs_cursor = version_cursor->hs_cursor;
    table_cursor = version_cursor->table_cursor;
    cbt = (WT_CURSOR_BTREE *)table_cursor;
    btree = CUR2BT(cbt);
    vpack = &_vpack;
    CURSOR_API_CALL(cursor, session, next, NULL);

    /* The cursor should be positioned, otherwise the next call will fail. */
    if (!F_ISSET(cursor, WT_CURSTD_KEY_SET)) {
        WT_IGNORE_RET(__wt_msg(
          session, "WT_ROLLBACK: rolling back version_cursor->next due to no initial position"));
        WT_ERR(WT_ROLLBACK);
    }

    upd = version_cursor->next_upd;

    if (!upd_found && !F_ISSET(version_cursor, WT_VERSION_CUR_UPDATE_EXHAUSTED)) {
        /*
         * If the update is an aborted update, we want to skip to the next update immediately or get
         * the ondisk value if the update is the last one in the update chain.
         */
        while (upd != NULL && upd->txnid == WT_TXN_ABORTED)
            upd = upd->next;

        if (upd == NULL)
            F_SET(version_cursor, WT_VERSION_CUR_UPDATE_EXHAUSTED);
        else {
            /*
             * If the update is a tombstone, we still want to record the stop information but we
             * also need traverse to the next update to get the full value. If the tombstone was the
             * last update in the update list, retrieve the ondisk value.
             */
            version_cursor->upd_stop_txnid = upd->txnid;
            version_cursor->upd_durable_stop_ts = upd->durable_ts;
            version_cursor->upd_stop_ts = upd->start_ts;
            if (upd->type == WT_UPDATE_TOMBSTONE) {
                upd = upd->next;

                /* Make sure the next update is not an aborted update. */
                while (upd != NULL && upd->txnid == WT_TXN_ABORTED)
                    upd = upd->next;
            }

            if (upd == NULL)
                F_SET(version_cursor, WT_VERSION_CUR_UPDATE_EXHAUSTED);
            else {
                /*
                 * Set the version cursor's key, which contains all the record metadata for that
                 * particular version of the update.
                 */
                __wt_cursor_set_key(cursor, upd->txnid, upd->start_ts, upd->durable_ts,
                  version_cursor->upd_stop_txnid, version_cursor->upd_stop_ts,
                  version_cursor->upd_durable_stop_ts, upd->type, upd->prepare_state, upd->flags,
                  WT_VERSION_UPDATE_CHAIN);

                /*
                 * Copy the update value into the version cursor as we don't know the value format.
                 * If the update is a modify, reconstruct the value.
                 */
                if (upd->type != WT_UPDATE_MODIFY)
                    __wt_upd_value_assign(cbt->upd_value, upd);
                else
                    WT_ERR(
                      __wt_modify_reconstruct_from_upd_list(session, cbt, upd, cbt->upd_value));

                cursor->value.data = cbt->upd_value->buf.data;
                cursor->value.size = cbt->upd_value->buf.size;
                F_SET(cursor, WT_CURSTD_VALUE_INT);

                upd_found = true;
                version_cursor->next_upd = upd->next;
            }
        }
    }

    if (!upd_found && !F_ISSET(version_cursor, WT_VERSION_CUR_ON_DISK_EXHAUSTED)) {
        /* If the key is on an insert list only, there is no ondisk value nor history store value.
         */
        if (cbt->ins || WT_COL_APPEND(page) != NULL) {
            F_SET(version_cursor, WT_VERSION_CUR_ON_DISK_EXHAUSTED);
            F_SET(version_cursor, WT_VERSION_CUR_HS_EXAUSTED);
        } else {
            page = cbt->ref->page;

            /* Retrieve the value for each type of underlying table structure. */
            switch (page->type) {
            case WT_PAGE_ROW_LEAF:
                rip = &page->pg_row[cbt->slot];
                __wt_row_leaf_value_cell(session, cbt->ref->page, rip, vpack);
                break;
            case WT_PAGE_COL_FIX:
                break;
            case WT_PAGE_COL_VAR:
                recno = cbt->ref->ref_recno;
                cip = &page->pg_var[cbt->slot];
                cell = WT_COL_PTR(page, cip);
                __wt_cell_unpack_kv(session, page->dsk, cell, vpack);
                break;
            default:
                WT_ERR(__wt_illegal_value(session, page->type));
                break;
            }

            if (!WT_TIME_WINDOW_HAS_STOP(&vpack->tw)) {
                durable_stop_ts = version_cursor->upd_durable_stop_ts;
                stop_ts = version_cursor->upd_stop_ts;
                stop_txn = version_cursor->upd_stop_txnid;
            } else {
                durable_stop_ts = vpack->tw.durable_stop_ts;
                stop_ts = vpack->tw.stop_ts;
                stop_txn = vpack->tw.stop_txn;
            }

            __wt_cursor_set_key(cursor, vpack->tw.start_txn, vpack->tw.start_ts,
              vpack->tw.durable_start_ts, stop_txn, stop_ts, durable_stop_ts, 0, 0, 0,
              WT_VERSION_DISK_IMAGE);
            cursor->value.data = vpack->data;
            cursor->value.size = vpack->size;
            F_SET(cursor, WT_CURSTD_VALUE_INT);

            upd_found = true;
            F_SET(version_cursor, WT_VERSION_CUR_ON_DISK_EXHAUSTED);
        }
    }

    if (!upd_found && !F_ISSET(version_cursor, WT_VERSION_CUR_HS_EXAUSTED)) {
        /*
         * If the history store cursor is not yet positioned, then we are traversing the history
         * store versions for the first time.
         */
        if (!F_ISSET(hs_cursor, WT_CURSTD_KEY_INT)) {
            hs_cursor->set_key(hs_cursor, 4, S2BT(session)->id, cursor->key, WT_TS_MAX, UINT64_MAX);
            WT_ERR_NOTFOUND_OK(__wt_curhs_search_near_before(session, hs_cursor), true);
        } else
            WT_ERR_NOTFOUND_OK(ret = hs_cursor->prev(hs_cursor), true);

        /*
         * If there are no history store records for the given key or if we have iterated through
         * all the records already, we have exhausted the history store.
         */
        if (ret == 0) {
            WT_TIME_WINDOW_INIT(&tw);
            hs_cursor->get_key(hs_cursor, &hs_btree_id, &hs_key, &tw.start_ts, &hs_counter);
            hs_cursor->get_value(
              hs_cursor, &tw.stop_ts, &tw.durable_start_ts, &hs_upd_type, &hs_value);
            __wt_cursor_set_key(cursor, tw.start_txn, tw.start_ts, tw.durable_start_ts, tw.stop_txn,
              tw.stop_ts, tw.durable_stop_ts, hs_upd_type, 0, 0, WT_VERSION_HISTORY_STORE);

            /*
             * Reconstruct the history store value if needed. Since we save the value inside the
             * version cursor every time we traverse a version, we can simply apply the modify onto
             * the latest value.
             */
            if (hs_upd_type == WT_UPDATE_MODIFY) {
                WT_ERR(__wt_modify_apply_item(
                  session, table_cursor->value_format, &cursor->value, hs_value.data));
            } else {
                WT_ASSERT(session, hs_upd_type == WT_UPDATE_STANDARD);
                cursor->value.data = hs_value.data;
                cursor->value.size = hs_value.size;
            }
            F_SET(cursor, WT_CURSTD_VALUE_INT);
            upd_found = true;
        } else
            F_SET(version_cursor, WT_VERSION_CUR_HS_EXAUSTED);
    }

done:
    if (0) {
err:
        WT_TRET(cursor->reset(cursor));
    }
    API_END_RET(session, ret);
}

/*
 * __curversion_reset --
 *     WT_CURSOR::reset for version cursors.
 */
static int
__curversion_reset(WT_CURSOR *cursor)
{
    WT_CURSOR *hs_cursor, *table_cursor;
    WT_CURSOR_VERSION *version_cursor;
    WT_DECL_RET;
    WT_SESSION_IMPL *session;

    version_cursor = (WT_CURSOR_VERSION *)cursor;
    hs_cursor = version_cursor->hs_cursor;
    table_cursor = version_cursor->table_cursor;
    CURSOR_API_CALL(cursor, session, reset, NULL);

    if (table_cursor != NULL)
        WT_TRET(table_cursor->reset(table_cursor));
    if (hs_cursor != NULL)
        WT_TRET(hs_cursor->reset(hs_cursor));
    version_cursor->next_upd = NULL;
    version_cursor->flags = 0;
    F_CLR(cursor, WT_CURSTD_KEY_SET);
    F_CLR(cursor, WT_CURSTD_VALUE_SET);

err:
    API_END_RET(session, ret);
}

/*
 * __curversion_search --
 *     WT_CURSOR->search method for version cursors.
 */
static int
__curversion_search(WT_CURSOR *cursor)
{
    WT_COL *cip;
    WT_CURSOR *table_cursor;
    WT_CURSOR_BTREE *cbt;
    WT_CURSOR_VERSION *version_cursor;
    WT_DECL_RET;
    WT_INSERT *ins;
    WT_PAGE *page;
    WT_ROW *rip;
    WT_SESSION_IMPL *session;
    WT_UPDATE *upd;
    bool key_only;

    version_cursor = (WT_CURSOR_VERSION *)cursor;
    table_cursor = version_cursor->table_cursor;
    key_only = F_ISSET(cursor, WT_CURSTD_KEY_ONLY);

    /*
     * For now, we assume that we are using simple cursors only.
     */
    cbt = (WT_CURSOR_BTREE *)table_cursor;
    CURSOR_API_CALL(cursor, session, search, CUR2BT(cbt));
    WT_ERR(__cursor_checkkey(table_cursor));

    /* Do a search and position on they key if it is found */
    F_SET(cursor, WT_CURSTD_KEY_ONLY);
    WT_ERR(__wt_btcur_search(cbt));
    WT_ASSERT(session, F_ISSET(cbt, WT_CURSTD_KEY_SET));

    /*
     * If we position on a key, set next update of the version cursor to be the first update on the
     * key if any.
     */
    page = cbt->ref->page;
    switch (page->type) {
    case WT_PAGE_ROW_LEAF:
        rip = &page->pg_row[cbt->slot];
        if (cbt->ins != NULL)
            version_cursor->next_upd = ins->upd;
        else if ((upd = WT_ROW_UPDATE(page, rip)) != NULL)
            version_cursor->next_upd = upd;
        else {
            version_cursor->next_upd = NULL;
            F_SET(version_cursor, WT_VERSION_CUR_UPDATE_EXHAUSTED);
        }
    case WT_PAGE_COL_FIX:
        break;
    case WT_PAGE_COL_VAR:
        cip = &page->pg_var[cbt->slot];
        if ((upd = WT_COL_APPEND(page)) != NULL)
            version_cursor->next_upd = upd;
        else if ((upd = WT_COL_UPDATE(page, cip)) != NULL)
            version_cursor->next_upd = upd;
        else {
            version_cursor->next_upd = NULL;
            F_SET(version_cursor, WT_VERSION_CUR_UPDATE_EXHAUSTED);
        }
        break;
    default:
        WT_ERR(__wt_illegal_value(session, page->type));
        break;
    }

err:
    if (!key_only)
        F_CLR(cursor, WT_CURSTD_KEY_ONLY);
    API_END_RET(session, ret);
}

/*
 * __curversion_close --
 *     WT_CURSOR->close method for version cursors.
 */
static int
__curversion_close(WT_CURSOR *cursor)
{
    WT_CURSOR *hs_cursor, *table_cursor;
    WT_CURSOR_VERSION *version_cursor;
    WT_DECL_RET;
    WT_SESSION_IMPL *session;

    version_cursor = (WT_CURSOR_VERSION *)cursor;
    hs_cursor = version_cursor->hs_cursor;
    table_cursor = version_cursor->table_cursor;
    CURSOR_API_CALL(cursor, session, close, NULL);
err:
    version_cursor->next_upd = NULL;
    if (table_cursor != NULL)
        WT_TRET(table_cursor->close(table_cursor));
    if (hs_cursor != NULL)
        WT_TRET(hs_cursor->close(hs_cursor));
    __wt_cursor_close(cursor);

    API_END_RET(session, ret);
}

/*
 * __wt_curversion_open --
 *     Initialize a version cursor.
 */
int
__wt_curversion_open(WT_SESSION_IMPL *session, const char *uri, WT_CURSOR *owner, const char *cfg[],
  WT_CURSOR **cursorp)
{
    WT_CURSOR_STATIC_INIT(iface, __curversion_get_key, /* get-key */
      __wt_cursor_get_value,                           /* get-value */
      __curversion_set_key,                            /* set-key */
      __wt_cursor_set_value_notsup,                    /* set-value */
      __wt_cursor_compare_notsup,                      /* compare */
      __wt_cursor_equals_notsup,                       /* equals */
      __curversion_next,                               /* next */
      __wt_cursor_notsup,                              /* prev */
      __curversion_reset,                              /* reset */
      __curversion_search,                             /* search */
      __wt_cursor_search_near_notsup,                  /* search-near */
      __wt_cursor_notsup,                              /* insert */
      __wt_cursor_modify_notsup,                       /* modify */
      __wt_cursor_notsup,                              /* update */
      __wt_cursor_notsup,                              /* remove */
      __wt_cursor_notsup,                              /* reserve */
      __wt_cursor_reconfigure_notsup,                  /* reconfigure */
      __wt_cursor_notsup,                              /* largest_key */
      __wt_cursor_notsup,                              /* cache */
      __wt_cursor_reopen_notsup,                       /* reopen */
      __curversion_close);                             /* close */

    WT_CURSOR *cursor;
    WT_CURSOR_VERSION *version_cursor;
    WT_DECL_RET;
    /* The table cursor is read only. */
    const char *table_cursor_cfg[] = {
      WT_CONFIG_BASE(session, WT_SESSION_open_cursor), "read_only=true", NULL};

    *cursorp = NULL;
    WT_RET(__wt_calloc_one(session, &version_cursor));
    cursor = (WT_CURSOR *)version_cursor;
    *cursor = iface;
    cursor->session = (WT_SESSION *)session;

    /* Open the table cursor. */
    WT_ERR(__wt_open_cursor(session, uri, cursor, table_cursor_cfg, &version_cursor->table_cursor));
    cursor->key_format = WT_UNCHECKED_STRING(QQQQQQBBBB);
    cursor->value_format = version_cursor->table_cursor->value_format;
    WT_ERR(__wt_strdup(session, uri, &cursor->uri));

    /* Open the history store cursor for operations on the regular history store .*/
    WT_ERR(__wt_curhs_open(session, cursor, &version_cursor->hs_cursor));
    F_SET(version_cursor->hs_cursor, WT_CURSTD_HS_READ_COMMITTED);

    /* Initialize information used to track update metadata. */
    version_cursor->upd_stop_txnid = WT_TXN_MAX;
    version_cursor->upd_durable_stop_ts = WT_TS_MAX;
    version_cursor->upd_stop_ts = WT_TS_MAX;

    WT_ERR(__wt_cursor_init(cursor, cursor->uri, owner, cfg, cursorp));

    if (0) {
err:
        WT_TRET(cursor->close(cursor));
        *cursorp = NULL;
    }
    return (ret);
}
