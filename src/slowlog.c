/* Slowlog implements a system that is able to remember the latest N
 * queries that took more than M microseconds to execute.
 *
 * The execution time to reach to be logged in the slow log is set
 * using the 'slowlog-log-slower-than' config directive, that is also
 * readable and writable using the CONFIG SET/GET command.
 *
 * The slow queries log is actually not "logged" in the Redis log file
 * but is accessible thanks to the SLOWLOG command.
 *
 * ----------------------------------------------------------------------------
 *
 * Copyright (c) 2009-2012, Salvatore Sanfilippo <antirez at gmail dot com>
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 *   * Redistributions of source code must retain the above copyright notice,
 *     this list of conditions and the following disclaimer.
 *   * Redistributions in binary form must reproduce the above copyright
 *     notice, this list of conditions and the following disclaimer in the
 *     documentation and/or other materials provided with the distribution.
 *   * Neither the name of Redis nor the names of its contributors may be used
 *     to endorse or promote products derived from this software without
 *     specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 */


#include "server.h"
#include "slowlog.h"
#include "orbit.h"
#include <assert.h>

#undef DEBUG_ORBIT

#ifdef DEBUG_ORBIT
#define printd(fmt, ...)                        \
    do {                                        \
        fprintf(stderr, fmt, ##__VA_ARGS__);    \
    } while (0)
#else
#define printd(fmt, ...) do {} while (0)
#endif

#define whatis(x) printd(#x " is %lu\n", (unsigned long)(x))
#define whatisp(x) printd(#x " is %p\n", (void*)(x))


/* Create a new slowlog entry.
 * Incrementing the ref count of all the objects retained is up to
 * this function. */
slowlogEntry *slowlogCreateEntry(client *c, robj **argv, int argc, long long duration) {
    slowlogEntry *se = zmalloc(sizeof(*se));
    int j, slargc = argc;

    if (slargc > SLOWLOG_ENTRY_MAX_ARGC) slargc = SLOWLOG_ENTRY_MAX_ARGC;
    se->argc = slargc;
    se->argv = zmalloc(sizeof(robj*)*slargc);
    for (j = 0; j < slargc; j++) {
        /* Logging too many arguments is a useless memory waste, so we stop
         * at SLOWLOG_ENTRY_MAX_ARGC, but use the last argument to specify
         * how many remaining arguments there were in the original command. */
        if (slargc != argc && j == slargc-1) {
            se->argv[j] = createObject(OBJ_STRING,
                sdscatprintf(sdsempty(),"... (%d more arguments)",
                argc-slargc+1));
        } else {
            /* Trim too long strings as well... */
            whatisp(argv);
            whatisp(argv[j]);
            if (argv[j]->type == OBJ_STRING &&
                sdsEncodedObject(argv[j]) &&
                sdslen(argv[j]->ptr) > SLOWLOG_ENTRY_MAX_STRING)
            {
                sds s = sdsnewlen(argv[j]->ptr, SLOWLOG_ENTRY_MAX_STRING);

                s = sdscatprintf(s,"... (%lu more bytes)",
                    (unsigned long)
                    sdslen(argv[j]->ptr) - SLOWLOG_ENTRY_MAX_STRING);
                se->argv[j] = createObject(OBJ_STRING,s);
            } else if (argv[j]->refcount == OBJ_SHARED_REFCOUNT) {
                /* how will we have those? */
                printd("Getting shared obj\n");
                se->argv[j] = argv[j];
            } else {
                /* Copy string object out of orbit pool.
                 * FIXME: reduce one time of copy */
                se->argv[j] = dupStringObject(argv[j]);
            }
        }
    }
    se->time = time(NULL);
    se->duration = duration;
    se->id = server.slowlog_entry_id++;
    se->peerid = sdsnew(c->peerid);
    se->cname = c->name ? sdsnew(c->name->ptr) : sdsempty();
    return se;
}

/* Free a slow log entry. The argument is void so that the prototype of this
 * function matches the one of the 'free' method of adlist.c.
 *
 * This function will take care to release all the retained object. */
void slowlogFreeEntry(void *septr) {
    slowlogEntry *se = septr;
    int j;

    /* FIXME: free in orbit */
    for (j = 0; j < se->argc; j++)
        decrRefCount(se->argv[j]);
    zfree(se->argv);
    sdsfree(se->peerid);
    sdsfree(se->cname);
    zfree(se);
}

struct orbit_module *slowlog_orbit;
struct orbit_pool *slowlog_pool, *slowlog_scratch_pool;
/* Fix linking */
extern struct orbit_allocator *slowlog_alloc;

struct orbit_scratch slowlog_scratch;
unsigned long slowlogPushEntry_orbit(void *store, void *args);

void *slowlogInit_orbit(void) {
    server.slowlog = listCreate();
    server.slowlog_entry_id = 0;
    listSetFreeMethod(server.slowlog,slowlogFreeEntry);
    return NULL;
}

static long long call_tot_time = 0, cnt = 0;
static long long cmd_tot_time = 0;
long long query_tot_time = 0;

int showThroughput(struct aeEventLoop *eventLoop, long long id, void *clientData) {
    UNUSED(eventLoop);
    UNUSED(id);
    UNUSED(clientData);

    static bool last_zero = false;
    if (!last_zero || cnt != 0) {
        printf("POOL: %lu\nOCALL: %f\n", slowlog_pool->used, cnt ? (double)call_tot_time / cnt : -1);
        printf("CMD: %f\n", cnt ? (double)cmd_tot_time / cnt : -1);
        printf("QUERY: %f\n", cnt ? (double)query_tot_time / cnt : -1);
    }
    last_zero = (cnt == 0);
    call_tot_time = cnt = 0;
    cmd_tot_time = 0;
    query_tot_time = 0;
    fflush(stdout);
    return 250; /* every 250ms */
}

/* Initialize the slow log. This function should be called a single time
 * at server startup. */
void slowlogInit(void) {
    slowlog_pool = orbit_pool_create_at(NULL, 256 * 1024 * 1024, (void*)0x820000000UL);
    slowlog_alloc = orbit_allocator_from_pool(slowlog_pool, true);

    slowlog_scratch_pool = orbit_pool_create_at(NULL, 1024 * 1024, (void*)0x810000000UL);
    slowlog_scratch_pool->mode = ORBIT_MOVE;
    orbit_scratch_set_pool(slowlog_scratch_pool);

    slowlog_orbit = orbit_create("slowlog", slowlogPushEntry_orbit, slowlogInit_orbit);
}

/* Push a new entry into the slow log.
 * This function will make sure to trim the slow log accordingly to the
 * configured max length. */
void slowlogPushEntry_real(client *c, robj **argv, int argc, long long duration) {
    listAddNodeHead(server.slowlog, slowlogCreateEntry(c,argv,argc,duration));
    /* Remove old entries if needed. */
    while (listLength(server.slowlog) > server.slowlog_max_len)
        listDelNode(server.slowlog,listLast(server.slowlog));
}

typedef struct _slowlog_push_orbit_args {
    client *c;
    robj **argv;
    int argc;
    long long duration;
} slowlog_push_orbit_args;

unsigned long slowlogPushEntry_orbit(void *store, void *_args) {
    (void)store;
    slowlog_push_orbit_args *args = (slowlog_push_orbit_args*)_args;
    slowlogPushEntry_real(args->c, args->argv, args->argc, args->duration);
    return 0;
}

void slowlogPushEntryIfNeeded_inner(client *c, robj **argv, int argc, long long duration) {
    if (server.slowlog_log_slower_than < 0) return; /* Slowlog disabled */
    /* FIXME: this changes default behavior that trims slowlog entires every time */
    if (!(duration >= server.slowlog_log_slower_than)) return;

    printd("In slowlog push\n");

    // pre-get c->peerid
    // can this be put into orbit?
    getClientPeerId(c);

    // slowlogPushEntryIfNeededReal(c, argv, argc, duration);
    slowlog_push_orbit_args args = { c, argv, argc, duration, };
    orbit_call_async(slowlog_orbit, ORBIT_NORETVAL, 1, &slowlog_pool, NULL, &args, sizeof(args), NULL);
}

void slowlogPushEntryIfNeeded(client *c, robj **argv, int argc, long long duration) {
    static int inited = 0;
    if (!inited) {
        inited = 1;
        aeCreateTimeEvent(server.el,1,showThroughput,NULL,NULL);
    }

    long long start = ustime();
    slowlogPushEntryIfNeeded_inner(c, argv, argc, duration);
    call_tot_time += ustime() - start;
    cmd_tot_time += duration;
    ++cnt;
}

/* Remove all the entries from the current slow log. */
void slowlogReset(void) {
    while (listLength(server.slowlog) > 0)
        listDelNode(server.slowlog,listLast(server.slowlog));
}

typedef struct _slowlog_command_args {
    int argc;
    robj **argv;
} slowlog_command_args;

unsigned long slowlogCommand_orbit(void *store, void *_args) {
    (void)store;
    slowlog_command_args *c = (slowlog_command_args *)_args;

    unsigned long ret = 0;

    printd("In slowlog query\n");

    orbit_scratch_create(&slowlog_scratch);

    slowlog_alloc = orbit_scratch_open_any(&slowlog_scratch, true);

    if (c->argc == 2 && !strcasecmp(c->argv[1]->ptr,"reset")) {
        slowlogReset();
    } else if (c->argc == 2 && !strcasecmp(c->argv[1]->ptr,"len")) {
        long long *len = orbit_alloc(slowlog_alloc, sizeof(long long));
        *len = listLength(server.slowlog);
        ret = (unsigned long)len;
    } else if ((c->argc == 2 || c->argc == 3) &&
               !strcasecmp(c->argv[1]->ptr,"get"))
    {
        long count = 10;
        listIter li;
        listNode *ln;
        slowlogEntry *se, *se_dup;

        if (c->argc == 3 &&
            getLongFromObject(c->argv[2],&count) != C_OK)
            goto exit;

        /* Duplicate slowlog list in the scratch. */
        list *slowlog_dup = listCreate_orbit();

        listRewind(server.slowlog,&li);
        while(count-- && (ln = listNext(&li))) {
            int j;

            se = ln->value;
            se_dup = orbit_alloc(slowlog_alloc, sizeof(*se_dup));

            *se_dup = *se;
            se_dup->argv = orbit_alloc(slowlog_alloc, sizeof(robj*)*se->argc);
            for (j = 0; j < se->argc; j++)
                se_dup->argv[j] = dupStringObject_orbit(se->argv[j]);
            se_dup->peerid = sdsdup_orbit(se->peerid);
            se_dup->cname = sdsdup_orbit(se->cname);

            listAddNodeHead_orbit(slowlog_dup, se_dup);
        }

        ret = (unsigned long)slowlog_dup;
    } else {
        /* TODO: put error reply in orbit side? */
    }

exit:
    /* Send query result. Open "any" will automatically be closed. */
    /* TODO: use specialized query API */
    orbit_sendv(&slowlog_scratch);

    return ret;
}

/* The SLOWLOG command. Implements all the subcommands needed to handle the
 * Redis slow log. */
void slowlogCommand(client *c) {
    struct orbit_task task;
    union orbit_result result;
    union orbit_result retval;
    int ret;

    slowlog_command_args args = { c->argc, c->argv, };
    orbit_call_async(slowlog_orbit, 0, 1, &slowlog_pool, slowlogCommand_orbit,
                     &args, sizeof(args), &task);

    ret = orbit_recvv(&result, &task);
    printd("orbit_recvv returns %d\n", ret);
    assert(ret == 1);

    ret = orbit_recvv(&retval, &task);
    printd("orbit_recvv returns %d\n", ret);
    assert(ret == 0);

    if (c->argc == 2 && !strcasecmp(c->argv[1]->ptr,"reset")) {
        addReply(c,shared.ok);
    } else if (c->argc == 2 && !strcasecmp(c->argv[1]->ptr,"len")) {
        struct orbit_repr *len = orbit_scratch_first(&result.scratch);

        whatis(len->type); whatis(len->any.length);

        assert(len && len->type == ORBIT_ANY);
        addReplyLongLong(c,*(long long*)retval.retval);

        whatisp(retval.retval); whatisp(len->any.data + 8);
    } else if ((c->argc == 2 || c->argc == 3) &&
               !strcasecmp(c->argv[1]->ptr,"get"))
    {
        long count = 10, sent = 0;
        listIter li;
        void *totentries;
        listNode *ln;
        slowlogEntry *se;

        if (c->argc == 3 &&
            getLongFromObjectOrReply(c,c->argv[2],&count,NULL) != C_OK)
            return;

        struct orbit_repr *slowlog_r = orbit_scratch_first(&result.scratch);
        assert(slowlog_r->type == ORBIT_ANY);
        list *slowlog = (list*)retval.retval;
        whatisp(retval.retval); whatisp(slowlog_r->any.data + 8);

        listRewind(slowlog,&li);
        totentries = addDeferredMultiBulkLength(c);
        while(count-- && (ln = listNext(&li))) {
            int j;

            se = ln->value;
            addReplyMultiBulkLen(c,6);
            addReplyLongLong(c,se->id);
            addReplyLongLong(c,se->time);
            addReplyLongLong(c,se->duration);
            addReplyMultiBulkLen(c,se->argc);
            for (j = 0; j < se->argc; j++)
                addReplyBulk(c,se->argv[j]);
            addReplyBulkCBuffer(c,se->peerid,sdslen(se->peerid));
            addReplyBulkCBuffer(c,se->cname,sdslen(se->cname));
            sent++;
        }
        setDeferredMultiBulkLength(c,totentries,sent);
    } else {
        addReplyError(c,
            "Unknown SLOWLOG subcommand or wrong # of args. Try GET, RESET, LEN.");
    }

    /* TODO */
    // orbit_scratch_free(slowlog_scratch);
}
