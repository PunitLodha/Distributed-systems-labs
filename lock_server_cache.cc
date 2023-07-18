// the caching lock server implementation

#include "lock_server_cache.h"
#include "jsl_log.h"
#include <sstream>
#include <stdio.h>
#include <unistd.h>
#include <arpa/inet.h>

static void *
revokethread(void *x)
{
    lock_server_cache *sc = (lock_server_cache *)x;
    sc->revoker();
    return 0;
}

static void *
retrythread(void *x)
{
    lock_server_cache *sc = (lock_server_cache *)x;
    sc->retryer();
    return 0;
}

lock_server_cache::lock_server_cache(class rsm *_rsm)
    : rsm(_rsm)
{
    jsl_log(JSL_DBG_3, "lock_server_cache created\n");
    pthread_cond_init(&retry_queue_cv, NULL);
    pthread_cond_init(&revoke_queue_cv, NULL);
    pthread_mutex_init(&global_lock, NULL);

    pthread_t th;
    int r = pthread_create(&th, NULL, &revokethread, (void *)this);
    assert(r == 0);

    r = pthread_create(&th, NULL, &retrythread, (void *)this);
    assert(r == 0);

    rsm->set_state_transfer(this);
}

lock_protocol::status
lock_server_cache::stat(lock_protocol::lockid_t lid, int &)
{
    lock_protocol::status ret = lock_protocol::OK;
    jsl_log(JSL_DBG_3, "stat request\n");
    return ret;
}

lock_protocol::status lock_server_cache::acquire(int clt, lock_protocol::lockid_t lid, int sequence_id, int &)
{
    jsl_log(JSL_DBG_4, "[clt:%d] acquire request: %d with seq#: %d\n", clt, lid, sequence_id);
    pthread_mutex_lock(&global_lock);
    sequence_store[clt][lid] = sequence_id;
    if (lock_owner.find(lid) == lock_owner.end())
    {
        jsl_log(JSL_DBG_4, "[clt:%d] Creating new Lock %llu\n", clt, lid);
        lock_owner[lid] = clt;
        pthread_mutex_unlock(&global_lock);
        jsl_log(JSL_DBG_4, "[clt:%d] Lock %llu created: sending OK to client\n", clt, lid);
        return lock_protocol::OK;
    }
    else
    {
        jsl_log(JSL_DBG_4, "[clt:%d] Lock %llu already exists: sending revoke to owner\n", clt, lid);
        retry_map[lid].insert(clt);
        revoke_queue.push_back(lid);
        pthread_cond_signal(&revoke_queue_cv);
        pthread_mutex_unlock(&global_lock);

        return lock_protocol::RETRY;
    }
}

lock_protocol::status lock_server_cache::release(int clt, lock_protocol::lockid_t lid, int sequence_id, int &)
{
    jsl_log(JSL_DBG_4, "[clt:%d] release request: %d\n", clt, lid);
    pthread_mutex_lock(&global_lock);

    if (sequence_store[clt][lid] != sequence_id)
    {
        jsl_log(JSL_DBG_4, "[clt:%d]Haa kaay prakar aahe?? expected: %d, actual: %d", clt, sequence_store[clt][lid], sequence_id);
    }
    lock_owner.erase(lid);
    // If there are clients waiting for this lock, signal them
    retry_queue.push_back(lid);

    pthread_cond_signal(&retry_queue_cv);
    pthread_mutex_unlock(&global_lock);

    jsl_log(JSL_DBG_4, "[clt:%d] Lock %llu released\n", clt, lid);
    return lock_protocol::OK;
}

lock_protocol::status lock_server_cache::subscribe(int clt, std::string dst, int &)
{
    jsl_log(JSL_DBG_4, "[clt:%d] subscribe request: %s\n", clt, dst.c_str());
    // TODO: Locking needed?
    sockaddr_in dstsock;
    make_sockaddr(dst.c_str(), &dstsock);
    rpcc *cl = new rpcc(dstsock);
    if (cl->bind() < 0)
    {
        jsl_log(JSL_DBG_4, "lock_server: call bind\n");
    }

    clients[clt] = cl;
    return lock_protocol::OK;
}

void lock_server_cache::revoker()
{

    // This method should be a continuous loop, that sends revoke
    // messages to lock holders whenever another client wants the
    // same lock
    while (true)
    {
        pthread_mutex_lock(&global_lock);

        while (revoke_queue.empty())
        {
            pthread_cond_wait(&revoke_queue_cv, &global_lock);
        }

        lock_protocol::lockid_t lid = revoke_queue.front();
        revoke_queue.pop_front();

        if (lock_owner.find(lid) == lock_owner.end())
        {
            jsl_log(JSL_DBG_4, "Lock %llu does not have a owner\n", lid);
            pthread_mutex_unlock(&global_lock);
            continue;
        }

        int owner = lock_owner[lid];
        rpcc *cl = clients[owner];

        int sequence_id = sequence_store[owner][lid];
        pthread_mutex_unlock(&global_lock);

        if (!rsm->amiprimary())
            continue;

        int r;
        jsl_log(JSL_DBG_4, "Sending revoke to lock owner: [%d]%llu\n", owner, lid);
        int ret = cl->call(rlock_protocol::revoke, lid, sequence_id, r);
        jsl_log(JSL_DBG_4, "Sent revoke to lock owner: [%d]%llu, returned: %d\n", owner, lid, ret);
        assert(ret == lock_protocol::OK);
    }
    jsl_log(JSL_DBG_4, "revoker thread exiting\n");
}

void lock_server_cache::retryer()
{

    // This method should be a continuous loop, waiting for locks
    // to be released and then sending retry messages to those who
    // are waiting for it.
    while (true)
    {
        pthread_mutex_lock(&global_lock);

        while (retry_queue.empty())
        {
            pthread_cond_wait(&retry_queue_cv, &global_lock);
        }

        lock_protocol::lockid_t lid = retry_queue.front();
        retry_queue.pop_front();
        std::set<int> waiting_clients = retry_map[lid];
        pthread_mutex_unlock(&global_lock);

        if (!rsm->amiprimary())
            continue;

        jsl_log(JSL_DBG_4, "Sending retry to waiting clients: %llu\n", lid);

        for (auto it = waiting_clients.begin(); it != waiting_clients.end(); it++)
        {
            int clt = *it;
            jsl_log(JSL_DBG_4, "[clt:%d] Preparing to Send retry to client: %d\n", *it, clt);
            pthread_mutex_lock(&global_lock);
            rpcc *cl = clients[clt];
            int sequence_id = sequence_store[clt][lid];
            pthread_mutex_unlock(&global_lock);
            jsl_log(JSL_DBG_4, "[clt:%d] Sending retry to client: %d\n", *it, clt);

            int r;
            int ret = cl->call(rlock_protocol::retry, lid, sequence_id, r);
            jsl_log(JSL_DBG_4, "I dont reach here...");
            assert(ret == rlock_protocol::OK);
            jsl_log(JSL_DBG_4, "[clt:%d] Sent retry to client: %d\n", *it, clt);
        }
    }
}

std::string lock_server_cache::marshal_state()
{
    pthread_mutex_lock(&global_lock);

    marshall rep;

    // marshall lock_owner map
    rep << (unsigned long long)lock_owner.size();
    for (auto pair : lock_owner)
    {
        rep << pair.first;
        rep << pair.second;
    }

    // marshall retry_map
    rep << (unsigned long long)retry_map.size();
    for (auto pair : retry_map)
    {
        rep << pair.first;

        rep << (unsigned long long)pair.second.size();
        for (auto id : pair.second)
            rep << id;
    }

    // marshall retry_queue
    rep << (unsigned long long)retry_queue.size();
    for (auto id : retry_queue)
        rep << id;

    // marshall revoke_queue
    rep << (unsigned long long)revoke_queue.size();
    for (auto id : revoke_queue)
        rep << id;

    // TODO: how do you want to marshall `clients`

    rep << (unsigned long long)sequence_store.size();
    for (auto pair : sequence_store)
    {
        rep << pair.first;

        rep << (unsigned long long)pair.second.size();
        for (auto _pair : pair.second)
        {
            rep << _pair.first;
            rep << _pair.second;
        }
    }

    pthread_mutex_unlock(&global_lock);
}

void lock_server_cache::unmarshal_state(std::string store_rep)
{
    // TODO: Should we clear all the data structures before populating them ?
    // TODO: Verify this marshalling and unmarshalling from PunitLo

    unmarshall store(store_rep);

    pthread_mutex_lock(&global_lock);

    // unmarshall lock_owner map
    unsigned long long size;
    store >> size;

    while (size--)
    {
        lock_protocol::lockid_t lock_id;
        store >> lock_id;

        int owner;
        store >> owner;

        lock_owner[lock_id] = owner;
    }

    // unmarshall retry map
    store >> size;

    while (size--)
    {
        lock_protocol::lockid_t lock_id;
        store >> lock_id;

        unsigned long long client_size;
        store >> client_size;

        std::set<int> clients;
        while (client_size--)
        {
            int client;
            store >> client;

            clients.insert(client);
        }
    }

    // unmarshall retry_queue
    store >> size;

    while (size--)
    {
        lock_protocol::lockid_t lid;
        store >> lid;
        retry_queue.push_back(lid);
    }

    // unmarshall revoke_queue
    store >> size;

    while (size--)
    {
        lock_protocol::lockid_t lid;
        store >> lid;
        revoke_queue.push_back(lid);
    }

    // TODO: how do you want to unmarshall `clients`

    store >> size;
    while (size--)
    {
        int key;
        store >> key;

        unsigned long long _size;
        store >> _size;

        while (_size--)
        {
            lock_protocol::lockid_t lid;
            store >> lid;

            int value;
            store >> value;

            sequence_store[key][lid] = value;
        }
    }

    pthread_mutex_unlock(&global_lock);
}