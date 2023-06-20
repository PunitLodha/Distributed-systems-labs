// the caching lock server implementation

#include "lock_server_cache.h"
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

lock_server_cache::lock_server_cache()
{
  pthread_cond_init(&retry_queue_cv, NULL);
  pthread_cond_init(&revoke_queue_cv, NULL);
  pthread_mutex_init(&global_lock, NULL);

  pthread_t th;
  int r = pthread_create(&th, NULL, &revokethread, (void *)this);
  assert(r == 0);

  r = pthread_create(&th, NULL, &retrythread, (void *)this);
  assert(r == 0);
}

lock_protocol::status lock_server_cache::acquire(int clt, lock_protocol::lockid_t lid, int &) {
  pthread_mutex_lock(&global_lock);
  if (lock_owner.find(lid) == lock_owner.end()) {
    lock_owner[lid] = clt;
    pthread_mutex_unlock(&global_lock);
    return lock_protocol::OK;
  }
  // TODO!: If lock free branch...
  else {
    retry_map[lid].insert(clt);
    revoke_queue.push(lid);
    pthread_mutex_unlock(&global_lock);
    pthread_cond_signal(&revoke_queue_cv);
    return lock_protocol::RETRY;
  }
}

lock_protocol::status lock_server_cache::release(int clt, lock_protocol::lockid_t lid, int &){
  pthread_mutex_lock(&global_lock);
  // TODO!: Set lock to free??
  lock_owner.erase(lid);
  // If there are clients waiting for this lock, signal them
  retry_queue.push(lid);
  pthread_cond_signal(&retry_queue_cv);
  pthread_mutex_unlock(&global_lock);
  return lock_protocol::OK;
}

lock_protocol::status lock_server_cache::subscribe(int clt, std::string dst, int &)
{
  sockaddr_in dstsock;
  make_sockaddr(dst.c_str(), &dstsock);
  rpcc * cl = new rpcc(dstsock);
  if (cl->bind() < 0)
  {
    printf("lock_server: call bind\n");
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
    // TODO! Send revoke messages to all? or just front?
    lock_protocol::lockid_t lid = revoke_queue.front();
    revoke_queue.pop();
    pthread_mutex_unlock(&global_lock);

    // TODO!: Send revoke message to lock owner
    int owner = lock_owner[lid];
    rpcc *cl = clients[owner];
    int r;
    cl->call(rlock_protocol::revoke, lid, r);
  }
  
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
    // TODO! Send retry messages to all? or just front?
    lock_protocol::lockid_t lid = retry_queue.front();
    retry_queue.pop();
    std::set<lock_protocol::lockid_t> waiting_clients = retry_map[lid];
    pthread_mutex_unlock(&global_lock);

    // TODO!: Send retry message to all waiting clients for this lock
    for (auto it = waiting_clients.begin(); it != waiting_clients.end(); it++)
    {
      int clt = *it;
      rpcc *cl = clients[clt];
      int r;
      cl->call(rlock_protocol::retry, lid, r);
    }
  }
}
