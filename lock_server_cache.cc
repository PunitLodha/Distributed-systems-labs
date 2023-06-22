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
  printf("lock_server_cache created\n");
  pthread_cond_init(&retry_queue_cv, NULL);
  pthread_cond_init(&revoke_queue_cv, NULL);
  pthread_mutex_init(&global_lock, NULL);

  pthread_t th;
  int r = pthread_create(&th, NULL, &revokethread, (void *)this);
  assert(r == 0);

  r = pthread_create(&th, NULL, &retrythread, (void *)this);
  assert(r == 0);
}

lock_protocol::status
lock_server_cache::stat(lock_protocol::lockid_t lid, int &)
{
  lock_protocol::status ret = lock_protocol::OK;
  printf("stat request\n");
  return ret;
}

lock_protocol::status lock_server_cache::acquire(int clt, lock_protocol::lockid_t lid, int &)
{
  printf("[clt:%d] acquire request: %d\n", clt, lid);
  pthread_mutex_lock(&global_lock);
  if (lock_owner.find(lid) == lock_owner.end())
  {
    printf("[clt:%d] Creating new Lock %llu\n", clt,lid);
    lock_owner[lid] = clt;
    pthread_mutex_unlock(&global_lock);
    printf("[clt:%d] Lock %llu created: sending OK to client\n", clt,lid);
    return lock_protocol::OK;
  }
  else
  {
    printf("[clt:%d] Lock %llu already exists: sending revoke to owner\n", clt,lid);
    retry_map[lid].insert(clt);
    revoke_queue.push(lid);
    pthread_mutex_unlock(&global_lock);
    pthread_cond_signal(&revoke_queue_cv);
    return lock_protocol::RETRY;
  }
}

lock_protocol::status lock_server_cache::release(int clt, lock_protocol::lockid_t lid, int &)
{
  printf("[clt:%d] release request: %d\n", clt, lid);
  pthread_mutex_lock(&global_lock);
  lock_owner.erase(lid);
  // If there are clients waiting for this lock, signal them
  retry_queue.push(lid);
  pthread_mutex_unlock(&global_lock);
  pthread_cond_signal(&retry_queue_cv);
  return lock_protocol::OK;
}

lock_protocol::status lock_server_cache::subscribe(int clt, std::string dst, int &)
{
  printf("[clt:%d] subscribe request: %s\n", clt, dst.c_str());
  //TODO: Locking needed?
  sockaddr_in dstsock;
  make_sockaddr(dst.c_str(), &dstsock);
  rpcc *cl = new rpcc(dstsock);
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
    lock_protocol::lockid_t lid = revoke_queue.front();
    revoke_queue.pop();
    pthread_mutex_unlock(&global_lock);

    int owner = lock_owner[lid];
    rpcc *cl = clients[owner];
    int r;
    printf("Sending revoke to lock owner: [%d]%llu\n", owner, lid);
    int ret = cl->call(rlock_protocol::revoke, lid, r);
    printf("Sent revoke to lock owner: [%d]%llu, returned: %d\n", owner, lid, ret);
    assert(ret == lock_protocol::OK);
  }
  printf("revoker thread exiting\n");
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
    retry_queue.pop();
    std::set<int> waiting_clients = retry_map[lid];
    pthread_mutex_unlock(&global_lock);
    printf("Sending retry to waiting clients: %llu\n", lid);

    for (auto it = waiting_clients.begin(); it != waiting_clients.end(); it++)
    {
      int clt = *it;
      rpcc *cl = clients[clt];
      int r;
      cl->call(rlock_protocol::retry, lid, r);
    }
  }
}
