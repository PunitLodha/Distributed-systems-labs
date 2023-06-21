// RPC stubs for clients to talk to lock_server, and cache the locks
// see lock_client.cache.h for protocol details.

#include "lock_client_cache.h"
#include "rpc.h"
#include <sstream>
#include <iostream>
#include <stdio.h>

static void *
releasethread(void *x)
{
  lock_client_cache *cc = (lock_client_cache *)x;
  cc->releaser();
  return 0;
}

int lock_client_cache::last_port = 0;

lock_client_cache::lock_client_cache(std::string xdst,
                                     class lock_release_user *_lu)
    : lock_client(xdst), lu(_lu)
{
  srand(time(NULL) ^ last_port);
  rlock_port = ((rand() % 32000) | (0x1 << 10));
  const char *hname;
  // assert(gethostname(hname, 100) == 0);
  hname = "127.0.0.1";
  std::ostringstream host;
  host << hname << ":" << rlock_port;
  id = host.str();
  last_port = rlock_port;
  printf("lock_client_cache: %s\n", id.c_str());

  rpcs *rlsrpc = new rpcs(rlock_port);
  /* register RPC handlers with rlsrpc */
  rlsrpc->reg(rlock_protocol::revoke, this, &lock_client_cache::revoke);
  rlsrpc->reg(rlock_protocol::retry, this, &lock_client_cache::retry);

  // Subscribe to the lock server
  int r;
  int ret = cl->call(lock_protocol::subscribe, cl->id(), id, r);
  assert(ret == lock_protocol::OK);

  // Init cv
  pthread_cond_init(&release_queue_cv, NULL);
  pthread_mutex_init(&release_queue_mutex, NULL);
  pthread_mutex_init(&global_lock, NULL);

  pthread_t th;
  int res = pthread_create(&th, NULL, &releasethread, (void *)this);
  assert(res == 0);
}

void lock_client_cache::releaser()
{

  // This method should be a continuous loop, waiting to be notified of
  // freed locks that have been revoked by the server, so that it can
  // send a release RPC.
  while (true)
  {
    pthread_mutex_lock(&release_queue_mutex);
    while (release_queue.empty())
    {
      pthread_cond_wait(&release_queue_cv, &release_queue_mutex);
    }
    lock_protocol::lockid_t lid = release_queue.front();
    release_queue.pop();
    pthread_mutex_unlock(&release_queue_mutex);
    printf("[clt:%s] releasing the lock to server: %llu\n", id.c_str(), lid);
    // Send release RPC
    int r;
    int ret = cl->call(lock_protocol::release, cl->id(), lid, r);
    assert(ret == lock_protocol::OK);

    lock_entry current_state = get_lock_entry(lid);

    pthread_mutex_lock(&current_state.mutex);
    current_state.state = lock_state::NONE;
    pthread_mutex_unlock(&current_state.mutex);

    // Signal other threads that were waiting becuase lock was in RELEASING OR ACQUIRING
    pthread_cond_broadcast(&current_state.cond);
  }
}

rlock_protocol::status
lock_client_cache::retry(lock_protocol::lockid_t lid, int &r)
{
  printf("[clt:%s] retry request: %llu\n", id.c_str(), lid);
  lock_entry current_state = get_lock_entry(lid);
  pthread_cond_broadcast(&current_state.cond);
  return rlock_protocol::OK;
}

rlock_protocol::status
lock_client_cache::revoke(lock_protocol::lockid_t lid, int &r)
{
  printf("[clt:%s] revoke request: %llu\n", id.c_str(), lid);
  lock_entry current_state = get_lock_entry(lid);

  pthread_mutex_lock(&current_state.mutex);
  if(current_state.state == lock_state::NONE)
  {
    printf("Duplicate revoke, ignoring %llu\n", lid);
    pthread_mutex_unlock(&current_state.mutex);
    return rlock_protocol::OK;
  }
  if (current_state.state == lock_state::FREE) {
    printf("Lock is free, releasing to server: %llu\n", lid);
    pthread_mutex_lock(&release_queue_mutex);
    current_state.state = lock_state::RELEASING;
    release_queue.push(lid);
    pthread_mutex_unlock(&release_queue_mutex);
    pthread_cond_broadcast(&release_queue_cv);
  }
  pthread_mutex_unlock(&current_state.mutex);

  return rlock_protocol::OK;
}

lock_protocol::status
lock_client_cache::acquire(lock_protocol::lockid_t lid)
{
  printf("[clt:%s] acquire request: %llu\n", id.c_str(), lid);
  pthread_mutex_lock(&global_lock);
  if (lock_map.find(lid) == lock_map.end())
  {
    printf("Creating new lock entry for lock: %llu\n", lid);
    lock_entry new_entry = lock_entry();
    new_entry.state = lock_state::NONE;
    new_entry.lid = lid;
    lock_map[lid] = new_entry;
  }
  pthread_mutex_unlock(&global_lock);

  lock_entry current_state = get_lock_entry(lid);

  pthread_mutex_lock(&current_state.mutex);
  while (current_state.state == lock_state::ACQUIRING || current_state.state == lock_state::RELEASING)
  {
    printf("Waiting as lock is in acquiring or releasing: %llu\n", lid);
    pthread_cond_wait(&current_state.cond, &current_state.mutex);
  }

  if (current_state.state == lock_state::NONE)
  {
    current_state.state = lock_state::ACQUIRING;
    pthread_mutex_unlock(&current_state.mutex);

    int r;
    lock_protocol::status ret = lock_protocol::RETRY;

    while (ret == lock_protocol::RETRY)
    {
      printf("Sending acquire request to server: %llu\n", lid);
      ret = cl->call(lock_protocol::acquire, cl->id(), lid, r);
      if (ret == lock_protocol::RETRY)
        pthread_cond_wait(&current_state.cond, &current_state.mutex);
    }

    pthread_mutex_lock(&current_state.mutex);
    current_state.state = lock_state::FREE;
  }

  while (current_state.state != lock_state::FREE)
  {
    printf("Waiting as lock is not free: %llu\n", lid);
    pthread_cond_wait(&current_state.cond, &current_state.mutex);
  }
  current_state.state = lock_state::LOCKED;
  pthread_mutex_unlock(&current_state.mutex);

  return lock_protocol::OK;
}

lock_protocol::status
lock_client_cache::release(lock_protocol::lockid_t lid)
{
  printf("[clt:%s] release request: %llu\n", id.c_str(), lid);
  lock_entry &current_state = get_lock_entry(lid);

  pthread_mutex_lock(&current_state.mutex);
  if (current_state.state == lock_state::RELEASING)
  {
    printf("Lock is in releasing state, adding to release queue: %llu\n", lid);
    pthread_mutex_lock(&release_queue_mutex);
    release_queue.push(lid);
    pthread_mutex_unlock(&release_queue_mutex);
    pthread_cond_broadcast(&release_queue_cv);
  }
  else
  {
    printf("Lock released to client cache: %llu\n", lid);
    current_state.state = lock_state::FREE;
    pthread_cond_broadcast(&current_state.cond);
  }
  pthread_mutex_unlock(&current_state.mutex);


  return lock_protocol::OK;
}

lock_client_cache::lock_entry &lock_client_cache::get_lock_entry(lock_protocol::lockid_t lid)
{
  pthread_mutex_lock(&global_lock);
  lock_entry &current_state = lock_map[lid];
  pthread_mutex_unlock(&global_lock);
  return current_state;
}