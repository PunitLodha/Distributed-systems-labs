// RPC stubs for clients to talk to lock_server, and cache the locks
// see lock_client.cache.h for protocol details.

#include "lock_client_cache.h"
#include "rpc.h"
#include "jsl_log.h"
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
  jsl_log(JSL_DBG_3, "lock_client_cache: %s\n", id.c_str());

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
    int sequence_id = get_lock_entry(lid).current_sequence_id;
    // Send release RPC
    int r;
    int ret = cl->call(lock_protocol::release, cl->id(), lid, sequence_id, r);
    assert(ret == lock_protocol::OK);
    jsl_log(JSL_DBG_4, "[clt:%s] releasing the lock to server: %llu\n", id.c_str(), lid);

    lock_entry &current_state = get_lock_entry(lid);

    pthread_mutex_lock(&current_state.mutex);
    current_state.state = lock_state::NONE;

    // Signal other threads that were waiting becuase lock was in RELEASING OR ACQUIRING
    pthread_cond_broadcast(&current_state.cond);
    pthread_mutex_unlock(&current_state.mutex);
  }
}

rlock_protocol::status
lock_client_cache::retry(lock_protocol::lockid_t lid, int sequence_id, int &r)
{
  jsl_log(JSL_DBG_4, "[clt:%s] retry request: %llu\n with sequence_id: %d", id.c_str(), lid, sequence_id);
  lock_entry &current_state = get_lock_entry(lid);
  if (sequence_id == current_state.current_sequence_id)
  {
    current_state.retry_present = true;
  }
  pthread_mutex_lock(&current_state.mutex);
  pthread_cond_broadcast(&current_state.cond);
  pthread_mutex_unlock(&current_state.mutex);
  return rlock_protocol::OK;
}

rlock_protocol::status
lock_client_cache::revoke(lock_protocol::lockid_t lid, int sequence_id, int &r)
{
  jsl_log(JSL_DBG_4, "[clt:%s] revoke request: %llu\n", id.c_str(), lid);
  lock_entry &current_state = get_lock_entry(lid);

  pthread_mutex_lock(&current_state.mutex);
  if (current_state.current_sequence_id > sequence_id)
  {
    jsl_log(JSL_DBG_4, "[clt:%s] Duplicate revoke, ignoring %llu\n", id.c_str(), lid);
    pthread_mutex_unlock(&current_state.mutex);
    return rlock_protocol::OK;
  }

  // TODO: Redundant chacking
  if (current_state.current_sequence_id == sequence_id)
  {
    current_state.revoke_present = true;

    if (current_state.state == lock_state::FREE)
    {
      jsl_log(JSL_DBG_4, "[clt:%s] Lock is free, releasing to server: %llu\n", id.c_str(), lid);
      pthread_mutex_lock(&release_queue_mutex);
      release_queue.push(lid);
      pthread_cond_broadcast(&release_queue_cv);
      pthread_mutex_unlock(&release_queue_mutex);
    }

    if (current_state.state != lock_state::ACQUIRING)
    {
      // TODO: We need to store the sequence number of the revoke
      jsl_log(JSL_DBG_4, "[clt:%s] Lock marked for RELEASING\n", id.c_str());
      current_state.state = lock_state::RELEASING;
    }
    pthread_mutex_unlock(&current_state.mutex);
  }

  return rlock_protocol::OK;
}

lock_protocol::status
lock_client_cache::acquire(lock_protocol::lockid_t lid)
{
  jsl_log(JSL_DBG_4, "[clt:%s] acquire request: %llu\n", id.c_str(), lid);
  pthread_mutex_lock(&global_lock);
  if (lock_map.find(lid) == lock_map.end())
  {
    jsl_log(JSL_DBG_4, "[clt:%s] Creating new lock entry for lock: %llu\n", id.c_str(), lid);
    lock_entry new_entry = lock_entry();
    new_entry.state = lock_state::NONE;
    new_entry.lid = lid;
    lock_map[lid] = new_entry;
  }
  pthread_mutex_unlock(&global_lock);

  lock_entry &current_state = get_lock_entry(lid);

  pthread_mutex_lock(&current_state.mutex);
  while (current_state.state == lock_state::ACQUIRING || current_state.state == lock_state::RELEASING)
  {
    jsl_log(JSL_DBG_4, "[clt:%s] Waiting as lock is in acquiring or releasing: %llu\n", id.c_str(), lid);
    pthread_cond_wait(&current_state.cond, &current_state.mutex);
  }

  if (current_state.state == lock_state::NONE)
  {
    current_state.state = lock_state::ACQUIRING;
    int sequence_id = current_state.current_sequence_id;
    pthread_mutex_unlock(&current_state.mutex);

    int r;
    lock_protocol::status ret = lock_protocol::RETRY;

    while (ret == lock_protocol::RETRY)
    {
      jsl_log(JSL_DBG_4, "[clt:%s] Sending acquire request to server: %llu\n", id.c_str(), lid);

      // TODO: Send the sequence number along with the call
      ret = cl->call(lock_protocol::acquire, cl->id(), lid, sequence_id, r);

      jsl_log(JSL_DBG_4, "[clt:%s] Acquire request response: %llu, %d\n", id.c_str(), lid, ret);
      pthread_mutex_lock(&current_state.mutex);
      if (ret == lock_protocol::RETRY && !current_state.retry_present)
      {
        jsl_log(JSL_DBG_4, "[clt:%s] Waiting to retry acquire request: %llu\n", id.c_str(), lid);
        pthread_cond_wait(&current_state.cond, &current_state.mutex);
      }
      current_state.retry_present = false;
      pthread_mutex_unlock(&current_state.mutex);
    }

    pthread_mutex_lock(&current_state.mutex);
    jsl_log(JSL_DBG_4, "[clt:%s] Lock acquired from server Setting to FREE: %llu\n", id.c_str(), lid);
    current_state.state = lock_state::FREE;
  }

  while (current_state.state != lock_state::FREE)
  {
    jsl_log(JSL_DBG_4, "[clt:%s] Waiting as lock is not free: %llu\n", id.c_str(), lid);
    pthread_cond_wait(&current_state.cond, &current_state.mutex);
  }

  current_state.state = lock_state::LOCKED;

  if (current_state.revoke_present) {
    current_state.state = lock_state::RELEASING;
    current_state.revoke_present = false;
  }

  jsl_log(JSL_DBG_4, "[clt:%s] Lock acquired: %llu, lock_state: %d, curr_state: %d\n", id.c_str(), lid, lock_map[lid].state, current_state.state);
  pthread_mutex_unlock(&current_state.mutex);
  return lock_protocol::OK;
}

lock_protocol::status
lock_client_cache::release(lock_protocol::lockid_t lid)
{
  jsl_log(JSL_DBG_4, "[clt:%s] release request: %llu\n", id.c_str(), lid);
  lock_entry &current_state = get_lock_entry(lid);

  pthread_mutex_lock(&current_state.mutex);
  if (current_state.state == lock_state::RELEASING)
  {
    jsl_log(JSL_DBG_4, "[clt:%s] Lock is in releasing state, adding to release queue: %llu\n", id.c_str(), lid);
    pthread_mutex_lock(&release_queue_mutex);
    release_queue.push(lid);
    current_state.current_sequence_id += 1;
    current_state.retry_present = false;
    current_state.revoke_present = false;
    pthread_cond_broadcast(&release_queue_cv);
    pthread_mutex_unlock(&release_queue_mutex);
  }
  else
  {
    jsl_log(JSL_DBG_4, "[clt:%s] Lock released to client cache: %llu\n", id.c_str(), lid);
    current_state.state = lock_state::FREE;
    pthread_cond_broadcast(&current_state.cond);
  }
  pthread_mutex_unlock(&current_state.mutex);

  jsl_log(JSL_DBG_4, "[clt:%s] Lock is ready for release: %llu\n", id.c_str(), lid);
  return lock_protocol::OK;
}

lock_client_cache::lock_entry &lock_client_cache::get_lock_entry(lock_protocol::lockid_t lid)
{
  pthread_mutex_lock(&global_lock);
  lock_entry &current_state = lock_map[lid];
  pthread_mutex_unlock(&global_lock);
  return current_state;
}