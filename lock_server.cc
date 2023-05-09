// the lock server implementation

#include "lock_server.h"
#include <sstream>
#include <stdio.h>
#include <unistd.h>
#include <arpa/inet.h>
#include <unordered_map>

lock_server::lock_server() : nacquire(0)
{
  pthread_mutex_init(&lock_state_lock, NULL);
}

lock_protocol::status
lock_server::stat(int clt, lock_protocol::lockid_t lid, int &r)
{
  lock_protocol::status ret = lock_protocol::OK;
  printf("stat request from clt %d\n", clt);
  r = nacquire;
  return ret;
}

lock_protocol::status
lock_server::release(int clt, lock_protocol::lockid_t lid, int &r)
{
  pthread_mutex_lock(&lock_state_lock);
  if (lock_state.count(lid) == 0)
    return lock_protocol::RPCERR;
  pthread_mutex_unlock(&lock_state_lock);

  pthread_mutex_lock(&lock_mutex[lid]);
  if (lock_state[lid] == false)
    return lock_protocol::RPCERR;
  lock_state[lid] = false;
  pthread_mutex_unlock(&lock_mutex[lid]);
  pthread_cond_signal(&lock_cv[lid]);
}

lock_protocol::status
lock_server::acquire(int clt, lock_protocol::lockid_t lid, int &r)
{
  pthread_mutex_lock(&lock_state_lock);
  if (lock_state.count(lid) == 0)
  {
    pthread_mutex_t mutex;
    pthread_mutex_init(&mutex, NULL);
    lock_mutex.insert({lid, mutex});

    pthread_cond_t cv;
    pthread_cond_init(&cv, NULL);
    lock_cv.insert({lid, cv});

    lock_state.insert({lid, false});
  }
  pthread_mutex_unlock(&lock_state_lock);

  pthread_mutex_lock(&lock_mutex[lid]);
  while (lock_state[lid] == true)
    pthread_cond_wait(&lock_cv[lid], &lock_mutex[lid]);
  lock_state[lid] = true;
  pthread_mutex_unlock(&lock_mutex[lid]);
}
