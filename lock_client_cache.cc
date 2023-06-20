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
  rlsrpc->reg(rlock_protocol::revoke, this, lock_client_cache::revoke);
  rlsrpc->reg(rlock_protocol::retry, this, lock_client_cache::retry);

  // Subscribe to the lock server
  int r;
  int ret = cl->call(lock_protocol::subscribe, cl->id(), id, r);
  assert(ret == lock_protocol::OK);

  // Init cv
  pthread_cond_init(&release_queue_cv, NULL);

  pthread_t th;
  int r = pthread_create(&th, NULL, &releasethread, (void *)this);
  assert(r == 0);
}

void lock_client_cache::releaser()
{

  // This method should be a continuous loop, waiting to be notified of
  // freed locks that have been revoked by the server, so that it can
  // send a release RPC. 
  while(true) {
    pthread_mutex_lock(&release_queue_mutex);
    while(release_queue.empty()) {
      pthread_cond_wait(&release_queue_cv, &release_queue_mutex);
    }
    lock_protocol::lockid_t lid = release_queue.front();
    release_queue.pop();
    pthread_mutex_unlock(&release_queue_mutex);

    // Send release RPC
    int r;
    int ret = cl->call(lock_protocol::release, cl->id(), lid, r);
    assert(ret == lock_protocol::OK);
  }
}

lock_protocol::status
lock_client_cache::acquire(lock_protocol::lockid_t lid)
{
  return lock_protocol::RPCERR;
}

lock_protocol::status
lock_client_cache::release(lock_protocol::lockid_t lid)
{
  return lock_protocol::RPCERR;
}
