// this is the lock server
// the lock client has a similar interface

#ifndef lock_server_h
#define lock_server_h

#include <string>
#include <unordered_map>
#include "lock_protocol.h"
#include "lock_client.h"
#include "rpc.h"

class lock_server
{

protected:
  int nacquire;
  pthread_mutex_t lock_state_lock;
  std::unordered_map<lock_protocol::lockid_t, pthread_mutex_t> lock_mutex;
  std::unordered_map<lock_protocol::lockid_t, pthread_cond_t> lock_cv;
  std::unordered_map<lock_protocol::lockid_t, bool> lock_state;

public:
  lock_server();
  ~lock_server(){};
  lock_protocol::status stat(int clt, lock_protocol::lockid_t lid, int &);
  lock_protocol::status acquire(int clt, lock_protocol::lockid_t lid, int &);
  lock_protocol::status release(int clt, lock_protocol::lockid_t lid, int &);
};

#endif
