#ifndef lock_server_cache_h
#define lock_server_cache_h

#include <string>
#include <unordered_map>
#include <set>
#include <queue>
#include "lock_protocol.h"
#include "rpc.h"
#include "lock_server.h"

class lock_server_cache
{
public:
  pthread_mutex_t global_lock;
  std::unordered_map<lock_protocol::lockid_t, std::string> lock_owner;

  std::unordered_map<lock_protocol::lockid_t, std::set<lock_protocol::lockid_t>> retry_map;
  std::queue<lock_protocol::lockid_t> retry_queue;
  pthread_cond_t retry_queue_cv;

  std::queue<lock_protocol::lockid_t> revoke_queue;
  pthread_cond_t revoke_queue_cv;

  std::unordered_map<int, rpcc*> clients;

  lock_server_cache();
  lock_protocol::status stat(lock_protocol::lockid_t, int &);
  lock_protocol::status acquire(int clt, lock_protocol::lockid_t lid, int &);
  lock_protocol::status release(int clt, lock_protocol::lockid_t lid, int &);
  lock_protocol::status subscribe(int clt, std::string dst, int &);
  void revoker();
  void retryer();
};

#endif
