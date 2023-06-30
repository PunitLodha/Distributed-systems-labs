// extent client interface.

#ifndef extent_client_h
#define extent_client_h

#include <string>
#include <unordered_map>
#include <chrono>
#include "extent_protocol.h"
#include "lock_client_cache.h"
#include "rpc.h"

class extent_client : public lock_release_user
{
private:
  rpcc *cl;
  pthread_mutex_t global_lock;

  struct attr
  {
    extent_protocol::attr _attr;
    bool _dirty;
    bool _remove;

    attr(extent_protocol::attr attr)
    {
      _attr = attr;
      _dirty = false;
      _remove = false;
    }

    attr(int content_size)
    {
      auto curr_time = std::chrono::duration_cast<std::chrono::seconds>(std::chrono::system_clock::now().time_since_epoch()).count();
      _attr.atime = curr_time;
      _attr.mtime = curr_time;
      _attr.ctime = curr_time;
      _attr.size = content_size;
       _dirty = false;
      _remove = false;
    }
    attr() {
      _dirty = false;
      _remove = false;
    }
  };

  std::unordered_map<extent_protocol::extentid_t, std::string> extent_data;
  std::unordered_map<extent_protocol::extentid_t, struct attr> extent_attr;
  std::unordered_map<extent_protocol::extentid_t, pthread_mutex_t> extent_mutex;

public:
  extent_client(std::string dst);

  extent_protocol::status get(extent_protocol::extentid_t eid,
                              std::string &buf);
  extent_protocol::status getattr(extent_protocol::extentid_t eid,
                                  extent_protocol::attr &a);
  extent_protocol::status put(extent_protocol::extentid_t eid, std::string buf, int content_size);
  extent_protocol::status remove(extent_protocol::extentid_t eid);
  void dorelease(lock_protocol::lockid_t);
};

#endif
