// extent client interface.

#ifndef extent_client_h
#define extent_client_h

#include <string>
#include "extent_protocol.h"
#include "lock_client_cache.h"
#include "rpc.h"

class extent_client: lock_release_user  {
 private:
  rpcc *cl;

 public:
  extent_client(std::string dst);

  extent_protocol::status get(extent_protocol::extentid_t eid, 
			      std::string &buf);
  extent_protocol::status getattr(extent_protocol::extentid_t eid, 
				  extent_protocol::attr &a);
  extent_protocol::status put(extent_protocol::extentid_t eid, std::string buf, int content_size);
  extent_protocol::status remove(extent_protocol::extentid_t eid);
  void dorelease(lock_protocol::lockid_t) {};
};

#endif 

