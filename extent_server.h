// this is the extent server

#ifndef extent_server_h
#define extent_server_h

#include <string>
#include <map>
#include "extent_protocol.h"

class extent_server
{

private:
  struct extent
  {
    std::string data;
    extent_protocol::attr attr;
    pthread_mutex_t mutex;

    extent()
    {
      pthread_mutex_init(&mutex, NULL);
    }
  };

  std::map<extent_protocol::extentid_t, extent> extent_map;
  pthread_mutex_t extent_map_lock;

public:
  extent_server();

  int put(extent_protocol::extentid_t id, std::string, int, int, int, int, int &);
  int get(extent_protocol::extentid_t id, std::string &);
  int getattr(extent_protocol::extentid_t id, extent_protocol::attr &);
  int remove(extent_protocol::extentid_t id, int &);
};

#endif
