// the extent server implementation

#include "extent_server.h"
#include <sstream>
#include <stdio.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <chrono>

extent_server::extent_server() {
  extent_map[1] = extent();
}

int extent_server::put(extent_protocol::extentid_t id, std::string buf, int &)
{
  pthread_mutex_lock(&extent_map_lock);
  if (!extent_map.count(id))
  {
    extent_map.insert({id, extent()});
  }

  extent_map[id].data = buf;
  extent_map[id].attr.size = buf.size();
  auto curr_time = std::chrono::duration_cast<std::chrono::seconds>(std::chrono::system_clock::now().time_since_epoch()).count();
  extent_map[id].attr.atime = curr_time;
  extent_map[id].attr.mtime = curr_time;
  extent_map[id].attr.ctime = curr_time;

  pthread_mutex_unlock(&extent_map_lock);
  return extent_protocol::OK;
}

int extent_server::get(extent_protocol::extentid_t id, std::string &buf)
{
  pthread_mutex_lock(&extent_map_lock);
  if (!extent_map.count(id))
  {
    return extent_protocol::NOENT;
  }
  buf = extent_map[id].data;
  auto curr_time = std::chrono::duration_cast<std::chrono::seconds>(std::chrono::system_clock::now().time_since_epoch()).count();
  extent_map[id].attr.atime = curr_time;
  pthread_mutex_unlock(&extent_map_lock);
  return extent_protocol::OK;
}

int extent_server::getattr(extent_protocol::extentid_t id, extent_protocol::attr &a)
{
  // take lock here
  pthread_mutex_lock(&extent_map_lock);
  if (!extent_map.count(id))
  {
    return extent_protocol::NOENT;
  }
  a = extent_map[id].attr;

  //? No need to update time here, as mentioned in the project description:-
  //? Tracking this data in the extent server should be straightforward in the handlers for the put(key,value) and get(key) RPCs. 

  // auto curr_time = std::chrono::duration_cast<std::chrono::seconds>(std::chrono::system_clock::now().time_since_epoch()).count();
  // extent_map[id].attr.ctime = curr_time;
  pthread_mutex_unlock(&extent_map_lock);
  return extent_protocol::OK;

  // You replace this with a real implementation. We send a phony response
  // for now because it's difficult to get FUSE to do anything (including
  // unmount) if getattr fails.
  // a.size = 0;
  // a.atime = 0;
  // a.mtime = 0;
  // a.ctime = 0;
  // return extent_protocol::OK;
}

int extent_server::remove(extent_protocol::extentid_t id, int &)
{
  pthread_mutex_lock(&extent_map_lock);
  if (!extent_map.count(id))
  {
    return extent_protocol::NOENT;
  }
  extent_map.erase(id);
  pthread_mutex_unlock(&extent_map_lock);
  return extent_protocol::OK;
}
