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

int extent_server::put(extent_protocol::extentid_t id, std::string buf, int content_size, int &)
{
  printf("[extent_server]Putting data for extentid: %llu\n", id);
  pthread_mutex_lock(&extent_map_lock);
  if (!extent_map.count(id))
  {
    extent_map.insert({id, extent()});
  }
  pthread_mutex_unlock(&extent_map_lock);

  pthread_mutex_lock(&extent_map[id].mutex);
  extent_map[id].data = buf;
  extent_map[id].attr.size = content_size;
  auto curr_time = std::chrono::duration_cast<std::chrono::seconds>(std::chrono::system_clock::now().time_since_epoch()).count();
  extent_map[id].attr.atime = curr_time;
  extent_map[id].attr.mtime = curr_time;
  extent_map[id].attr.ctime = curr_time;
  pthread_mutex_unlock(&extent_map[id].mutex);

  return extent_protocol::OK;
}

int extent_server::get(extent_protocol::extentid_t id, std::string &buf)
{
  printf("[extent_server]Getting data for extentid: %llu\n", id);
  pthread_mutex_lock(&extent_map_lock);
  if (!extent_map.count(id))
  {
    return extent_protocol::NOENT;
  }
  pthread_mutex_unlock(&extent_map_lock);

  pthread_mutex_lock(&extent_map[id].mutex);
  buf = extent_map[id].data;
  auto curr_time = std::chrono::duration_cast<std::chrono::seconds>(std::chrono::system_clock::now().time_since_epoch()).count();
  extent_map[id].attr.atime = curr_time;
  pthread_mutex_unlock(&extent_map[id].mutex);

  return extent_protocol::OK;
}

int extent_server::getattr(extent_protocol::extentid_t id, extent_protocol::attr &a)
{
  printf("[extent_server]Getting attr for extentid: %llu\n", id);
  // take lock here
  pthread_mutex_lock(&extent_map_lock);
  if (!extent_map.count(id))
  {
    return extent_protocol::NOENT;
  }
  pthread_mutex_unlock(&extent_map_lock);

  pthread_mutex_lock(&extent_map[id].mutex);
  a = extent_map[id].attr;
  pthread_mutex_unlock(&extent_map[id].mutex);

  return extent_protocol::OK;
}

int extent_server::remove(extent_protocol::extentid_t id, int &)
{
  printf("[extent_server]Removing extentid: %llu\n", id);
  pthread_mutex_lock(&extent_map_lock);
  if (!extent_map.count(id))
  {
    return extent_protocol::NOENT;
  }
  extent_map.erase(id);
  pthread_mutex_unlock(&extent_map_lock);
  return extent_protocol::OK;
}
