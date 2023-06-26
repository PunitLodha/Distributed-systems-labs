// RPC stubs for clients to talk to extent_server

#include "extent_client.h"
#include <sstream>
#include <iostream>
#include <stdio.h>
#include <unistd.h>
#include <time.h>

// The calls assume that the caller holds a lock on the extent

extent_client::extent_client(std::string dst)
{
  sockaddr_in dstsock;
  make_sockaddr(dst.c_str(), &dstsock);
  pthread_mutex_init(&global_lock, NULL);
  cl = new rpcc(dstsock);
  if (cl->bind() != 0)
  {
    printf("extent_client: bind failed\n");
  }
}

extent_protocol::status
extent_client::get(extent_protocol::extentid_t eid, std::string &buf)
{
  pthread_mutex_lock(&global_lock);
  if (extent_mutex.count(eid) == 0)
    pthread_mutex_init(&extent_mutex[eid], NULL);

  if (extent_data.count(eid) == 0)
  {
    pthread_mutex_unlock(&global_lock);
    extent_protocol::status ret = extent_protocol::OK;
    ret = cl->call(extent_protocol::get, eid, buf);
    if (ret != extent_protocol::OK)
      return ret;

    pthread_mutex_lock(&extent_mutex[eid]);
    extent_data[eid] = buf;
    pthread_mutex_unlock(&extent_mutex[eid]);
  }

  pthread_mutex_lock(&global_lock);
  if (extent_attr.count(eid) == 0)
  {
    pthread_mutex_unlock(&global_lock);
    extent_protocol::status ret = extent_protocol::OK;

    extent_protocol::attr attr;
    ret = cl->call(extent_protocol::getattr, eid, attr);
    if (ret != extent_protocol::OK)
      return ret;

    pthread_mutex_lock(&extent_mutex[eid]);
    extent_attr[eid] = extent_client::attr(attr);
    pthread_mutex_unlock(&extent_mutex[eid]);
  }

  pthread_mutex_lock(&extent_mutex[eid]);
  auto curr_time = std::chrono::duration_cast<std::chrono::seconds>(std::chrono::system_clock::now().time_since_epoch()).count();
  extent_attr[eid]._attr.atime = curr_time;
  buf = extent_data[eid];
  pthread_mutex_unlock(&extent_mutex[eid]);

  return extent_protocol::OK;
}

extent_protocol::status
extent_client::getattr(extent_protocol::extentid_t eid,
                       extent_protocol::attr &attr)
{
  pthread_mutex_lock(&global_lock);
  if (extent_mutex.count(eid) == 0)
    pthread_mutex_init(&extent_mutex[eid], NULL);

  if (extent_attr.count(eid) == 0)
  {
    pthread_mutex_unlock(&global_lock);
    extent_protocol::status ret = extent_protocol::OK;

    extent_protocol::attr attr;
    ret = cl->call(extent_protocol::getattr, eid, attr);
    if (ret != extent_protocol::OK)
      return ret;

    pthread_mutex_lock(&extent_mutex[eid]);
    extent_attr[eid] = extent_client::attr(attr);
    pthread_mutex_unlock(&extent_mutex[eid]);
  }

  pthread_mutex_lock(&extent_mutex[eid]);
  attr = extent_attr[eid]._attr;
  pthread_mutex_unlock(&extent_mutex[eid]);

  return extent_protocol::OK;
}

extent_protocol::status
extent_client::put(extent_protocol::extentid_t eid, std::string buf, int content_size)
{

  pthread_mutex_lock(&global_lock);
  if (extent_mutex.count(eid) == 0)
    pthread_mutex_init(&extent_mutex[eid], NULL);
  pthread_mutex_unlock(&global_lock);

  pthread_mutex_lock(&extent_mutex[eid]);
  extent_data[eid] = buf;
  extent_attr[eid] = extent_client::attr(content_size);
  extent_attr[eid]._dirty = true;
  pthread_mutex_unlock(&extent_mutex[eid]);
}

extent_protocol::status
extent_client::remove(extent_protocol::extentid_t eid)
{
  extent_protocol::status ret = extent_protocol::OK;
  extent_protocol::attr attr;
  ret = getattr(eid, attr);
  if (ret != extent_protocol::OK)
    return ret;

  pthread_mutex_lock(&extent_mutex[eid]);
  extent_attr[eid]._dirty = true;
  extent_attr[eid]._remove = true;
  pthread_mutex_unlock(&extent_mutex[eid]);
}

void extent_client::dorelease(lock_protocol::lockid_t eid)
{
  pthread_mutex_lock(&extent_mutex[eid]);
  if (extent_attr[eid]._remove)
  {
    extent_protocol::status ret;
    int r;
    ret = cl->call(extent_protocol::remove, eid, r);
    //* We ignore the ENOENT here as the remove can be called for a file that
    //* exists in the cache but not in the server
    // assert(ret == extent_protocol::OK);
  }
  else if (extent_attr[eid]._dirty)
  {
    extent_protocol::status ret;
    int r;
    ret = cl->call(extent_protocol::put, eid, extent_data[eid],
                   extent_attr[eid]._attr.atime,
                   extent_attr[eid]._attr.ctime,
                   extent_attr[eid]._attr.mtime,
                   extent_attr[eid]._attr.size,
                   r);

    assert(ret == extent_protocol::OK);
  }

  extent_data.erase(eid);
  extent_attr.erase(eid);

  pthread_mutex_unlock(&extent_mutex[eid]);
  extent_mutex.erase(eid);
}