// yfs client.  implements FS operations using extent and lock server
#include "yfs_client.h"
#include "extent_client.h"
#include <sstream>
#include <iostream>
#include <stdio.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <chrono>

yfs_client::yfs_client(std::string extent_dst, std::string lock_dst)
{
  ec = new extent_client(extent_dst);
  unsigned seed = std::chrono::system_clock::now().time_since_epoch().count();
  srand(seed);
}

yfs_client::fileinfo::fileinfo(mode_t mode, std::string name)
{
  this->mode = mode;
  this->name = name;
  // this->content = "";
  printf("[Create] fileinfo: %s, %d\n", content, content.size());
}

yfs_client::fileinfo::fileinfo(unsigned long atime, unsigned long mtime, unsigned long ctime, std::string contents)
{
  this->atime = atime;
  this->mtime = mtime;
  this->ctime = ctime;

  std::istringstream iss(contents);
  std::string line;

  printf("Trying Deserialize file info\n");
  std::getline(iss, line);
  this->name = line;
  printf("\t\t Deserialized name\n");

  std::getline(iss, line);
  printf("\t\t Trying Deserialized mode ... %s\n", line.c_str());
  printf("\t\t Deserialized mode\n");

  while (std::getline(iss, line))
  {
    this->content.append(line);
    std::cout <<"Deserialize file info:- " << line << std::endl;
    // this->content.append("\n");
  }

  this->size = this->content.size();
}

std::ostream &operator<<(std::ostream &os, const yfs_client::fileinfo &fileinfo)
{
  os << fileinfo.name << "\n"
     << fileinfo.mode << "\n"
     << fileinfo.content;
  return os;
}

yfs_client::dirinfo::dirinfo(unsigned long atime, unsigned long mtime, unsigned long ctime, std::string contents)
{
  this->atime = atime;
  this->mtime = mtime;
  this->ctime = ctime;

  // parse contents
  std::istringstream iss(contents);
  std::string line;
  while (std::getline(iss, line))
  {
    std::istringstream iss2(line);
    dirent entry;

    iss2 >> entry.inum;
    iss2 >> entry.name;

    name_to_inum.insert({entry.name, entry.inum});
  }
}

std::ostream &operator<<(std::ostream &os, const yfs_client::dirinfo &dirinfo)

{
  for (auto entry : dirinfo.name_to_inum)
  {
    // {name: inum}, inum is second and name is first
    os << entry.second << " " << entry.first << "\n";
  }
  return os;
}

yfs_client::inum
yfs_client::n2i(std::string n)
{
  std::istringstream ist(n);
  unsigned long long finum;
  ist >> finum;
  return finum;
}

std::string
yfs_client::filename(inum inum)
{
  std::ostringstream ost;
  ost << inum;
  return ost.str();
}

bool yfs_client::isfile(inum inum)
{
  if (inum & 0x80000000)
    return true;
  return false;
}

bool yfs_client::isdir(inum inum)
{
  return !isfile(inum);
}

int yfs_client::getfile(inum inum, fileinfo &fin)
{
  int r = OK;

  printf("getfile %016llx\n", inum);
  extent_protocol::attr a;
  yfs_client::fileinfo temp_fin = fileinfo();
  std::string buf;
  printf("\t\tgetfileattr %016llx\n", inum);
  if (ec->getattr(inum, a) != extent_protocol::OK)
  {
    r = IOERR;
    goto release;
  }

  printf("\t\tgetfile from extent server %016llx\n", inum);
  if (ec->get(inum, buf) != extent_protocol::OK)
  {
    r = IOERR;
    goto release;
  }
  temp_fin = fileinfo(a.atime, a.mtime, a.ctime, buf);
  printf("\t\tcreated fileinfo\n");

  fin.content = temp_fin.content;
  fin.atime = temp_fin.atime;
  fin.mtime = temp_fin.mtime;
  fin.ctime = temp_fin.ctime;
  fin.size = temp_fin.size;
  printf("getfile %016llx -> sz %llu\n", inum, fin.size);

release:

  return r;
}

int yfs_client::getdir(inum inum, dirinfo &din)
{
  int r = OK;

  printf("getdir %016llx\n", inum);
  extent_protocol::attr a;
  yfs_client::dirinfo temp_din = dirinfo();
  std::string buf;
  if (ec->getattr(inum, a) != extent_protocol::OK)
  {
    r = IOERR;
    goto release;
  }
  if (ec->get(inum, buf) != extent_protocol::OK)
  {
    r = IOERR;
    goto release;
  }
  temp_din = dirinfo(a.atime, a.mtime, a.ctime, buf);

  din.atime = temp_din.atime;
  din.ctime = temp_din.ctime;
  din.mtime = temp_din.mtime;
  din.name_to_inum = temp_din.name_to_inum;

release:
  return r;
}

int yfs_client::getattr(inum inum, extent_protocol::attr &attr)
{
  int r = OK;

  printf("getfileattr %016llx\n", inum);
  extent_protocol::attr a;
  if (ec->getattr(inum, a) != extent_protocol::OK)
  {
    r = IOERR;
    goto release;
  }

  attr.atime = a.atime;
  attr.mtime = a.mtime;
  attr.ctime = a.ctime;
  attr.size = a.size;
  printf("getattr %016llx -> sz %u\n", inum, attr.size);

release:

  return r;
}

yfs_client::inum
yfs_client::gen_rand()
{
  long rand_number = static_cast<long>(std::rand()) << 16 | std::rand();
  return rand_number;
}

int yfs_client::putdir(inum parent, dirinfo &dir)
{
  int r = OK;
  std::string buf;
  std::ostringstream ost;
  ost << dir;
  buf = ost.str();
  if (ec->put(parent, buf, buf.size()) != extent_protocol::OK)
  {
    r = IOERR;
    goto release;
  }
release:
  return r;
}

int yfs_client::putfile(inum file_inum, fileinfo &file)
{
  int r = OK;
  std::string buf;
  std::ostringstream ost;
  ost << file;
  buf = ost.str();
  printf("putfile %016llx -> sz %llu\n", file_inum, file.size);
  if (ec->put(file_inum, buf, file.size) != extent_protocol::OK)
  {
    r = IOERR;
    goto release;
  }
release:
  return r;
}

int yfs_client::remove(inum file_inum)
{
  int r = OK;
  if (ec->remove(file_inum) != extent_protocol::OK)
  {
    r = IOERR;
    goto release;
  }
release:
  return r;
}