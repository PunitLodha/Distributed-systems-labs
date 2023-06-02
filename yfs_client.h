#ifndef yfs_client_h
#define yfs_client_h

#include <string>
// #include "yfs_protocol.h"
#include "extent_client.h"
#include <unordered_map>

class yfs_client
{
  extent_client *ec;

public:
  typedef unsigned long long inum;
  enum xxstatus
  {
    OK,
    RPCERR,
    NOENT,
    IOERR,
    FBIG
  };
  typedef int status;

  struct fileinfo
  {
    fileinfo(){};
    fileinfo(unsigned long atime, unsigned long mtime, unsigned long ctime, std::string contents);
    fileinfo(mode_t mode, std::string name);
    std::string name;
    unsigned long atime;
    unsigned long mtime;
    unsigned long ctime;
    std::string content;
    mode_t mode;
    unsigned long long size;
  };
  struct dirent
  {
    std::string name;
    unsigned long long inum;
  };
  struct dirinfo
  {
    dirinfo(){};
    dirinfo(unsigned long atime, unsigned long mtime, unsigned long ctime, std::string contents);
    unsigned long atime;
    unsigned long mtime;
    unsigned long ctime;
    std::unordered_map<std::string, unsigned long long> name_to_inum;
  };

private:
  static std::string filename(inum);
  static inum n2i(std::string);

public:
  yfs_client(std::string, std::string);

  bool isfile(inum);
  bool isdir(inum);
  inum ilookup(inum di, std::string name);
  inum gen_rand();

  int getfile(inum, fileinfo &);
  int getdir(inum, dirinfo &);
  int getattr(inum, extent_protocol::attr &);
  int putfile(inum, fileinfo &);
  int putdir(inum, dirinfo &);
  int remove(inum);
};

#endif
