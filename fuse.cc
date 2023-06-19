/*
 * receive request from fuse and call methods of yfs_client
 *
 * started life as low-level example in the fuse distribution.  we
 * have to use low-level interface in order to get i-numbers.  the
 * high-level interface only gives us complete paths.
 */

#include <fuse_lowlevel.h>
#include <stdio.h>
#include <stdlib.h>
#include <strings.h>
#include <string.h>
#include <errno.h>
#include <fcntl.h>
#include <unistd.h>
#include <assert.h>
#include <arpa/inet.h>
#include "yfs_client.h"

#define min(x,y) ((x) < (y) ? (x) : (y))

int myid;
yfs_client *yfs;

int id()
{
  return myid;
}

yfs_client::status
getattr(yfs_client::inum inum, struct stat &st)
{
  yfs_client::status ret;

  bzero(&st, sizeof(st));

  st.st_ino = inum;
  printf("getattr %016llx %d\n", inum, yfs->isfile(inum));
  extent_protocol::attr attr;
  ret = yfs->getattr(inum, attr);
  if (ret != yfs_client::OK)
    return ret;
  st.st_atime = attr.atime;
  st.st_mtime = attr.mtime;
  st.st_ctime = attr.ctime;

  if (yfs->isfile(inum))
  {
    st.st_mode = S_IFREG | 0666;
    st.st_nlink = 1;
    st.st_size = attr.size;
    printf("   getattr -> %u\n", attr.size);
  }
  else
  {
    st.st_mode = S_IFDIR | 0777;
    st.st_nlink = 2;
    printf("   getattr -> %u %u %u\n", attr.atime, attr.mtime, attr.ctime);
  }
  return yfs_client::OK;
}

void fuseserver_getattr(fuse_req_t req, fuse_ino_t ino,
                        struct fuse_file_info *fi)
{
  struct stat st;
  yfs_client::inum inum = ino; // req->in.h.nodeid;
  yfs_client::status ret;

  yfs->lc->acquire(inum);
  ret = getattr(inum, st);
  yfs->lc->release(inum);
  if (ret != yfs_client::OK)
  {
    fuse_reply_err(req, ENOENT);
    return;
  }
  fuse_reply_attr(req, &st, 0);
}

void fuseserver_setattr(fuse_req_t req, fuse_ino_t ino, struct stat *attr, int to_set, struct fuse_file_info *fi)
{
  printf("fuseserver_setattr 0x%x\n", to_set);

  // Get the current attributes
  yfs_client::fileinfo current_fileinfo;
  yfs_client::inum inum = ino; // req->in.h.nodeid;
  yfs_client::status ret;

  yfs->lc->acquire(inum);

  ret = yfs->getfile(inum, current_fileinfo);
  if (ret != yfs_client::OK)
  {
    fuse_reply_err(req, ENOENT);
    goto release;
  }

  if (FUSE_SET_ATTR_SIZE & to_set)
  {
    printf("\tfuseserver_setattr set size to %zu\n", attr->st_size);
    current_fileinfo.content.resize(attr->st_size);
    current_fileinfo.size = attr->st_size;

    ret = yfs->putfile(inum, current_fileinfo);
    if (ret != yfs_client::OK)
    {
      fuse_reply_err(req, ENOENT);
      goto release;
    }

    // TODO: might be wrong because putting the file will update time values
    struct stat st;
    st.st_atime = current_fileinfo.atime;
    st.st_mtime = current_fileinfo.mtime;
    st.st_ctime = current_fileinfo.ctime;
    st.st_mode = S_IFREG | 0666;
    st.st_nlink = 1;
    st.st_size = current_fileinfo.size;
    fuse_reply_attr(req, &st, 0);
  }
  else
  {
    // If none of the attribute is to be set then raise an error
    fuse_reply_err(req, ENOSYS);
  }

  release:
    yfs->lc->release(inum);
    return;
}

void fuseserver_read(fuse_req_t req, fuse_ino_t ino, size_t size,
                     off_t off, struct fuse_file_info *fi)
{
  // You fill this in

  // Get the current attributes
  yfs_client::fileinfo current_fileinfo;
  yfs_client::inum inum = ino;
  yfs_client::status ret;
  char *buf = new char[size];
  printf("fuseserver_read %016llx offset: %d, size: %d\n", inum, off, size);

  yfs->lc->acquire(inum);
  ret = yfs->getfile(inum, current_fileinfo);
  if (ret != yfs_client::OK)
  {
    fuse_reply_err(req, ENOENT);
    goto release;
  }

  printf("\t\tcontents read:- ");
  std::cout<<std::hex<<current_fileinfo.content<<std::endl;
  // Read the file contents at the offset in a char buffer
  memset(buf, '\0', size);
  if (off < current_fileinfo.size) {
    memcpy(buf, current_fileinfo.content.c_str() + off, min(current_fileinfo.size - off, size));
  }
  printf("\t\tread contents in buffer:- ");
  for (int i = 0; i < size; i++) {
    printf("%c", buf[i]);
  }
  printf("\n");
  // Send the buffer to the fuse client
  fuse_reply_buf(req, buf, size);
  delete[] buf;

  release:
    yfs->lc->release(inum);
    return;
}

void fuseserver_write(fuse_req_t req, fuse_ino_t ino,
                      const char *buf, size_t size, off_t off,
                      struct fuse_file_info *fi)
{
  // You fill this in
  // Get the current attributes
  yfs_client::fileinfo current_fileinfo;
  yfs_client::inum inum = ino;
  yfs_client::status ret;
  std::string replacement_str(buf, size);
  printf("fuseserver_write %016llx, size: %d, offset: %d\n", inum, size, off);
  printf("\t\twrite input:");
  std::cout<<" hehe "<<std::hex<<replacement_str<<std::endl;

  yfs->lc->acquire(inum);
  ret = yfs->getfile(inum, current_fileinfo);
  if (ret != yfs_client::OK)
  {
    fuse_reply_err(req, ENOENT);
    goto release;
  }

  printf("\t\tbefore write size:%d\n", current_fileinfo.size );
  if ((off + size) >= current_fileinfo.content.size()) {
    printf("\t\t RESIZE: write offset + size(%d) >= current_fileinfo.content.size():(%d)\n",off+size, current_fileinfo.content.size());
    current_fileinfo.content.resize(off + size);
    printf("\t\t new size: %d\n", current_fileinfo.content.size());
  }
  printf("\t\t new size1: %d\n", current_fileinfo.content.size());
  // current_fileinfo.content.insert(off, replacement_str);
  current_fileinfo.content.replace(off, size, replacement_str);
  printf("\t\t new size2: %d\n", current_fileinfo.content.size());
  current_fileinfo.size = current_fileinfo.content.size();
  printf("\t\tafter write contenrts: %s, size:%d\n", current_fileinfo.content.c_str(), current_fileinfo.size );
  // Update the file contents
  ret = yfs->putfile(inum, current_fileinfo);
  if (ret != yfs_client::OK)
  {
    fuse_reply_err(req, ENOENT);
    goto release;
  }
  // Send the number of bytes written to the fuse client
  fuse_reply_write(req, size);

  release:
    yfs->lc->release(inum);
    return;
}

yfs_client::status
fuseserver_createhelper(fuse_ino_t parent, const char *name,
                        mode_t mode, struct fuse_entry_param *e)
{
  // You fill this in
  yfs_client::inum parent_inum = parent;
  yfs_client::inum file_inum = yfs->gen_rand() | 0x80000000;
  yfs_client::status ret;
  yfs_client::dirinfo info;
  struct stat st;
  yfs_client::fileinfo fileinfo;
  yfs_client::status r = yfs_client::OK;

  yfs->lc->acquire(parent_inum);
  yfs->lc->acquire(file_inum);

  // Get parent directory
  printf("create %016lx %s\n", parent, name);
  ret = yfs->getdir(parent_inum, info);
  if (ret != yfs_client::OK){
    r = ret;
    goto release;
  }

  printf("\t\t found parent dir\n");
  // TODO: check if parent is a directory
  // TODO: check if name already exists in parent

  // Add new file to the directory
  info.name_to_inum[name] = file_inum;

  // Sent extent server the new directory contents
  ret = yfs->putdir(parent_inum, info);
  if (ret != yfs_client::OK){
    r = ret;
    goto release;
  }
  printf("\t\t updated new dir\n");

  // Create the new file
  fileinfo = yfs_client::fileinfo(mode, name);
  ret = yfs->putfile(file_inum, fileinfo);
  printf("\t\t created new file\n");
  if (ret != yfs_client::OK){
    r = ret;
    goto release;
  }

  ret = getattr(file_inum, st);
  if (ret != yfs_client::OK){
    r = ret;
    goto release;
  }

  e->ino = file_inum;
  e->attr_timeout = 0.0;
  e->entry_timeout = 0.0;
  e->attr = st;

  release:
    yfs->lc->release(parent_inum);
    yfs->lc->release(file_inum);
    return r;
}

void fuseserver_create(fuse_req_t req, fuse_ino_t parent, const char *name,
                       mode_t mode, struct fuse_file_info *fi)
{
  struct fuse_entry_param e;
  if (fuseserver_createhelper(parent, name, mode, &e) == yfs_client::OK)
  {
    fuse_reply_create(req, &e, fi);
  }
  else
  {
    fuse_reply_err(req, ENOENT);
  }
}

void fuseserver_mknod(fuse_req_t req, fuse_ino_t parent,
                      const char *name, mode_t mode, dev_t rdev)
{
  struct fuse_entry_param e;
  if (fuseserver_createhelper(parent, name, mode, &e) == yfs_client::OK)
  {
    fuse_reply_entry(req, &e);
  }
  else
  {
    fuse_reply_err(req, ENOENT);
  }
}

void fuseserver_lookup(fuse_req_t req, fuse_ino_t parent, const char *name)
{
  struct fuse_entry_param e;
  bool found = false;

  e.attr_timeout = 0.0;
  e.entry_timeout = 0.0;

  // You fill this in:
  // Look up the file named `name' in the directory referred to by
  // `parent' in YFS. If the file was found, initialize e.ino and
  // e.attr appropriately.

  printf("lookup %016lx %s\n", parent, name);
  yfs_client::status ret;
  yfs_client::dirinfo info;

  yfs->lc->acquire(parent);

  ret = yfs->getdir(parent, info);
  if (ret != yfs_client::OK)
  {
    fuse_reply_err(req, ENOENT);
    goto release;
  }

  printf("\t\tlookup, directory found\n");
  found = info.name_to_inum.count(name);
  printf("\t\tlookup %016lx %s -> %d\n", parent, name, found);
  if (found)
  {
    e.ino = info.name_to_inum[name];
    printf("\t\tlookup %016lx %s -> %016lx\n", parent, name, e.ino);
    struct stat st;
    yfs->lc->acquire(e.ino);
    ret = getattr(info.name_to_inum[name], st);
    yfs->lc->release(e.ino);
    if (ret != yfs_client::OK)
    {
      fuse_reply_err(req, ENOENT);
      goto release;
    }
    e.attr = st;
    fuse_reply_entry(req, &e);
  }
  else
    fuse_reply_err(req, ENOENT);
  
  release:
    yfs->lc->release(parent);
    return;
}

struct dirbuf
{
  char *p;
  size_t size;
};

void dirbuf_add(struct dirbuf *b, const char *name, fuse_ino_t ino)
{
  struct stat stbuf;
  size_t oldsize = b->size;
  b->size += fuse_dirent_size(strlen(name));
  b->p = (char *)realloc(b->p, b->size);
  memset(&stbuf, 0, sizeof(stbuf));
  stbuf.st_ino = ino;
  fuse_add_dirent(b->p + oldsize, name, &stbuf, b->size);
}

#define min(x, y) ((x) < (y) ? (x) : (y))

int reply_buf_limited(fuse_req_t req, const char *buf, size_t bufsize,
                      off_t off, size_t maxsize)
{
  if ((size_t)off < bufsize)
    return fuse_reply_buf(req, buf + off, min(bufsize - off, maxsize));
  else
    return fuse_reply_buf(req, NULL, 0);
}

void fuseserver_readdir(fuse_req_t req, fuse_ino_t ino, size_t size,
                        off_t off, struct fuse_file_info *fi)
{
  yfs_client::inum inum = ino; // req->in.h.nodeid;
  struct dirbuf b;
  memset(&b, 0, sizeof(b));
  yfs_client::dirent e;


  printf("fuseserver_readdir\n");

  if (!yfs->isdir(inum))
  {
    fuse_reply_err(req, ENOTDIR);
    return;
  }

  yfs->lc->acquire(inum);

  // fill in the b data structure using dirbuf_add
  yfs_client::dirinfo info;
  yfs_client::status ret;

  ret = yfs->getdir(inum, info);
  if (ret != yfs_client::OK)
  {
    fuse_reply_err(req, ENOENT);
    goto release;
  }

  for (auto dirent : info.name_to_inum)
  {
    dirbuf_add(&b, dirent.first.c_str(), dirent.second);
  }

  reply_buf_limited(req, b.p, b.size, off, size);
  free(b.p);
  release:
    yfs->lc->release(inum);
    return;
}

void fuseserver_open(fuse_req_t req, fuse_ino_t ino,
                     struct fuse_file_info *fi)
{
  // You fill this in
  printf("open %016lx\n", ino);
  printf("\t\topen flags %d\n", fi->flags);
  fuse_reply_open(req, fi);
}

void fuseserver_mkdir(fuse_req_t req, fuse_ino_t parent, const char *name,
                      mode_t mode)
{

  // You fill this in
  printf("\t\tmkdir parent:%d, name: %s\n", parent, name);

  yfs_client::inum parent_inum = parent;
  yfs_client::inum dir_inum = yfs->gen_rand() & ~0x80000000;
  yfs_client::status ret;
  yfs_client::dirinfo info;
  yfs_client::dirinfo new_info;
  struct fuse_entry_param e;

  yfs->lc->acquire(parent_inum);
  yfs->lc->acquire(dir_inum);


  // Put the new directory in the parent directory

  // 1 - Get parent directory
  ret = yfs->getdir(parent_inum, info);
  if (ret != yfs_client::OK)
  {
    fuse_reply_err(req, ENOSYS);
    goto release;
  }
  
  // 2 - Add new file to the directory
  info.name_to_inum[name] = dir_inum;

  // 3 - Sent extent server the new parent directory contents
  ret = yfs->putdir(parent_inum, info);
  if (ret != yfs_client::OK)
  {
    fuse_reply_err(req, ENOSYS);
    goto release;
  }

  // Create the new directory
  ret = yfs->putdir(dir_inum, new_info);
  if (ret != yfs_client::OK)
  {
    fuse_reply_err(req, ENOSYS);
    goto release;
  }

  struct stat st;
  ret = getattr(dir_inum, st);
  if (ret != yfs_client::OK)
  {
    fuse_reply_err(req, ENOENT);
    goto release;
  }
  e.ino = dir_inum;
  e.attr = st;
  e.attr_timeout = 0.0;
  e.entry_timeout = 0.0;
  printf("\t\tmkdir ino: %d, isdir?: %d\n", e.ino, yfs->isdir(e.ino));
  printf("\t\tmkdir st_mode: %d\n", e.attr.st_mode);
  printf("\t\tmkdir st_nlink: %d\n", e.attr.st_nlink);
  printf("\t\tmkdir st_atime: %d\n", e.attr.st_atime);
  printf("\t\tmkdir st_ctime: %d\n", e.attr.st_ctime);
  printf("\t\tmkdir st_mtime: %d\n", e.attr.st_mtime);
  fuse_reply_entry(req, &e);
  release:
    yfs->lc->release(parent_inum);
    yfs->lc->release(dir_inum);
    return;
}

void fuseserver_unlink(fuse_req_t req, fuse_ino_t parent, const char *name)
{

  // You fill this in
  printf("\t\tunlink parent:%d, name: %s\n", parent, name);

  yfs_client::inum parent_inum = parent;
  yfs_client::inum file_inum;
  yfs_client::status ret;
  yfs_client::dirinfo info;

  yfs->lc->acquire(parent_inum);

  // 1 - Get parent directory
  ret = yfs->getdir(parent_inum, info);
  if (ret != yfs_client::OK)
  {
    fuse_reply_err(req, ENOSYS);
    goto release;
  }

  // Check if name exists in parent
  if (info.name_to_inum.count(name) == 0)
  {
    fuse_reply_err(req, ENOENT);
    goto release;
  } else {
    // Remove the file from the parent directory
    file_inum = info.name_to_inum[name];
    info.name_to_inum.erase(name);
  }

  // 2 - Sent extent server the new parent directory contents
  ret = yfs->putdir(parent_inum, info);
  if (ret != yfs_client::OK)
  {
    fuse_reply_err(req, ENOSYS);
    goto release;
  }

  // 3 - Delete the file
  yfs->lc->acquire(file_inum);
  ret = yfs->remove(file_inum);
  yfs->lc->release(file_inum);
  if (ret != yfs_client::OK)
  {
    fuse_reply_err(req, ENOSYS);
    goto release;
  }

  fuse_reply_err(req, 0);
  // Success:	fuse_reply_err(req, 0);
  // Not found:	fuse_reply_err(req, ENOENT);
  // fuse_reply_err(req, ENOSYS);

  release:
    yfs->lc->release(parent_inum);
    return;
}

void fuseserver_statfs(fuse_req_t req)
{
  struct statvfs buf;

  printf("statfs\n");

  memset(&buf, 0, sizeof(buf));

  buf.f_namemax = 255;
  buf.f_bsize = 512;

  fuse_reply_statfs(req, &buf);
}

struct fuse_lowlevel_ops fuseserver_oper;

int main(int argc, char *argv[])
{
  char *mountpoint = 0;
  int err = -1;
  int fd;

  setvbuf(stdout, NULL, _IONBF, 0);

  if (argc != 4)
  {
    fprintf(stderr, "Usage: yfs_client <mountpoint> <port-extent-server> <port-lock-server>\n");
    exit(1);
  }
  mountpoint = argv[1];

  srandom(getpid());

  myid = random();

  yfs = new yfs_client(argv[2], argv[3]);

  fuseserver_oper.getattr = fuseserver_getattr;
  fuseserver_oper.statfs = fuseserver_statfs;
  fuseserver_oper.readdir = fuseserver_readdir;
  fuseserver_oper.lookup = fuseserver_lookup;
  fuseserver_oper.create = fuseserver_create;
  fuseserver_oper.mknod = fuseserver_mknod;
  fuseserver_oper.open = fuseserver_open;
  fuseserver_oper.read = fuseserver_read;
  fuseserver_oper.write = fuseserver_write;
  fuseserver_oper.setattr = fuseserver_setattr;
  fuseserver_oper.unlink = fuseserver_unlink;
  fuseserver_oper.mkdir = fuseserver_mkdir;

  const char *fuse_argv[20];
  int fuse_argc = 0;
  fuse_argv[fuse_argc++] = argv[0];
#ifdef __APPLE__
  fuse_argv[fuse_argc++] = "-o";
  fuse_argv[fuse_argc++] = "nolocalcaches"; // no dir entry caching
  fuse_argv[fuse_argc++] = "-o";
  fuse_argv[fuse_argc++] = "daemon_timeout=86400";
#endif

  // everyone can play, why not?
  // fuse_argv[fuse_argc++] = "-o";
  // fuse_argv[fuse_argc++] = "allow_other";

  fuse_argv[fuse_argc++] = mountpoint;
  fuse_argv[fuse_argc++] = "-d";

  fuse_args args = FUSE_ARGS_INIT(fuse_argc, (char **)fuse_argv);
  int foreground;
  int res = fuse_parse_cmdline(&args, &mountpoint, 0 /*multithreaded*/,
                               &foreground);
  if (res == -1)
  {
    fprintf(stderr, "fuse_parse_cmdline failed\n");
    return 0;
  }

  args.allocated = 0;

  fd = fuse_mount(mountpoint, &args);
  if (fd == -1)
  {
    fprintf(stderr, "fuse_mount failed\n");
    exit(1);
  }

  struct fuse_session *se;

  se = fuse_lowlevel_new(&args, &fuseserver_oper, sizeof(fuseserver_oper),
                         NULL);
  if (se == 0)
  {
    fprintf(stderr, "fuse_lowlevel_new failed\n");
    exit(1);
  }

  struct fuse_chan *ch = fuse_kern_chan_new(fd);
  if (ch == NULL)
  {
    fprintf(stderr, "fuse_kern_chan_new failed\n");
    exit(1);
  }

  fuse_session_add_chan(se, ch);
  // err = fuse_session_loop_mt(se);   // FK: wheelfs does this; why?
  err = fuse_session_loop(se);

  fuse_session_destroy(se);
  close(fd);
  fuse_unmount(mountpoint);

  return err ? 1 : 0;
}
