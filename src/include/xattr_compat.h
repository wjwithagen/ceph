#ifndef CEPH_XATTR_COMPAT_H
#define CEPH_XATTR_COMPAT_H

#include <errno.h>
#include <string.h>
#include <stdint.h>
#include <sys/types.h>

#if defined(__FreeBSD__)

// The xattr shim

#ifndef XATTR_CREATE
#define XATTR_CREATE  1
#endif
#ifndef XATTR_REPLACE
#define XATTR_REPLACE 2
#endif

#include <sys/extattr.h>

static inline void
xattr_split(const char *name, int *ns, const char **bare)
{
  if (!strncmp(name, "user.", 5))        { *ns = EXTATTR_NAMESPACE_USER;   *bare = name + 5; }
  else if (!strncmp(name, "system.", 7)) { *ns = EXTATTR_NAMESPACE_SYSTEM; *bare = name + 7; }
  else                                   { *ns = EXTATTR_NAMESPACE_USER;   *bare = name; }
}

static inline ssize_t
getxattr(const char *path, const char *name, void *val, size_t sz)
{
  int ns; const char *bare;
  xattr_split(name, &ns, &bare);
  ssize_t rc = extattr_get_file(path, ns, bare, val, sz);
  if (rc < 0 && errno == ENOATTR) errno = ENODATA;
  return rc;
}

static inline int
setxattr(const char *path, const char *name, const void *val, size_t sz, int flags)
{
  int ns; const char *bare;
  xattr_split(name, &ns, &bare);
  if (flags & (XATTR_CREATE | XATTR_REPLACE)) {
    int have = extattr_get_file(path, ns, bare, NULL, 0) >= 0;
    if ((flags & XATTR_CREATE)  && have)  { errno = EEXIST;  return -1; }
    if ((flags & XATTR_REPLACE) && !have) { errno = ENODATA; return -1; }
  }
  return extattr_set_file(path, ns, bare, val, sz) < 0 ? -1 : 0;
}

static inline int
removexattr(const char *path, const char *name)
{
  int ns; const char *bare;
  xattr_split(name, &ns, &bare);
  int rc = extattr_delete_file(path, ns, bare);
  if (rc < 0 && errno == ENOATTR) errno = ENODATA;
  return rc;
}

static inline ssize_t
listxattr(const char *path, char *list, size_t size)
{
  struct ns_prefix { int ns; const char *prefix; size_t plen; };
  static const struct ns_prefix spaces[2] = {
    { EXTATTR_NAMESPACE_USER,   "user.",   5 },
    { EXTATTR_NAMESPACE_SYSTEM, "system.", 7 },
  };
  size_t out = 0;
  size_t si;

  for (si = 0; si < 2; si++) {
    const struct ns_prefix *s = &spaces[si];
    uint8_t buf[4096];
    ssize_t n = extattr_list_file(path, s->ns, buf, sizeof(buf));
    size_t i;

    if (n < 0) continue;

    i = 0;
    while (i < (size_t)n) {
      uint8_t len = buf[i++];
      size_t need = s->plen + len + 1;

      if (size) {
        if (out + need > size) { errno = ERANGE; return -1; }
        memcpy(list + out, s->prefix, s->plen); out += s->plen;
        memcpy(list + out, &buf[i], len);       out += len;
        list[out++] = '\0';
      } else {
        out += need;
      }
      i += len;
    }
  }
  return out;
}

#endif /* __FreeBSD__ */
#endif /* CEPH_XATTR_COMPAT_H */
