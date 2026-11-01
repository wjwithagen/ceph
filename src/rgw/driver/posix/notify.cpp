// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab ft=cpp

#include "notify.h"
#if defined(__linux__) || defined(__FreeBSD__)
#include <sys/inotify.h>
#endif

namespace file::listing {

  std::unique_ptr<Notify> Notify::factory(Notifiable* n, const std::string& bucket_root)
  {
#if defined(__linux__) || defined(__FreeBSD__)
    return std::unique_ptr<Notify>(new Inotify(n, bucket_root));
#else
#error currently, rgw posix driver requires inotify
#endif /* __linux__) || defined(__FreeBSD__ */
    return nullptr;
  } /* Notify::factory */

} // namespace file::listing
