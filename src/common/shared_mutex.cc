// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2004-2006 Sage Weil <sage@newdream.net>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#include "ceph_context.h"
#include "config.h"
#include "shared_mutex.h"

namespace ceph {
bool shared_timed_mutex::try_lock_until(const CephContext* cct,
					const ceph::real_time& t) {
  struct timespec ts = ceph::real_clock::to_timespec(t);
  if (cct) {
    // Same effect as skewing the clock forward
    ts.tv_sec -= cct->_conf->clock_offset;
  }
  int r = pthread_rwlock_timedwrlock(&l, &ts);
  switch (r) {
  case 0:
    return true;
  case ETIMEDOUT:
    return false;
  default:
    throw std::system_error(r, std::generic_category());
  }
}

bool shared_timed_mutex::try_lock_shared_until(const CephContext* cct,
					       const ceph::real_time& t) {
  struct timespec ts = ceph::real_clock::to_timespec(t);
  if (cct) {
    // Same effect as skewing the clock forward
    ts.tv_sec -= cct->_conf->clock_offset;
  }
  int r = pthread_rwlock_timedrdlock(&l, &ts);
  switch (r) {
  case 0:
    return true;
  case ETIMEDOUT:
    return false;
  default:
    throw std::system_error(r, std::generic_category());
  }
}
} // namespace ceph
