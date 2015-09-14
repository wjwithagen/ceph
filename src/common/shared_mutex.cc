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

#include <boost/uuid/uuid.hpp>
#include <boost/uuid/uuid_generators.hpp>
#include <boost/uuid/uuid_io.hpp>

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

shared_mutex_debug::shared_mutex_debug(const std::string& n)
  : l(PTHREAD_RWLOCK_INITIALIZER), nrlock(0), nwlock(0) {
  if (n.empty()) {
    uuid_d uu;
    uu.generate_random();
    name = string("Unnamed-Mutex-") + uu.to_string();
  } else {
    name = n;
  }
  if (g_lockdep)
    id = lockdep_register(name.c_str());
  else
    id = -1;
}

shared_mutex_debug::~shared_mutex_debug() {
  // The following check is racy but we are about to destroy the
  // object and we assume that there are no other users.
  assert(!is_locked());
  if (g_lockdep) {
    lockdep_unregister(id);
  }
}

void shared_mutex_debug::lock(bool no_lockdep) {
  if (g_lockdep && !no_lockdep)
    id = lockdep_will_lock(name.c_str(), id);
  int r = pthread_rwlock_wrlock(&l);
  // Allowed error codes for Mutex concept
  if (unlikely(r == EPERM ||
	       r == EDEADLK ||
	       r == EBUSY)) {
    throw std::system_error(r, std::generic_category());
  }
  assert(r == 0);
  if (g_lockdep && !no_lockdep)
    id = lockdep_locked(name.c_str(), id);
  ++nwlock;
}

void shared_mutex_debug::unlock(bool no_lockdep) noexcept {
  if (nwlock > 0) {
    --nwlock;
  } else {
    assert(nrlock > 0);
    --nrlock;
  }
  if (g_lockdep && !no_lockdep)
    id = lockdep_will_unlock(name.c_str(), id);
  int r = pthread_rwlock_unlock(&l);
  assert(r == 0);
}

bool shared_mutex_debug::try_lock(bool no_lockdep) {
  int r = pthread_rwlock_trywrlock(&l);
  switch (r) {
  case 0:
    if (g_lockdep && !no_lockdep)
      id = lockdep_locked(name.c_str(), id);
    ++nwlock;
    return true;
  case EBUSY:
    return false;
  default:
    throw std::system_error(r, std::generic_category());
  }
}

void shared_mutex_debug::lock_shared(bool no_lockdep) {
  if (g_lockdep && !no_lockdep)
    id = lockdep_will_lock(name.c_str(), id);
  int r = pthread_rwlock_rdlock(&l);
  // Allowed error codes for Mutex concept
  if (unlikely(r == EPERM ||
	       r == EDEADLK ||
	       r == EBUSY)) {
    throw std::system_error(r, std::generic_category());
  }
  assert(r == 0);
  if (g_lockdep && !no_lockdep)
    id = lockdep_locked(name.c_str(), id);
  ++nrlock;
}

bool shared_mutex_debug::try_lock_shared(bool no_lockdep) {
  int r = pthread_rwlock_tryrdlock(&l);
  switch (r) {
  case 0:
    ++nrlock;
    if (g_lockdep && !no_lockdep)
      id = lockdep_locked(name.c_str(), id);
    return true;
  case EBUSY:
    return false;
  default:
    throw std::system_error(r, std::generic_category());
  }
}

bool shared_timed_mutex_debug::try_lock_until(const ceph::real_time& t,
					      bool no_lockdep) {
  struct timespec ts = ceph::real_clock::to_timespec(t);
  int r = pthread_rwlock_timedwrlock(&l, &ts);
  switch (r) {
  case 0:
    if (g_lockdep && !no_lockdep)
      id = lockdep_locked(name.c_str(), id);
    ++nwlock;
    return true;
  case ETIMEDOUT:
    return false;
  default:
    throw std::system_error(r, std::generic_category());
  }
}

bool shared_timed_mutex_debug::try_lock_until(const CephContext* cct,
					      const ceph::real_time& t,
					      bool no_lockdep) {
  struct timespec ts = ceph::real_clock::to_timespec(t);
  if (cct) {
    // Same effect as skewing the clock forward
    ts.tv_sec -= cct->_conf->clock_offset;
  }
  int r = pthread_rwlock_timedwrlock(&l, &ts);
  switch (r) {
  case 0:
    if (g_lockdep && !no_lockdep)
      id = lockdep_locked(name.c_str(), id);
    ++nwlock;
    return true;
  case ETIMEDOUT:
    return false;
  default:
    throw std::system_error(r, std::generic_category());
  }
}

bool shared_timed_mutex_debug::try_lock_shared_until(const ceph::real_time& t,
						     bool no_lockdep) {
  struct timespec ts = ceph::real_clock::to_timespec(t);
  int r = pthread_rwlock_timedrdlock(&l, &ts);
  switch (r) {
  case 0:
    ++nrlock;
    if (g_lockdep && !no_lockdep)
      id = lockdep_locked(name.c_str(), id);
    return true;
  case ETIMEDOUT:
    return false;
  default:
    throw std::system_error(r, std::generic_category());
  }
}

bool shared_timed_mutex_debug::try_lock_shared_until(const CephContext* cct,
						     const ceph::real_time& t,
						     bool no_lockdep) {
  struct timespec ts = ceph::real_clock::to_timespec(t);
  int r = pthread_rwlock_timedrdlock(&l, &ts);
  switch (r) {
  case 0:
    ++nrlock;
    if (g_lockdep && !no_lockdep)
      id = lockdep_locked(name.c_str(), id);
    return true;
  case ETIMEDOUT:
    return false;
  default:
    throw std::system_error(r, std::generic_category());
  }
}
} // namespace ceph
