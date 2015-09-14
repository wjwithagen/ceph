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

#ifndef CEPH_COMMON_SHARED_MUTEX_H
#define CEPH_COMMON_SHARED_MUTEX_H

#include <atomic>
#include <mutex>
#include <system_error>

#include <pthread.h>

#include "ceph_time.h"
#include "likely.h"

class CephContext;

namespace ceph {
class shared_mutex {
protected:
  pthread_rwlock_t l;
public:
  // Mutex concept is DefaultConstructible
  shared_mutex() : l(PTHREAD_RWLOCK_INITIALIZER) {}
  ~shared_mutex() = default;

  // Mutex concept is non-Copyable
  shared_mutex(const shared_mutex&) = delete;
  shared_mutex& operator =(const shared_mutex&) = delete;

  // Mutex concept is non-Movable
  shared_mutex(shared_mutex&&) = delete;
  shared_mutex& operator =(shared_mutex&&) = delete;

  // BasicLockable concept
  void lock() {
    int r = pthread_rwlock_wrlock(&l);
    // Allowed error codes for Mutex concept
    if (unlikely(r == EPERM ||
		 r == EDEADLK ||
		 r == EBUSY)) {
      throw std::system_error(r, std::generic_category());
    }
    assert(r == 0);
  }

  void unlock() noexcept {
    int r = pthread_rwlock_unlock(&l);
    assert(r == 0);
  }

  // Lockable concept
  bool try_lock() {
    int r = pthread_rwlock_trywrlock(&l);
    switch (r) {
    case 0:
      return true;
    case EBUSY:
      return false;
    default:
      throw std::system_error(r, std::generic_category());
    }
  }

  // SharedMutex concept
  void lock_shared() {
    int r = pthread_rwlock_rdlock(&l);
    // Allowed error codes for Mutex concept
    if (unlikely(r == EPERM ||
		 r == EDEADLK ||
		 r == EBUSY)) {
      throw std::system_error(r, std::generic_category());
    }
    assert(r == 0);
  }

  bool try_lock_shared() {
    int r = pthread_rwlock_tryrdlock(&l);
    switch (r) {
    case 0:
      return true;
    case EBUSY:
      return false;
    default:
      throw std::system_error(r, std::generic_category());
    }
  }

  // I think having unlock and unlock_shared both is schmuck bait,
  // but it's in the Standard and that's what I'm coding to.
  void unlock_shared() noexcept {
    int r = pthread_rwlock_unlock(&l);
    assert(r == 0);
  }
};

// This is equivalent to the shared_timed_mutex class in C++14. If we
// were coding to C++14 we would just use that. But since we're not,
// I'm implementing it here.

class shared_timed_mutex : public shared_mutex {
  // TimedLock/TimedMutex concepts
public:
  template<typename Rep, typename Period>
  bool try_lock_for(
    const typename std::chrono::duration<Rep, Period>& dur) {
    // POSIX requires that timedwrlock work in terms of
    // CLOCK_REALTIME only. This displeases me.
    return try_lock_until(ceph::real_clock::now() + dur);
  }

  template<typename Rep, typename Period>
  bool try_lock_for(
    const CephContext* cct,
    const typename std::chrono::duration<Rep, Period>& dur) {
    // POSIX requires that timedwrlock work in terms of
    // CLOCK_REALTIME only. This displeases me.
    return try_lock_until(cct, ceph::real_clock::now() + dur);
  }

  bool try_lock_until(const ceph::real_time& t) {
    struct timespec ts = ceph::real_clock::to_timespec(t);
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

  bool try_lock_until(const CephContext* cct, const ceph::real_time& t);

  template<typename Clock, typename Duration>
  bool try_lock_until(const typename std::chrono::time_point<
		      Clock, Duration>& t) {
    ceph::signedspan ss = t - Clock::now();
    if (ss < ceph::signedspan::zero())
      return try_lock();
    else
      return try_lock_for(ss);
  }

  template<typename Clock, typename Duration>
  bool try_lock_until(const CephContext* cct,
		      const typename std::chrono::time_point<
		      Clock, Duration>& t) {
    ceph::signedspan ss = t - Clock::now();
    if (ss < ceph::signedspan::zero())
      return try_lock();
    else
      return try_lock_for(cct, ss);
  }

  // SharedTimedMutex concept
  template<typename Rep, typename Period>
  bool try_lock_shared_for(
    const typename std::chrono::duration<Rep, Period>& dur) noexcept {
    // POSIX requires that timedrdlock work in terms of
    // CLOCK_REALTIME only. This displeases me.
    return try_lock_shared_until(ceph::real_clock::now() + dur);
  }

  template<typename Rep, typename Period>
  bool try_lock_shared_for(
    const CephContext* cct,
    const typename std::chrono::duration<Rep, Period>& dur) noexcept {
    // POSIX requires that timedrdlock work in terms of
    // CLOCK_REALTIME only. This displeases me.
    return try_lock_shared_until(cct, ceph::real_clock::now() + dur);
  }

  bool try_lock_shared_until(const ceph::real_time& t) {
    struct timespec ts = ceph::real_clock::to_timespec(t);
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

  bool try_lock_shared_until(const CephContext* cct,
			     const ceph::real_time& t);

  template<typename Clock, typename Duration>
  bool try_lock_shared_until(const typename std::chrono::time_point<
			     Clock, Duration>& t) {
    ceph::signedspan ss = t - Clock::now();
    if (ss < ceph::signedspan::zero())
      return try_lock_shared();
    else
      return try_lock_shared_for(ss);
  }

  template<typename Clock, typename Duration>
  bool try_lock_shared_until(const CephContext* cct,
			     const typename std::chrono::time_point<
			     Clock, Duration>& t) {
    ceph::signedspan ss = t - Clock::now();
    if (ss < ceph::signedspan::zero())
      return try_lock_shared();
    else
      return try_lock_shared_for(cct, ss);
  }
};

// C++14 shared_lock implementation
template<typename Mutex>
class shared_lock {
  typedef Mutex mutex_type;

  mutex_type* m;
  bool owns;

public:

  shared_lock() noexcept : m(nullptr), owns(false) {}

  shared_lock(const shared_lock&) = delete;

  shared_lock(shared_lock&& o) noexcept : m(o.m), owns(o.owns) {
    o.m = nullptr;
    o.owns = false;
  }

  explicit shared_lock(mutex_type& _m) : m(&_m) {
    m->lock_shared();
    owns = true;
  }

  shared_lock(mutex_type& _m, std::defer_lock_t) noexcept
    : m(&_m), owns(false) {}
  shared_lock(mutex_type& _m, std::try_to_lock_t) : m(&_m) {
    owns = m->try_lock_shared();
  }
  shared_lock(mutex_type& _m, std::adopt_lock_t) : m(&_m), owns(true) {}

  template<typename Rep, typename Period>
  shared_lock(mutex_type& _m,
	      const std::chrono::duration<Rep, Period>& dur)
    : m(&_m) {
    owns = m->try_lock_shared_for(dur);
  }

  template<class Clock, class Duration>
  shared_lock(mutex_type& _m,
	      const std::chrono::time_point<Clock, Duration>& t)
    : m(&_m) {
    owns = m->try_lock_shared_until(t);
  }

  ~shared_lock() {
    if (owns)
      m->unlock_shared();
  }

  shared_lock& operator =(const shared_lock&) = delete;

  shared_lock& operator =(shared_lock&& o) noexcept {
    if (owns)
      m->unlock_shared();
    m = o.m;
    owns = o.owns;
    o.m = nullptr;
    o.owns = false;
    return *this;
  }

  void lock() {
    if (unlikely(!m))
      throw std::system_error(EPERM, std::generic_category());
    if (unlikely(owns))
      throw std::system_error(EDEADLK, std::generic_category());
    m->lock_shared();
    owns = true;
  }

  bool try_lock() {
    if (unlikely(!m))
      throw std::system_error(EPERM, std::generic_category());
    if (unlikely(owns))
      throw std::system_error(EDEADLK, std::generic_category());
    return owns = m->try_lock_shared();
  }

  template<typename Rep, typename Period>
  bool try_lock_for(const std::chrono::duration<Rep, Period>& dur) {
    if (unlikely(!m))
      throw std::system_error(EPERM, std::generic_category());
    if (unlikely(owns))
      throw std::system_error(EDEADLK, std::generic_category());
    return owns = m->try_lock_shared_for(dur);
  }

  template<typename Clock, typename Duration>
  bool try_lock_until(const std::chrono::time_point<Clock, Duration>& t) {
    if (unlikely(!m))
      throw std::system_error(EPERM, std::generic_category());
    if (unlikely(owns))
      throw std::system_error(EDEADLK, std::generic_category());
    return owns = m->try_lock_shared_until(t);
  }

  void unlock() {
    if (unlikely(!owns))
      throw std::system_error(EPERM, std::generic_category());
    m->unlock_shared();
    owns = false;
  }

  void swap(shared_lock& o) noexcept {
    std::swap(m, o.m);
    std::swap(owns, o.owns);
  }

  mutex_type* release() noexcept {
    mutex_type* _m = m;
    owns = false;
    m = nullptr;
    return _m;
  }

  mutex_type* mutex() const {
    return m;
  }

  bool owns_lock() const {
    return owns;
  }

  explicit operator bool() const {
    return owns;
  }
};

class shared_mutex_debug {
protected:
  pthread_rwlock_t l;
  std::string name;
  int id;
  std::atomic<uint32_t> nrlock, nwlock;

public:
  // Mutex concept is DefaultConstructible
  shared_mutex_debug(const std::string& n = std::string());
  // Mutex is Destructible
  ~shared_mutex_debug();

  // Mutex concept is non-Copyable
  shared_mutex_debug(const shared_mutex_debug&) = delete;
  shared_mutex_debug& operator =(const shared_mutex_debug&) = delete;

  // Mutex concept is non-Movable
  shared_mutex_debug(shared_mutex_debug&&) = delete;
  shared_mutex_debug& operator =(shared_mutex_debug&&) = delete;

  // BasicLockable concept
  void lock(bool no_lockdep = false);

  void unlock(bool no_lockdep = false) noexcept;

  // Lockable concept
  bool try_lock(bool no_lockdep = false);

  // SharedMutex concept
  void lock_shared(bool no_lockdep = false);

  bool try_lock_shared(bool no_lockdep = false);

  // I think having unlock and unlock_shared both is schmuck bait,
  // but it's in the Standard and that's what I'm coding to.
  void unlock_shared(bool no_lockdep = false) noexcept {
    unlock(no_lockdep);
  }

  bool is_locked() const {
    return (nwlock > 0);
  }

  bool is_locked_shared() const {
    return (nrlock > 0);
  }

  explicit operator bool() const {
    return (nrlock > 0) || (nwlock > 0);
  }
};

class shared_timed_mutex_debug : public shared_mutex_debug {
public:
  // TimedLock/TimedMutex concepts
  template<typename Rep, typename Period>
  bool try_lock_for(
    const typename std::chrono::duration<Rep, Period>& dur,
    bool no_lockdep = false) {
    // POSIX requires that timedwrlock work in terms of
    // CLOCK_REALTIME only. This displeases me.
    return try_lock_until(ceph::real_clock::now() + dur, no_lockdep);
  }

  template<typename Rep, typename Period>
  bool try_lock_for(
    const CephContext* cct,
    const typename std::chrono::duration<Rep, Period>& dur,
    bool no_lockdep = false) {
    // POSIX requires that timedwrlock work in terms of
    // CLOCK_REALTIME only. This displeases me.
    return try_lock_until(cct, ceph::real_clock::now() + dur, no_lockdep);
  }

  bool try_lock_until(const ceph::real_time& t, bool no_lockdep = false);

  bool try_lock_until(const CephContext* cct, const ceph::real_time& t,
		      bool no_ockdep = false);

  template<typename Clock, typename Duration>
  bool try_lock_until(const typename std::chrono::time_point<
		      Clock, Duration>& t, bool no_lockdep = false) {
    ceph::signedspan ss = t - Clock::now();
    if (ss < ceph::signedspan::zero())
      return try_lock(no_lockdep);
    else
      return try_lock_for(ss, no_lockdep);
  }

  template<typename Clock, typename Duration>
  bool try_lock_until(const CephContext* cct,
		      const typename std::chrono::time_point<
		      Clock, Duration>& t,
		      bool no_lockdep = false) {
    ceph::signedspan ss = t - Clock::now();
    if (ss < ceph::signedspan::zero())
      return try_lock(no_lockdep);
    else
      return try_lock_for(cct, ss, no_lockdep);
  }

  // SharedTimedMutex concept
  template<typename Rep, typename Period>
  bool try_lock_shared_for(
    const typename std::chrono::duration<Rep, Period>& dur,
    bool no_lockdep = false) noexcept {
    // POSIX requires that timedrdlock work in terms of
    // CLOCK_REALTIME only. This displeases me.
    return try_lock_shared_until(ceph::real_clock::now() + dur, no_lockdep);
  }

  // SharedTimedMutex concept
  template<typename Rep, typename Period>
  bool try_lock_shared_for(
    const CephContext* cct,
    const typename std::chrono::duration<Rep, Period>& dur,
    bool no_lockdep = false) noexcept {
    // POSIX requires that timedrdlock work in terms of
    // CLOCK_REALTIME only. This displeases me.
    return try_lock_shared_until(cct, ceph::real_clock::now() + dur,
				 no_lockdep);
  }

  bool try_lock_shared_until(const ceph::real_time& t,
			     bool no_lockdep = false);

  bool try_lock_shared_until(const CephContext* cct,
			     const ceph::real_time& t,
			     bool no_lockdep = false);

  template<typename Clock, typename Duration>
  bool try_lock_shared_until(const typename std::chrono::time_point<
			     Clock, Duration>& t, bool no_lockdep = false) {
    ceph::signedspan ss = t - Clock::now();
    if (ss < ceph::signedspan::zero())
      return try_lock_shared(no_lockdep);
    else
      return try_lock_shared_for(ss, no_lockdep);
  }

  template<typename Clock, typename Duration>
  bool try_lock_shared_until(const CephContext* cct,
			     const typename std::chrono::time_point<
			     Clock, Duration>& t,
			     bool no_lockdep = false) {
    ceph::signedspan ss = t - Clock::now();
    if (ss < ceph::signedspan::zero())
      return try_lock_shared(no_lockdep);
    else
      return try_lock_shared_for(cct, ss, no_lockdep);
  }
};
} // namespace ceph

namespace std {
template<typename Mutex>
void swap(ceph::shared_lock<Mutex>& m1, ceph::shared_lock<Mutex>& ma) {
  m1.swap(ma);
}
} // namespace std

#endif // !CEPH_COMMON_SHARED_MUTEX_H
