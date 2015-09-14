// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 &smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2011 New Dream Network
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public
 * License version 2, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#include <condition_variable>
#include <future>
#include <mutex>
#include <thread>

#include "common/ceph_time.h"
#include "common/shared_mutex.h"

#include "gtest/gtest.h"


template<typename SharedMutex>
static bool test_try_lock(SharedMutex* sm) {
  if (!sm->try_lock())
    return false;
  sm->unlock();
  return true;
}

template<typename SharedMutex>
static bool test_try_lock_shared(SharedMutex* sm) {
  if (!sm->try_lock_shared())
    return false;
  sm->unlock_shared();
  return true;
}

template<typename SharedTimedMutex, typename Rep, typename Period>
static bool test_try_lock_for(
  SharedTimedMutex* sm,
  const typename std::chrono::duration<Rep, Period>& dur) {
  if (!sm->try_lock_for(dur))
    return false;
  sm->unlock();
  return true;
}

template<typename SharedTimedMutex, typename Clock, typename Duration>
bool test_try_lock_until(
  SharedTimedMutex* sm,
  const typename std::chrono::time_point<Clock, Duration>& t) {
  if (!sm->try_lock_until(t))
    return false;
  sm->unlock();
  return true;
}

template<typename SharedTimedMutex, typename Rep, typename Period>
static bool test_try_lock_shared_for(
  SharedTimedMutex* sm,
  const typename std::chrono::duration<Rep, Period>& dur) {
  if (!sm->try_lock_shared_for(dur))
    return false;
  sm->unlock_shared();
  return true;
}

template<typename SharedTimedMutex, typename Clock, typename Duration>
bool test_try_lock_shared_until(
  SharedTimedMutex* sm,
  const typename std::chrono::time_point<Clock, Duration>& t) {
  if (!sm->try_lock_shared_until(t))
    return false;
  sm->unlock_shared();
  return true;
}

TEST(SharedMutex, Lock) {
  ceph::shared_mutex sm;
  auto ttl = &test_try_lock<ceph::shared_mutex>;
  auto ttls = &test_try_lock_shared<ceph::shared_mutex>;

  sm.lock();
  // What the heck, why do you use decay here?
  auto f1 = std::async(std::launch::async, ttl, &sm);
  ASSERT_FALSE(f1.get());

  auto f2 = std::async(std::launch::async, ttls, &sm);
  ASSERT_FALSE(f2.get());

  sm.unlock();

  auto f3 = std::async(std::launch::async, ttl, &sm);
  ASSERT_TRUE(f3.get());

  auto f4 = std::async(std::launch::async, ttls, &sm);
  ASSERT_TRUE(f4.get());
}

TEST(SharedMutex, LockShared) {
  ceph::shared_mutex sm;

  auto ttl = test_try_lock<ceph::shared_mutex>;
  auto ttls = test_try_lock_shared<ceph::shared_mutex>;

  sm.lock_shared();

  auto f1 = std::async(std::launch::async, ttl, &sm);
  ASSERT_FALSE(f1.get());

  auto f2 = std::async(std::launch::async, ttls, &sm);
  ASSERT_TRUE(f2.get());

  sm.unlock_shared();

  auto f3 = std::async(std::launch::async, ttl, &sm);
  ASSERT_TRUE(f3.get());

  auto f4 = std::async(std::launch::async, ttls, &sm);
  ASSERT_TRUE(f4.get());
}

TEST(SharedTimedMutex, TimedFail) {
  ceph::shared_timed_mutex sm;

  auto ttlf = test_try_lock_for<ceph::shared_timed_mutex, uint64_t,
				std::nano>;
  auto ttlsf = test_try_lock_shared_for<ceph::shared_timed_mutex, uint64_t,
					std::nano>;

  auto ttlu = test_try_lock_until<ceph::shared_timed_mutex, ceph::real_clock,
				  ceph::timespan>;
  auto ttlsu = test_try_lock_shared_until<ceph::shared_timed_mutex,
					  ceph::real_clock, ceph::timespan>;

  auto ttlum = test_try_lock_until<ceph::shared_timed_mutex, ceph::mono_clock,
				   ceph::timespan>;
  auto ttlsum = test_try_lock_shared_until<ceph::shared_timed_mutex,
					   ceph::mono_clock, ceph::timespan>;
  sm.lock();

  auto f1 = std::async(std::launch::async, ttlf, &sm,
		       std::chrono::milliseconds(10));
  ASSERT_FALSE(f1.get());

  auto f2 = std::async(std::launch::async, ttlu, &sm,
		       ceph::real_clock::now() +
		       std::chrono::milliseconds(10));
  ASSERT_FALSE(f2.get());

  auto f3 = std::async(std::launch::async, ttlum, &sm,
		       ceph::mono_clock::now() +
		       std::chrono::milliseconds(10));
  ASSERT_FALSE(f3.get());

  auto f4 = std::async(std::launch::async, ttlsf, &sm,
		       std::chrono::milliseconds(10));
  ASSERT_FALSE(f4.get());

  auto f5 = std::async(std::launch::async, ttlsu, &sm,
		       ceph::real_clock::now() +
		       std::chrono::milliseconds(10));
  ASSERT_FALSE(f5.get());

  auto f6 = std::async(std::launch::async, ttlsum, &sm,
		       ceph::mono_clock::now() +
		       std::chrono::milliseconds(10));
  ASSERT_FALSE(f6.get());

  sm.unlock();

  sm.lock_shared();

  auto f7 = std::async(std::launch::async, ttlf, &sm,
		       std::chrono::milliseconds(10));
  ASSERT_FALSE(f7.get());

  auto f8 = std::async(std::launch::async, ttlu, &sm,
		       ceph::real_clock::now() +
		       std::chrono::milliseconds(10));
  ASSERT_FALSE(f8.get());

  auto f9 = std::async(std::launch::async, ttlum, &sm,
		       ceph::mono_clock::now() +
		       std::chrono::milliseconds(10));
  ASSERT_FALSE(f9.get());

  sm.unlock_shared();
}

TEST(SharedTimedMutex, TimedAcquire) {
  ceph::shared_timed_mutex sm;

  auto ttlf = test_try_lock_for<ceph::shared_timed_mutex, uint64_t,
				std::nano>;
  auto ttlsf = test_try_lock_shared_for<ceph::shared_timed_mutex, uint64_t,
					std::nano>;

  auto ttlu = test_try_lock_until<ceph::shared_timed_mutex, ceph::real_clock,
				  ceph::timespan>;
  auto ttlsu = test_try_lock_shared_until<ceph::shared_timed_mutex,
					  ceph::real_clock, ceph::timespan>;

  auto ttlum = test_try_lock_until<ceph::shared_timed_mutex, ceph::mono_clock,
				   ceph::timespan>;
  auto ttlsum = test_try_lock_shared_until<ceph::shared_timed_mutex,
					   ceph::mono_clock, ceph::timespan>;

  sm.lock();
  auto f1 = std::async(std::launch::async, ttlf, &sm,
		       std::chrono::milliseconds(100));
  sm.unlock();
  ASSERT_TRUE(f1.get());

  sm.lock();
  auto f2 = std::async(std::launch::async, ttlu, &sm,
		       ceph::real_clock::now() +
		       std::chrono::milliseconds(100));
  sm.unlock();
  ASSERT_TRUE(f2.get());

  sm.lock();
  auto f3 = std::async(std::launch::async, ttlum, &sm,
		       ceph::mono_clock::now() +
		       std::chrono::milliseconds(100));
  sm.unlock();
  ASSERT_TRUE(f3.get());

  sm.lock();
  auto f4 = std::async(std::launch::async, ttlsf, &sm,
		       std::chrono::milliseconds(100));
  sm.unlock();
  ASSERT_TRUE(f4.get());

  sm.lock();
  auto f5 = std::async(std::launch::async, ttlsu, &sm,
		       ceph::real_clock::now() +
		       std::chrono::milliseconds(100));
  sm.unlock();
  ASSERT_TRUE(f5.get());

  sm.lock();
  auto f6 = std::async(std::launch::async, ttlsum, &sm,
		       ceph::mono_clock::now() +
		       std::chrono::milliseconds(100));
  sm.unlock();
  ASSERT_TRUE(f6.get());

  sm.lock_shared();
  auto f7 = std::async(std::launch::async, ttlf, &sm,
		       std::chrono::milliseconds(100));
  sm.unlock_shared();
  ASSERT_TRUE(f7.get());

  sm.lock_shared();
  auto f8 = std::async(std::launch::async, ttlu, &sm,
		       ceph::real_clock::now() +
		       std::chrono::milliseconds(100));
  sm.unlock_shared();
  ASSERT_TRUE(f8.get());

  sm.lock_shared();
  auto f9 = std::async(std::launch::async, ttlum, &sm,
		       ceph::mono_clock::now() +
		       std::chrono::milliseconds(100));
  sm.unlock_shared();
  ASSERT_TRUE(f9.get());
}

TEST(SharedTimedMutex, TimedUncontested) {
  ceph::shared_timed_mutex sm;

  auto ttlf = test_try_lock_for<ceph::shared_timed_mutex, uint64_t,
				std::nano>;
  auto ttlsf = test_try_lock_shared_for<ceph::shared_timed_mutex, uint64_t,
					std::nano>;

  auto ttlu = test_try_lock_until<ceph::shared_timed_mutex, ceph::real_clock,
				  ceph::timespan>;
  auto ttlsu = test_try_lock_shared_until<ceph::shared_timed_mutex,
					  ceph::real_clock, ceph::timespan>;

  auto ttlum = test_try_lock_until<ceph::shared_timed_mutex, ceph::mono_clock,
				   ceph::timespan>;
  auto ttlsum = test_try_lock_shared_until<ceph::shared_timed_mutex,
					   ceph::mono_clock, ceph::timespan>;

  auto f1 = std::async(std::launch::async, ttlf, &sm,
		       std::chrono::milliseconds(100));
  ASSERT_TRUE(f1.get());

  auto f2 = std::async(std::launch::async, ttlu, &sm,
		       ceph::real_clock::now() +
		       std::chrono::milliseconds(10));
  ASSERT_TRUE(f2.get());

  auto f3 = std::async(std::launch::async, ttlum, &sm,
		       ceph::mono_clock::now() +
		       std::chrono::milliseconds(10));
  ASSERT_TRUE(f3.get());

  auto f4 = std::async(std::launch::async, ttlsf, &sm,
		       std::chrono::milliseconds(10));
  ASSERT_TRUE(f4.get());

  auto f5 = std::async(std::launch::async, ttlsu, &sm,
		       ceph::real_clock::now() +
		       std::chrono::milliseconds(10));
  ASSERT_TRUE(f5.get());

  auto f6 = std::async(std::launch::async, ttlsum, &sm,
		       ceph::mono_clock::now() +
		       std::chrono::milliseconds(10));
  ASSERT_TRUE(f6.get());
}

TEST(SharedLock, DefaultConstructor) {
  typedef ceph::shared_lock<ceph::shared_timed_mutex> shared_lock;

  shared_lock l;

  ASSERT_EQ(l.mutex(), nullptr);
  ASSERT_FALSE(l.owns_lock());
  ASSERT_FALSE(!!l);

  ASSERT_THROW(l.lock(), std::system_error);
  ASSERT_THROW(l.try_lock(), std::system_error);
  ASSERT_THROW(l.try_lock_for(std::chrono::milliseconds(10)),
	       std::system_error);
  ASSERT_THROW(l.try_lock_until(ceph::real_clock::now() +
				std::chrono::milliseconds(10)),
	       std::system_error);
  ASSERT_THROW(l.try_lock_until(ceph::mono_clock::now() +
				std::chrono::milliseconds(10)),
	       std::system_error);
  ASSERT_THROW(l.unlock(), std::system_error);

  ASSERT_EQ(l.mutex(), nullptr);
  ASSERT_FALSE(l.owns_lock());
  ASSERT_FALSE(!!l);

  ASSERT_EQ(l.release(), nullptr);

  ASSERT_EQ(l.mutex(), nullptr);
  ASSERT_FALSE(l.owns_lock());
  ASSERT_FALSE(!!l);
}

TEST(SharedLock, LockUnlock) {
  ceph::shared_mutex sm;
  auto ttl = &test_try_lock<ceph::shared_mutex>;
  auto ttls = &test_try_lock_shared<ceph::shared_mutex>;

  typedef ceph::shared_lock<ceph::shared_mutex> shared_lock;

  shared_lock l(sm);

  ASSERT_EQ(l.mutex(), &sm);
  ASSERT_TRUE(l.owns_lock());
  ASSERT_TRUE(!!l);
  ASSERT_FALSE(std::async(std::launch::async, ttl, &sm).get());
  ASSERT_TRUE(std::async(std::launch::async, ttls, &sm).get());

  l.unlock();

  ASSERT_EQ(l.mutex(), &sm);
  ASSERT_FALSE(l.owns_lock());
  ASSERT_FALSE(!!l);
  ASSERT_TRUE(std::async(std::launch::async, ttl, &sm).get());
  ASSERT_TRUE(std::async(std::launch::async, ttls, &sm).get());

  l.lock();

  ASSERT_EQ(l.mutex(), &sm);
  ASSERT_TRUE(l.owns_lock());
  ASSERT_TRUE(!!l);
  ASSERT_FALSE(std::async(std::launch::async, ttl, &sm).get());
  ASSERT_TRUE(std::async(std::launch::async, ttls, &sm).get());
}

TEST(SharedLock, LockDestruct) {
  ceph::shared_mutex sm;
  auto ttl = &test_try_lock<ceph::shared_mutex>;
  auto ttls = &test_try_lock_shared<ceph::shared_mutex>;

  typedef ceph::shared_lock<ceph::shared_mutex> shared_lock;

  {
    shared_lock l(sm);

    ASSERT_EQ(l.mutex(), &sm);
    ASSERT_TRUE(l.owns_lock());
    ASSERT_TRUE(!!l);
    ASSERT_FALSE(std::async(std::launch::async, ttl, &sm).get());
    ASSERT_TRUE(std::async(std::launch::async, ttls, &sm).get());
  }

  ASSERT_TRUE(std::async(std::launch::async, ttl, &sm).get());
  ASSERT_TRUE(std::async(std::launch::async, ttls, &sm).get());
}

TEST(SharedLock, MoveConstruct) {
  ceph::shared_mutex sm;
  auto ttl = &test_try_lock<ceph::shared_mutex>;
  auto ttls = &test_try_lock_shared<ceph::shared_mutex>;

  typedef ceph::shared_lock<ceph::shared_mutex> shared_lock;

  {
    shared_lock l(sm);

    ASSERT_EQ(l.mutex(), &sm);
    ASSERT_TRUE(l.owns_lock());
    ASSERT_TRUE(!!l);
    ASSERT_FALSE(std::async(std::launch::async, ttl, &sm).get());
    ASSERT_TRUE(std::async(std::launch::async, ttls, &sm).get());

    shared_lock o(std::move(l));

    ASSERT_EQ(l.mutex(), nullptr);
    ASSERT_FALSE(l.owns_lock());
    ASSERT_FALSE(!!l);

    ASSERT_EQ(o.mutex(), &sm);
    ASSERT_TRUE(o.owns_lock());
    ASSERT_TRUE(!!o);
    ASSERT_FALSE(std::async(std::launch::async, ttl, &sm).get());
    ASSERT_TRUE(std::async(std::launch::async, ttls, &sm).get());

    o.unlock();

    shared_lock c(std::move(o));

    ASSERT_EQ(o.mutex(), nullptr);
    ASSERT_FALSE(o.owns_lock());
    ASSERT_FALSE(!!o);

    ASSERT_EQ(c.mutex(), &sm);
    ASSERT_FALSE(c.owns_lock());
    ASSERT_FALSE(!!c);

    ASSERT_TRUE(std::async(std::launch::async, ttl, &sm).get());
    ASSERT_TRUE(std::async(std::launch::async, ttls, &sm).get());
  }
}

TEST(SharedLock, MoveAssign) {
  ceph::shared_mutex sm;
  auto ttl = &test_try_lock<ceph::shared_mutex>;
  auto ttls = &test_try_lock_shared<ceph::shared_mutex>;

  typedef ceph::shared_lock<ceph::shared_mutex> shared_lock;

  {
    shared_lock l(sm);

    ASSERT_EQ(l.mutex(), &sm);
    ASSERT_TRUE(l.owns_lock());
    ASSERT_TRUE(!!l);
    ASSERT_FALSE(std::async(std::launch::async, ttl, &sm).get());
    ASSERT_TRUE(std::async(std::launch::async, ttls, &sm).get());

    shared_lock o;

    o = std::move(l);

    ASSERT_EQ(l.mutex(), nullptr);
    ASSERT_FALSE(l.owns_lock());
    ASSERT_FALSE(!!l);

    ASSERT_EQ(o.mutex(), &sm);
    ASSERT_TRUE(o.owns_lock());
    ASSERT_TRUE(!!o);
    ASSERT_FALSE(std::async(std::launch::async, ttl, &sm).get());
    ASSERT_TRUE(std::async(std::launch::async, ttls, &sm).get());

    o.unlock();

    shared_lock c(std::move(o));

    ASSERT_EQ(o.mutex(), nullptr);
    ASSERT_FALSE(o.owns_lock());
    ASSERT_FALSE(!!o);

    ASSERT_EQ(c.mutex(), &sm);
    ASSERT_FALSE(c.owns_lock());
    ASSERT_FALSE(!!c);
    ASSERT_TRUE(std::async(std::launch::async, ttl, &sm).get());
    ASSERT_TRUE(std::async(std::launch::async, ttls, &sm).get());

    shared_lock k;

    c = std::move(k);

    ASSERT_EQ(k.mutex(), nullptr);
    ASSERT_FALSE(k.owns_lock());
    ASSERT_FALSE(!!k);

    ASSERT_EQ(c.mutex(), nullptr);
    ASSERT_FALSE(c.owns_lock());
    ASSERT_FALSE(!!c);

    ASSERT_TRUE(std::async(std::launch::async, ttl, &sm).get());
    ASSERT_TRUE(std::async(std::launch::async, ttls, &sm).get());
  }
}

TEST(SharedLock, ConstructDeferred) {
  ceph::shared_mutex sm;
  auto ttl = &test_try_lock<ceph::shared_mutex>;
  auto ttls = &test_try_lock_shared<ceph::shared_mutex>;

  typedef ceph::shared_lock<ceph::shared_mutex> shared_lock;

  {
    shared_lock l(sm, std::defer_lock);
    ASSERT_EQ(l.mutex(), &sm);
    ASSERT_FALSE(l.owns_lock());
    ASSERT_FALSE(!!l);
    ASSERT_TRUE(std::async(std::launch::async, ttl, &sm).get());
    ASSERT_TRUE(std::async(std::launch::async, ttls, &sm).get());

    ASSERT_THROW(l.unlock(), std::system_error);

    ASSERT_EQ(l.mutex(), &sm);
    ASSERT_FALSE(l.owns_lock());
    ASSERT_FALSE(!!l);
    ASSERT_TRUE(std::async(std::launch::async, ttl, &sm).get());
    ASSERT_TRUE(std::async(std::launch::async, ttls, &sm).get());

    l.lock();
    ASSERT_EQ(l.mutex(), &sm);
    ASSERT_TRUE(l.owns_lock());
    ASSERT_TRUE(!!l);
    ASSERT_FALSE(std::async(std::launch::async, ttl, &sm).get());
    ASSERT_TRUE(std::async(std::launch::async, ttls, &sm).get());
  }

  {
    shared_lock l(sm, std::defer_lock);
    ASSERT_EQ(l.mutex(), &sm);
    ASSERT_FALSE(l.owns_lock());
    ASSERT_FALSE(!!l);
    ASSERT_TRUE(std::async(std::launch::async, ttl, &sm).get());
    ASSERT_TRUE(std::async(std::launch::async, ttls, &sm).get());

    ASSERT_THROW(l.unlock(), std::system_error);

    ASSERT_EQ(l.mutex(), &sm);
    ASSERT_FALSE(l.owns_lock());
    ASSERT_FALSE(!!l);
    ASSERT_TRUE(std::async(std::launch::async, ttl, &sm).get());
    ASSERT_TRUE(std::async(std::launch::async, ttls, &sm).get());
  }
  ASSERT_TRUE(std::async(std::launch::async, ttl, &sm).get());
  ASSERT_TRUE(std::async(std::launch::async, ttls, &sm).get());
}

TEST(SharedLock, ConstructTry) {
  ceph::shared_mutex sm;
  auto ttl = &test_try_lock<ceph::shared_mutex>;
  auto ttls = &test_try_lock_shared<ceph::shared_mutex>;

  typedef ceph::shared_lock<ceph::shared_mutex> shared_lock;

  {
    shared_lock l(sm, std::try_to_lock);
    ASSERT_EQ(l.mutex(), &sm);
    ASSERT_TRUE(l.owns_lock());
    ASSERT_TRUE(!!l);
    ASSERT_FALSE(std::async(std::launch::async, ttl, &sm).get());
    ASSERT_TRUE(std::async(std::launch::async, ttls, &sm).get());
  }

  {
    std::unique_lock<ceph::shared_mutex> l(sm);
    ASSERT_FALSE(std::async(std::launch::async, ttl, &sm).get());
    ASSERT_FALSE(std::async(std::launch::async, ttls, &sm).get());

    std::async(std::launch::async, [&sm, ttl, ttls]() {
	shared_lock l(sm, std::try_to_lock);
	ASSERT_EQ(l.mutex(), &sm);
	ASSERT_FALSE(l.owns_lock());
	ASSERT_FALSE(!!l);
	ASSERT_FALSE(std::async(std::launch::async, ttl, &sm).get());
	ASSERT_FALSE(std::async(std::launch::async, ttls, &sm).get());
      }).get();

    l.unlock();

    std::async(std::launch::async, [&sm, ttl, ttls]() {
	shared_lock l(sm, std::try_to_lock);
	ASSERT_EQ(l.mutex(), &sm);
	ASSERT_TRUE(l.owns_lock());
	ASSERT_TRUE(!!l);
	ASSERT_FALSE(std::async(std::launch::async, ttl, &sm).get());
	ASSERT_TRUE(std::async(std::launch::async, ttls, &sm).get());
      }).get();
  }
}

TEST(SharedLock, ConstructAdopt) {
  ceph::shared_mutex sm;
  auto ttl = &test_try_lock<ceph::shared_mutex>;
  auto ttls = &test_try_lock_shared<ceph::shared_mutex>;

  typedef ceph::shared_lock<ceph::shared_mutex> shared_lock;

  sm.lock_shared();
  ASSERT_FALSE(std::async(std::launch::async, ttl, &sm).get());
  ASSERT_TRUE(std::async(std::launch::async, ttls, &sm).get());

  {
    shared_lock l(sm, std::adopt_lock);
    ASSERT_EQ(l.mutex(), &sm);
    ASSERT_TRUE(l.owns_lock());
    ASSERT_TRUE(!!l);
    ASSERT_FALSE(std::async(std::launch::async, ttl, &sm).get());
    ASSERT_TRUE(std::async(std::launch::async, ttls, &sm).get());
  }

  ASSERT_TRUE(std::async(std::launch::async, ttl, &sm).get());
  ASSERT_TRUE(std::async(std::launch::async, ttls, &sm).get());
}

TEST(SharedLock, ConstructTryFor) {
  ceph::shared_timed_mutex sm;
  auto ttl = &test_try_lock<ceph::shared_timed_mutex>;
  auto ttls = &test_try_lock_shared<ceph::shared_timed_mutex>;

  typedef ceph::shared_lock<ceph::shared_timed_mutex> shared_lock;

  {
    shared_lock l(sm, std::chrono::milliseconds(10));
    ASSERT_EQ(l.mutex(), &sm);
    ASSERT_TRUE(l.owns_lock());
    ASSERT_TRUE(!!l);
    ASSERT_FALSE(std::async(std::launch::async, ttl, &sm).get());
    ASSERT_TRUE(std::async(std::launch::async, ttls, &sm).get());
  }

  {
    std::unique_lock<ceph::shared_mutex> l(sm);

    std::async(std::launch::async, [&sm, ttl, ttls]() {
	shared_lock l(sm, std::chrono::milliseconds(10));
	ASSERT_EQ(l.mutex(), &sm);
	ASSERT_FALSE(l.owns_lock());
	ASSERT_FALSE(!!l);
	ASSERT_FALSE(std::async(std::launch::async, ttl, &sm).get());
	ASSERT_FALSE(std::async(std::launch::async, ttls, &sm).get());
      }).get();

    l.unlock();

    std::async(std::launch::async, [&sm, ttl, ttls]() {
	shared_lock l(sm, std::chrono::milliseconds(10));
	ASSERT_EQ(l.mutex(), &sm);
	ASSERT_TRUE(l.owns_lock());
	ASSERT_TRUE(!!l);
	ASSERT_FALSE(std::async(std::launch::async, ttl, &sm).get());
	ASSERT_TRUE(std::async(std::launch::async, ttls, &sm).get());
      }).get();
  }

  {
    std::unique_lock<ceph::shared_mutex> l(sm);
    auto f = std::async(std::launch::async, [&sm, ttl, ttls]() {
	shared_lock l(sm, std::chrono::milliseconds(500));
	ASSERT_EQ(l.mutex(), &sm);
	ASSERT_TRUE(l.owns_lock());
	ASSERT_TRUE(!!l);
	ASSERT_FALSE(std::async(std::launch::async, ttl, &sm).get());
	ASSERT_TRUE(std::async(std::launch::async, ttls, &sm).get());
      });

    std::this_thread::sleep_for(std::chrono::milliseconds(50));
    l.unlock();

    f.get();
  }
}

TEST(SharedLock, ConstructTryUntil) {
  ceph::shared_timed_mutex sm;
  auto ttl = &test_try_lock<ceph::shared_timed_mutex>;
  auto ttls = &test_try_lock_shared<ceph::shared_timed_mutex>;

  typedef ceph::shared_lock<ceph::shared_timed_mutex> shared_lock;

  {
    shared_lock l(sm, ceph::real_clock::now() + std::chrono::milliseconds(10));
    ASSERT_EQ(l.mutex(), &sm);
    ASSERT_TRUE(l.owns_lock());
    ASSERT_TRUE(!!l);
    ASSERT_FALSE(std::async(std::launch::async, ttl, &sm).get());
    ASSERT_TRUE(std::async(std::launch::async, ttls, &sm).get());
  }

  {
    shared_lock l(sm, ceph::mono_clock::now() + std::chrono::milliseconds(10));
    ASSERT_EQ(l.mutex(), &sm);
    ASSERT_TRUE(l.owns_lock());
    ASSERT_TRUE(!!l);
    ASSERT_FALSE(std::async(std::launch::async, ttl, &sm).get());
    ASSERT_TRUE(std::async(std::launch::async, ttls, &sm).get());
  }

  {
    std::unique_lock<ceph::shared_mutex> l(sm);
    std::async(std::launch::async, [&sm, ttl, ttls]() {
	shared_lock l(sm, ceph::real_clock::now() +
		      std::chrono::milliseconds(10));
	ASSERT_EQ(l.mutex(), &sm);
	ASSERT_FALSE(l.owns_lock());
	ASSERT_FALSE(!!l);
	ASSERT_FALSE(std::async(std::launch::async, ttl, &sm).get());
	ASSERT_FALSE(std::async(std::launch::async, ttls, &sm).get());
      }).get();

    l.unlock();

    std::async(std::launch::async, [&sm, ttl, ttls]() {
	shared_lock l(sm, ceph::real_clock::now() +
		      std::chrono::milliseconds(10));
	ASSERT_EQ(l.mutex(), &sm);
	ASSERT_TRUE(l.owns_lock());
	ASSERT_TRUE(!!l);
	ASSERT_FALSE(std::async(std::launch::async, ttl, &sm).get());
	ASSERT_TRUE(std::async(std::launch::async, ttls, &sm).get());
      }).get();
  }

  {
    std::unique_lock<ceph::shared_mutex> l(sm);

    std::async(std::launch::async, [&sm, ttl, ttls]() {
	shared_lock l(sm, ceph::mono_clock::now() +
		      std::chrono::milliseconds(10));
	ASSERT_EQ(l.mutex(), &sm);
	ASSERT_FALSE(l.owns_lock());
	ASSERT_FALSE(!!l);
	ASSERT_FALSE(std::async(std::launch::async, ttl, &sm).get());
	ASSERT_FALSE(std::async(std::launch::async, ttls, &sm).get());
      }).get();

    l.unlock();

    std::async(std::launch::async, [&sm, ttl, ttls]() {
	shared_lock l(sm, ceph::mono_clock::now() +
		      std::chrono::milliseconds(10));
	ASSERT_EQ(l.mutex(), &sm);
	ASSERT_TRUE(l.owns_lock());
	ASSERT_TRUE(!!l);
	ASSERT_FALSE(std::async(std::launch::async, ttl, &sm).get());
	ASSERT_TRUE(std::async(std::launch::async, ttls, &sm).get());
      }).get();
  }

  {
    std::unique_lock<ceph::shared_mutex> l(sm);

    auto f = std::async(std::launch::async, [&sm, ttl, ttls]() {
	shared_lock l(sm, ceph::real_clock::now() +
		      std::chrono::milliseconds(500));
	ASSERT_EQ(l.mutex(), &sm);
	ASSERT_TRUE(l.owns_lock());
	ASSERT_TRUE(!!l);
	ASSERT_FALSE(std::async(std::launch::async, ttl, &sm).get());
	ASSERT_TRUE(std::async(std::launch::async, ttls, &sm).get());
      });

    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    l.unlock();

    f.get();
  }

  {
    std::unique_lock<ceph::shared_mutex> l(sm);

    auto f = std::async(std::launch::async, [&sm, ttl, ttls]() {
	shared_lock l(sm, ceph::mono_clock::now() +
		      std::chrono::milliseconds(500));
	ASSERT_EQ(l.mutex(), &sm);
	ASSERT_TRUE(l.owns_lock());
	ASSERT_TRUE(!!l);
	ASSERT_FALSE(std::async(std::launch::async, ttl, &sm).get());
	ASSERT_TRUE(std::async(std::launch::async, ttls, &sm).get());
      });

    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    l.unlock();

    f.get();
  }
}

TEST(SharedLock, TryLock) {
  ceph::shared_mutex sm;
  auto ttl = &test_try_lock<ceph::shared_mutex>;
  auto ttls = &test_try_lock_shared<ceph::shared_mutex>;

  typedef ceph::shared_lock<ceph::shared_mutex> shared_lock;

  {
    shared_lock l(sm, std::defer_lock);
    l.try_lock();

    ASSERT_EQ(l.mutex(), &sm);
    ASSERT_TRUE(l.owns_lock());
    ASSERT_TRUE(!!l);
    ASSERT_FALSE(std::async(std::launch::async, ttl, &sm).get());
    ASSERT_TRUE(std::async(std::launch::async, ttls, &sm).get());
  }

  {
    std::unique_lock<ceph::shared_mutex> l(sm);

    std::async(std::launch::async, [&sm, ttl, ttls]() {
	shared_lock l(sm, std::defer_lock);
	l.try_lock();

	ASSERT_EQ(l.mutex(), &sm);
	ASSERT_FALSE(l.owns_lock());
	ASSERT_FALSE(!!l);
	ASSERT_FALSE(std::async(std::launch::async, ttl, &sm).get());
	ASSERT_FALSE(std::async(std::launch::async, ttls, &sm).get());
      }).get();


    l.unlock();
    std::async(std::launch::async, [&sm, ttl, ttls]() {
	shared_lock l(sm, std::defer_lock);
	l.try_lock();

	ASSERT_EQ(l.mutex(), &sm);
	ASSERT_TRUE(l.owns_lock());
	ASSERT_TRUE(!!l);
	ASSERT_FALSE(std::async(std::launch::async, ttl, &sm).get());
	ASSERT_TRUE(std::async(std::launch::async, ttls, &sm).get());
      }).get();
  }
}

TEST(SharedLock, TryFor) {
  ceph::shared_timed_mutex sm;
  auto ttl = &test_try_lock<ceph::shared_timed_mutex>;
  auto ttls = &test_try_lock_shared<ceph::shared_timed_mutex>;

  typedef ceph::shared_lock<ceph::shared_timed_mutex> shared_lock;

  {
    shared_lock l(sm, std::defer_lock);
    l.try_lock_for(std::chrono::milliseconds(10));

    ASSERT_EQ(l.mutex(), &sm);
    ASSERT_TRUE(l.owns_lock());
    ASSERT_TRUE(!!l);
    ASSERT_FALSE(std::async(std::launch::async, ttl, &sm).get());
    ASSERT_TRUE(std::async(std::launch::async, ttls, &sm).get());
  }

  {
    std::unique_lock<ceph::shared_mutex> l(sm);

    std::async(std::launch::async, [&sm, ttl, ttls]() {
	shared_lock l(sm, std::defer_lock);
	l.try_lock_for(std::chrono::milliseconds(10));

	ASSERT_EQ(l.mutex(), &sm);
	ASSERT_FALSE(l.owns_lock());
	ASSERT_FALSE(!!l);
	ASSERT_FALSE(std::async(std::launch::async, ttl, &sm).get());
	ASSERT_FALSE(std::async(std::launch::async, ttls, &sm).get());
      }).get();

    l.unlock();

    std::async(std::launch::async, [&sm, ttl, ttls]() {
	shared_lock l(sm, std::defer_lock);
	l.try_lock_for(std::chrono::milliseconds(10));

	ASSERT_EQ(l.mutex(), &sm);
	ASSERT_TRUE(l.owns_lock());
	ASSERT_TRUE(!!l);
	ASSERT_FALSE(std::async(std::launch::async, ttl, &sm).get());
	ASSERT_TRUE(std::async(std::launch::async, ttls, &sm).get());
      }).get();
  }

  {
    std::unique_lock<ceph::shared_mutex> l(sm);

    auto f = std::async(std::launch::async, [&sm, ttl, ttls]() {
	shared_lock l(sm, std::defer_lock);
	l.try_lock_for(std::chrono::milliseconds(500));

	ASSERT_EQ(l.mutex(), &sm);
	ASSERT_TRUE(l.owns_lock());
	ASSERT_TRUE(!!l);
	ASSERT_FALSE(std::async(std::launch::async, ttl, &sm).get());
	ASSERT_TRUE(std::async(std::launch::async, ttls, &sm).get());
      });

    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    l.unlock();

    f.get();
  }
}

TEST(SharedLock, TryUntil) {
  ceph::shared_timed_mutex sm;
  auto ttl = &test_try_lock<ceph::shared_timed_mutex>;
  auto ttls = &test_try_lock_shared<ceph::shared_timed_mutex>;

  typedef ceph::shared_lock<ceph::shared_timed_mutex> shared_lock;

  {
    shared_lock l(sm, std::defer_lock);
    l.try_lock_until(ceph::real_clock::now() + std::chrono::milliseconds(10));

    ASSERT_EQ(l.mutex(), &sm);
    ASSERT_TRUE(l.owns_lock());
    ASSERT_TRUE(!!l);
    ASSERT_FALSE(std::async(std::launch::async, ttl, &sm).get());
    ASSERT_TRUE(std::async(std::launch::async, ttls, &sm).get());
  }

  {
    shared_lock l(sm, std::defer_lock);
    l.try_lock_until(ceph::mono_clock::now() + std::chrono::milliseconds(10));

    ASSERT_EQ(l.mutex(), &sm);
    ASSERT_TRUE(l.owns_lock());
    ASSERT_TRUE(!!l);
    ASSERT_FALSE(std::async(std::launch::async, ttl, &sm).get());
    ASSERT_TRUE(std::async(std::launch::async, ttls, &sm).get());
  }

  {
    std::unique_lock<ceph::shared_mutex> l(sm);

    std::async(std::launch::async, [&sm, ttl, ttls]() {
	shared_lock l(sm, std::defer_lock);
	l.try_lock_until(ceph::real_clock::now() +
			 std::chrono::milliseconds(10));

	ASSERT_EQ(l.mutex(), &sm);
	ASSERT_FALSE(l.owns_lock());
	ASSERT_FALSE(!!l);
	ASSERT_FALSE(std::async(std::launch::async, ttl, &sm).get());
	ASSERT_FALSE(std::async(std::launch::async, ttls, &sm).get());
      }).get();

    l.unlock();

    std::async(std::launch::async, [&sm, ttl, ttls]() {
	shared_lock l(sm, std::defer_lock);
	l.try_lock_until(ceph::real_clock::now() +
			 std::chrono::milliseconds(10));

	ASSERT_EQ(l.mutex(), &sm);
	ASSERT_TRUE(l.owns_lock());
	ASSERT_TRUE(!!l);
	ASSERT_FALSE(std::async(std::launch::async, ttl, &sm).get());
	ASSERT_TRUE(std::async(std::launch::async, ttls, &sm).get());
      }).get();
  }

  {
    std::unique_lock<ceph::shared_mutex> l(sm);

    std::async(std::launch::async, [&sm, ttl, ttls]() {
	shared_lock l(sm, std::defer_lock);
	l.try_lock_until(ceph::mono_clock::now() +
			 std::chrono::milliseconds(10));

	ASSERT_EQ(l.mutex(), &sm);
	ASSERT_FALSE(l.owns_lock());
	ASSERT_FALSE(!!l);
	ASSERT_FALSE(std::async(std::launch::async, ttl, &sm).get());
	ASSERT_FALSE(std::async(std::launch::async, ttls, &sm).get());
      }).get();

    l.unlock();

    std::async(std::launch::async, [&sm, ttl, ttls]() {
	shared_lock l(sm, std::defer_lock);
	l.try_lock_until(ceph::mono_clock::now() +
			 std::chrono::milliseconds(10));

	ASSERT_EQ(l.mutex(), &sm);
	ASSERT_TRUE(l.owns_lock());
	ASSERT_TRUE(!!l);
	ASSERT_FALSE(std::async(std::launch::async, ttl, &sm).get());
	ASSERT_TRUE(std::async(std::launch::async, ttls, &sm).get());
      }).get();
  }

  {
    std::unique_lock<ceph::shared_mutex> l(sm);

    auto f = std::async(std::launch::async, [&sm, ttl, ttls]() {
	shared_lock l(sm, std::defer_lock);
	l.try_lock_until(ceph::real_clock::now() +
			 std::chrono::milliseconds(500));

	ASSERT_EQ(l.mutex(), &sm);
	ASSERT_TRUE(l.owns_lock());
	ASSERT_TRUE(!!l);
	ASSERT_FALSE(std::async(std::launch::async, ttl, &sm).get());
	ASSERT_TRUE(std::async(std::launch::async, ttls, &sm).get());
      });

    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    l.unlock();

    f.get();
  }

  {
    std::unique_lock<ceph::shared_mutex> l(sm);

    auto f = std::async(std::launch::async, [&sm, ttl, ttls]() {
	shared_lock l(sm, std::defer_lock);
	l.try_lock_until(ceph::mono_clock::now() +
			 std::chrono::milliseconds(500));
	ASSERT_EQ(l.mutex(), &sm);
	ASSERT_TRUE(l.owns_lock());
	ASSERT_TRUE(!!l);
	ASSERT_FALSE(std::async(std::launch::async, ttl, &sm).get());
	ASSERT_TRUE(std::async(std::launch::async, ttls, &sm).get());
      });

    std::this_thread::sleep_for(std::chrono::milliseconds(50));
    l.unlock();

    f.get();
  }
}

TEST(SharedLock, Release) {
  ceph::shared_mutex sm;
  auto ttl = &test_try_lock<ceph::shared_mutex>;
  auto ttls = &test_try_lock_shared<ceph::shared_mutex>;

  typedef ceph::shared_lock<ceph::shared_mutex> shared_lock;

  {
    shared_lock l(sm);
    ASSERT_EQ(l.mutex(), &sm);
    ASSERT_TRUE(l.owns_lock());
    ASSERT_TRUE(!!l);
    ASSERT_FALSE(std::async(std::launch::async, ttl, &sm).get());
    ASSERT_TRUE(std::async(std::launch::async, ttls, &sm).get());

    l.release();
    ASSERT_EQ(l.mutex(), nullptr);
    ASSERT_FALSE(l.owns_lock());
    ASSERT_FALSE(!!l);

    ASSERT_FALSE(std::async(std::launch::async, ttl, &sm).get());
    ASSERT_TRUE(std::async(std::launch::async, ttls, &sm).get());
  }
  ASSERT_FALSE(std::async(std::launch::async, ttl, &sm).get());
  ASSERT_TRUE(std::async(std::launch::async, ttls, &sm).get());

  sm.unlock_shared();

  ASSERT_TRUE(std::async(std::launch::async, ttl, &sm).get());
  ASSERT_TRUE(std::async(std::launch::async, ttls, &sm).get());

  sm.lock_shared();
  {
    shared_lock l(sm, std::defer_lock);
    ASSERT_EQ(l.mutex(), &sm);
    ASSERT_FALSE(l.owns_lock());
    ASSERT_FALSE(!!l);
    ASSERT_FALSE(std::async(std::launch::async, ttl, &sm).get());
    ASSERT_TRUE(std::async(std::launch::async, ttls, &sm).get());

    l.release();
    ASSERT_EQ(l.mutex(), nullptr);
    ASSERT_FALSE(l.owns_lock());
    ASSERT_FALSE(!!l);

    ASSERT_FALSE(std::async(std::launch::async, ttl, &sm).get());
    ASSERT_TRUE(std::async(std::launch::async, ttls, &sm).get());
  }
  ASSERT_FALSE(std::async(std::launch::async, ttl, &sm).get());
  ASSERT_TRUE(std::async(std::launch::async, ttls, &sm).get());

  sm.unlock();

  ASSERT_TRUE(std::async(std::launch::async, ttl, &sm).get());
  ASSERT_TRUE(std::async(std::launch::async, ttls, &sm).get());
}

TEST(SharedLock, NoRecursion) {
  ceph::shared_timed_mutex sm;

  typedef ceph::shared_lock<ceph::shared_timed_mutex> shared_lock;

  shared_lock l(sm);
  ASSERT_THROW(l.lock(), std::system_error);
  ASSERT_THROW(l.try_lock(), std::system_error);
  ASSERT_THROW(l.try_lock_for(std::chrono::milliseconds(10)),
	       std::system_error);
  ASSERT_THROW(l.try_lock_until(ceph::real_clock::now() +
				std::chrono::milliseconds(10)),
	       std::system_error);
  ASSERT_THROW(l.try_lock_until(ceph::mono_clock::now() +
				std::chrono::milliseconds(10)),
	       std::system_error);
}
