// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_SCRUB_RESULT_H
#define CEPH_SCRUB_RESULT_H

#include "SnapMapper.h"		// for OSDriver
#include "common/map_cacher.hpp"

namespace Scrub {

  /**
   * errors found when comparing shard with auth replica
   */
  struct ShardError {
    ShardError(const pg_shard_t& shard,
	       const ScrubMap::object *object = nullptr)
      : shard(shard), object(object)
    {}
    void set_shard_missing();
    void set_stat_error();
    void set_read_error();
    void set_omap_digest_mismatch();
    void set_data_digest_mismatch();
    void set_size_mismatch();
    void set_attr_missing();
    void set_attr_mismatch();
    void set_attr_unexpected();
    void set_attr_corrupted();

    const pg_shard_t shard;
    const ScrubMap::object *object;
    uint64_t errors = 0;
  };

  /**
   * errors found when 
   */
  struct ObjectError {
    const hobject_t oid;
    // osd => ScrubMap::object
    std::map<int32_t, const ScrubMap::object*> shards;
    uint64_t errors = 0;
    ObjectError(const hobject_t& oid);
    void set_auth_missing(const map<pg_shard_t, ScrubMap*>& maps);
    void set_shard_error(const ShardError& e);
    // technically, these are ShardErrors, but we don't have
    // their keys in omap when we encounter them in ReplicatedPG::_scrub().
    // if we can use the object_t for the key, the problem is solved.
    //
    // auth copy errors, most likely SS_ATTR/OI_ATTR
    void set_attr_missing(const char* name);
    void set_attr_decode_failure(const char* name);
    void set_oi_size_mismatch(uint64_t disk_size);

    void encode(bufferlist& bl) const;
  };

  class Store {
  public:
    Store(const coll_t& coll, const hobject_t& oid, ObjectStore* store);
    ~Store();
    void add_object_error(const ObjectError& e);
    void add_snap_error(const SnapError& e);
    bool empty() const;
    ObjectStore::Transaction *get_transaction();

  private:
    ObjectStore::Transaction* _transaction();

  private:
    // a temp object holding mappings from seq-id to inconsistencies found in
    // scrubbing
    OSDriver driver;
    MapCacher::MapCacher<std::string, bufferlist> backend;
    ObjectStore::Transaction *txn = nullptr;
  };

  inline hobject_t make_scrub_object(const pg_t& pgid)
  {
    ostringstream ss;
    ss << "scrub_" << pgid;
    return spg_t{pgid}.make_temp_object(ss.str());
  }

  inline string first_object_key(int64_t pool)
  {
    return "SCRUB_OBJ_" + std::to_string(pool) + "-";
  }

  // the object_key should be unique across pools
  inline string to_object_key(int64_t pool,
			      const std::string& name,
			      const std::string& nspace,
			      snapid_t snap)
  {
    return ("SCRUB_OBJ_" +
	    std::to_string(pool) + "." +
	    name + nspace + std::to_string(snap));
  }

  inline string last_object_key(int64_t pool)
  {
    return "SCRUB_OBJ_" + std::to_string(pool) + "/";
  }
}

#endif // CEPH_SCRUB_RESULT_H
