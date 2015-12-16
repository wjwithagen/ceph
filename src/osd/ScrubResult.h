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

  struct SnapError {
    uint64_t errors = 0;
    // the head/snapset/dangling object related to this error
    hobject_t oid;
    SnapSet *snapset = nullptr;
    vector<snapid_t> missing;
    SnapError() = default;
    SnapError(const hobject_t& oid);
    // a clone without head is spotted, but i am expecting a clone belonging to
    // current snapset, or a new snapset/head.
    static SnapError headless_clone(const hobject_t& oid);
    void set_snapset(SnapSet *ss);
    // soid claims that it is a head or a snapdir, but its SS_ATTR
    // attr is missing.
    //
    // please note that missing/corrupted OI_ATTR attr is considered as an
    // error, but `PGBackend::be_compare_scrub_objects()` already filters out
    // objects with this error.
    void set_ss_attr_missing();
    void set_ss_attr_corrupted();
    // snapset with missing clone
    void add_clone_missing(snapid_t);
    // the snapset claims that it has clones but its `seq` is 0
    void set_snapset_mismatch();
    // two possiblities
    // 1. soid claims that it is a head object, but snapset puts otherwise
    // 2. soid claims that it is a snapdir, but snapset indicates that it has a
    //    head
    void set_head_mismatch();
    void add_headless_clone(const hobject_t& clone);
    // the snap is either missing in ss.clone_size or ss.clone_overlap
    // or its size is not consistent per OI_ATTR and SS_ATTR
    void add_size_mismatch(snapid_t);
    void encode(bufferlist& bl) const;
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

  inline string first_snap_key(int64_t pool)
  {
    return "SCRUB_SS_" + std::to_string(pool) + "-";
  }

  inline string to_snap_key(int64_t pool,
			    const std::string& name,
			    const std::string& nspace)
  {
    return "SCRUB_SS_" + std::to_string(pool) + "." + name + nspace;
  }

  inline string last_snap_key(int64_t pool)
  {
    return "SCRUB_SS_" + std::to_string(pool) + "/";
  }
}

#endif // CEPH_SCRUB_RESULT_H
