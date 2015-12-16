// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
// vim: ts=8 sw=2 smarttab

#include "ScrubResult.h"
#include "osd_types.h"
#include "include/rados/rados_types.hpp"

namespace Scrub {

using inc_obj_t = librados::inconsistent_obj_t;

void ShardError::set_shard_missing()
{
  errors |= inc_obj_t::SHARD_MISSING;
}

void ShardError::set_stat_error()
{
  errors |= inc_obj_t::SHARD_STAT_ERR;
}

void ShardError::set_read_error()
{
  errors |= inc_obj_t::SHARD_READ_ERR;
}

void ShardError::set_omap_digest_mismatch()
{
  errors |= inc_obj_t::OMAP_DIGEST_MISMATCH;
}

void ShardError::set_data_digest_mismatch()
{
  errors |= inc_obj_t::DATA_DIGEST_MISMATCH;
}

void ShardError::set_size_mismatch()
{
  errors |= inc_obj_t::SIZE_MISMATCH;
}

// if any attr does not match, will attach all attrs in the scrub result
void ShardError::set_attr_missing()
{
  errors |= inc_obj_t::ATTR_MISMATCH;
}

void ShardError::set_attr_mismatch()
{
  errors |= inc_obj_t::ATTR_MISMATCH;
}

void ShardError::set_attr_unexpected()
{
  errors |= inc_obj_t::ATTR_MISMATCH;
}

SnapError::SnapError(const hobject_t& oid)
  : oid(oid)
{}

using inc_snapset_t = librados::inconsistent_snapset_t;

SnapError SnapError::headless_clone(const hobject_t& clone)
{
  SnapError e{clone};
  e.errors |= inc_snapset_t::HEADLESS_CLONE;
  return e;
}

void SnapError::set_snapset(SnapSet *ss)
{
  snapset = ss;
}

void SnapError::set_ss_attr_missing()
{
  errors |= inc_snapset_t::ATTR_MISSING;
}

void SnapError::set_ss_attr_corrupted()
{
  errors |= inc_snapset_t::ATTR_MISSING;
}

void SnapError::add_clone_missing(snapid_t snap)
{
  errors |= inc_snapset_t::CLONE_MISSING;
  missing.push_back(snap);
}

void SnapError::set_snapset_mismatch()
{
  errors |= inc_snapset_t::SNAP_MISMATCH;
}

void SnapError::set_head_mismatch()
{
  errors |= inc_snapset_t::HEAD_MISMATCH;
}

void SnapError::add_size_mismatch(snapid_t)
{
  errors |= inc_snapset_t::SIZE_MISMATCH;
}

void SnapError::encode(bufferlist& bl) const
{
  ENCODE_START(1, 1, bl);
  ::encode(oid, bl);
  ::encode(errors, bl);
  if (errors & inc_snapset_t::CLONE_MISSING) {
    ::encode(snapset->clones, bl);
    ::encode(missing, bl);
  }
  ENCODE_FINISH(bl);
}

ObjectError::ObjectError(const hobject_t& oid)
  : oid(oid)
{}

void ObjectError::set_auth_missing(const map<pg_shard_t, ScrubMap*>& maps)
{
  errors |= (inc_obj_t::SHARD_MISSING |
	     inc_obj_t::SHARD_READ_ERR |
	     inc_obj_t::OMAP_DIGEST_MISMATCH |
	     inc_obj_t::DATA_DIGEST_MISMATCH |
	     inc_obj_t::ATTR_MISMATCH);
  for (auto pg_map : maps) {
    auto oid_object = pg_map.second->objects.find(oid);
    if (oid_object == pg_map.second->objects.end()) {
      shards[pg_map.first.osd] = nullptr;
    } else {
      shards[pg_map.first.osd] = &oid_object->second;
    }
  }
}

void ObjectError::set_shard_error(const ShardError& e)
{
  errors |= e.errors;
  shards[e.shard.osd] = e.object;
}

void ObjectError::encode(bufferlist& bl) const
{
  ENCODE_START(1, 1, bl);
  ::encode(oid, bl);
  ::encode(errors, bl);
  const uint32_t n = shards.size();
  ::encode(n, bl);
  // selectively encode the inconsistent fields. if we always want to put all
  // the fields into the omap, use the default
  // "encode(const map<T,U>&, bufferlist&)" instead.
  for (auto osd_shard : shards) {
    ::encode(osd_shard.first, bl);  // the osd_id
    if (errors & inc_obj_t::SHARD_MISSING) {
      bool is_missing = !osd_shard.second;
      ::encode(is_missing, bl);
      if (is_missing) {
	continue;
      }
    }
    auto& shard = *osd_shard.second;
    if (errors & inc_obj_t::SHARD_STAT_ERR) {
      // ::encode(shard.stat_error, bl);
    }
    if (errors & inc_obj_t::SHARD_READ_ERR) {
      ::encode(shard.read_error, bl);
    }
    if (errors & inc_obj_t::DATA_DIGEST_MISMATCH) {
      ::encode(shard.digest, bl);
    }
    if (errors & inc_obj_t::OMAP_DIGEST_MISMATCH) {
      ::encode(shard.omap_digest, bl);
    }
    if (errors & inc_obj_t::SIZE_MISMATCH) {
      ::encode(shard.size, bl);
    }
    if (errors & inc_obj_t::ATTR_MISMATCH) {
      ::encode(shard.attrs, bl);
    }
  }
  ENCODE_FINISH(bl);
}

Store::Store(const coll_t& coll, const hobject_t& oid, ObjectStore* store)
  : driver(store, coll, ghobject_t(oid)),
    backend(&driver)
{}

Store::~Store()
{
  assert(!txn);
}

void
Store::add_object_error(const ObjectError& e)
{
  const string key = Scrub::to_object_key(e.oid.pool, e.oid.oid.name,
					  e.oid.nspace, e.oid.snap);
  bufferlist bl;
  e.encode(bl);
  map<string, bufferlist> keys;
  keys[key] = bl;
  OSDriver::OSTransaction t = driver.get_transaction(_transaction());
  backend.set_keys(keys, &t);
}

void
Store::add_snap_error(const SnapError& e)
{
  OSDriver::OSTransaction t = driver.get_transaction(_transaction());
  map<string, bufferlist> keys;
  bufferlist bl;
  e.encode(bl);
  const string key = to_snap_key(e.oid.pool, e.oid.oid.name,
				 e.oid.nspace);
  keys[key] = bl;
  backend.set_keys(keys, &t);
}

bool
Store::empty() const
{
  return !txn || txn->empty();
}

ObjectStore::Transaction*
Store::_transaction()
{
  if (!txn) {
    txn = new ObjectStore::Transaction;
  }
  return txn;
}

ObjectStore::Transaction*
Store::get_transaction()
{
  assert(txn);
  auto ret = txn;
  txn = nullptr;
  return ret;
}

} // namespace Scrub
