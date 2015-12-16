#ifndef CEPH_RADOS_TYPES_HPP
#define CEPH_RADOS_TYPES_HPP

#include <map>
#include <utility>
#include <vector>
#include <stdint.h>
#include <string>

#include "buffer.h"
#include "rados_types.h"

namespace librados {

typedef uint64_t snap_t;

enum {
  SNAP_HEAD = (uint64_t)(-2),
  SNAP_DIR = (uint64_t)(-1)
};

struct clone_info_t {
  snap_t cloneid;
  std::vector<snap_t> snaps;          // ascending
  std::vector< std::pair<uint64_t,uint64_t> > overlap;  // with next newest
  uint64_t size;
  clone_info_t() : cloneid(0), size(0) {}
};

struct snap_set_t {
  std::vector<clone_info_t> clones;   // ascending
  snap_t seq;   // newest snapid seen by the object
  snap_set_t() : seq(0) {}
};

struct object_id_t {
  std::string name;
  std::string nspace;
  std::string locator;
  snap_t snap = 0;
  object_id_t() = default;
  object_id_t(const std::string& name,
              const std::string& nspace,
              const std::string& locator,
              snap_t snap)
    : name(name),
      nspace(nspace),
      locator(locator),
      snap(snap)
  {}
};

struct shard_info_t {
  bool missing = false;
  bool stat_error = false;
  bool read_error = false;
  bool data_digest_present = false;
  uint32_t data_digest = 0;
  bool omap_digest_present = false;
  uint32_t omap_digest = 0;
  uint64_t size = -1;
  std::map<std::string, bufferptr> attrs;
};

struct inconsistent_obj_t {
  enum {
    SHARD_MISSING        = 1 << 0,
    SHARD_STAT_ERR       = 1 << 1,
    SHARD_READ_ERR       = 1 << 2,
    DATA_DIGEST_MISMATCH = 1 << 3,
    OMAP_DIGEST_MISMATCH = 1 << 4,
    SIZE_MISMATCH        = 1 << 5,
    OI_SIZE_MISMATCH     = 1 << 6,
    ATTR_MISMATCH        = 1 << 7,
  };
  object_id_t object;
  uint64_t errors = 0;
  std::map<int32_t, shard_info_t> shard_by_osd;

  bool has_shard_missing() const {
    return errors & SHARD_MISSING;
  }
  bool has_stat_error() const {
    return errors & SHARD_STAT_ERR;
  }
  bool has_read_error() const {
    return errors & SHARD_READ_ERR;
  }
  bool has_data_digest_mismatch() const {
    return errors & DATA_DIGEST_MISMATCH;
  }
  bool has_omap_digest_mismatch() const {
    return errors & OMAP_DIGEST_MISMATCH;
  }
  bool has_size_mismatch() const {
    return errors & SIZE_MISMATCH;
  }
  bool has_attr_mismatch() const {
    return errors & ATTR_MISMATCH;
  }
};

struct inconsistent_snapset_t {
  enum {
    ATTR_MISSING   = 1 << 0,
    CLONE_MISSING  = 1 << 1,
    SNAP_MISMATCH  = 1 << 2,
    HEAD_MISMATCH  = 1 << 3,
    HEADLESS_CLONE = 1 << 4,
    SIZE_MISMATCH  = 1 << 5,
  };
  object_id_t object;
  uint64_t errors = 0;
  std::vector<snap_t> clones;
  std::vector<snap_t> missing;

  bool has_attr_missing() const {
    return errors & ATTR_MISSING;
  }
  bool has_clone_missing() const {
    return errors & CLONE_MISSING;
  }
  bool has_snapset_mismatch() const {
    return errors & SNAP_MISMATCH;
  }
  bool is_headless_clone() const {
    return errors & HEADLESS_CLONE;
  }
  bool has_size_mismatch() const {
    return errors & SIZE_MISMATCH;
  }
};

/**
 * @var all_nspaces
 * Pass as nspace argument to IoCtx::set_namespace()
 * before calling nobjects_begin() to iterate
 * through all objects in all namespaces.
 */
const std::string all_nspaces(LIBRADOS_ALL_NSPACES);

}
#endif
