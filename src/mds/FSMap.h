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


#ifndef CEPH_FSMAP_H
#define CEPH_FSMAP_H

#include <errno.h>

#include "include/types.h"
#include "common/Clock.h"
#include "msg/Message.h"
#include "mds/MDSMap.h"

#include <set>
#include <map>
#include <string>

#include "common/config.h"

#include "include/CompatSet.h"
#include "include/ceph_features.h"
#include "common/Formatter.h"
#include "mds/mdstypes.h"

/*

 boot  --> standby, creating, or starting.


 dne  ---->   creating  ----->   active*
 ^ ^___________/                /  ^ ^
 |                             /  /  |
 destroying                   /  /   |
   ^                         /  /    |
   |                        /  /     |
 stopped <---- stopping* <-/  /      |
      \                      /       |
        ----- starting* ----/        |
                                     |
 failed                              |
    \                                |
     \--> replay*  --> reconnect* --> rejoin*

     * = can fail

*/

class CephContext;

#define MDS_FEATURE_INCOMPAT_BASE CompatSet::Feature(1, "base v0.20")
#define MDS_FEATURE_INCOMPAT_CLIENTRANGES CompatSet::Feature(2, "client writeable ranges")
#define MDS_FEATURE_INCOMPAT_FILELAYOUT CompatSet::Feature(3, "default file layouts on dirs")
#define MDS_FEATURE_INCOMPAT_DIRINODE CompatSet::Feature(4, "dir inode in separate object")
#define MDS_FEATURE_INCOMPAT_ENCODING CompatSet::Feature(5, "mds uses versioned encoding")
#define MDS_FEATURE_INCOMPAT_OMAPDIRFRAG CompatSet::Feature(6, "dirfrag is stored in omap")
#define MDS_FEATURE_INCOMPAT_INLINE CompatSet::Feature(7, "mds uses inline data")
#define MDS_FEATURE_INCOMPAT_NOANCHOR CompatSet::Feature(8, "no anchor table")

#define MDS_FS_NAME_DEFAULT "cephfs"

class Filesystem
{
  public:
  std::string fs_name;
  mds_namespace_t ns;
  epoch_t last_failure;  // mds epoch of last failure
  epoch_t last_failure_osd_epoch; // osd epoch of last failure; any mds entering replay needs
                                  // at least this osdmap to ensure the blacklist propagates.

  mds_rank_t tableserver;   // which MDS has snaptable
  mds_rank_t root;          // which MDS has root directory

  std::set<int64_t> data_pools;  // file data pools available to clients (via an ioctl).  first is the default.
  int64_t cas_pool;            // where CAS objects go
  int64_t metadata_pool;       // where fs metadata objects go

  bool ever_allowed_snaps; //< the cluster has ever allowed snap creation
  bool explicitly_allowed_snaps; //< the user has explicitly enabled snap creation

  bool inline_data_enabled;
  uint64_t max_file_size;

  uint32_t flags;

  /*
   * in: the set of logical mds #'s that define the cluster.  this is the set
   *     of mds's the metadata may be distributed over.
   * up: map from logical mds #'s to the addrs filling those roles.
   * failed: subset of @in that are failed.
   * stopped: set of nodes that have been initialized, but are not active.
   *
   *    @up + @failed = @in.  @in * @stopped = {}.
   */

  mds_rank_t max_mds; /* The maximum number of active MDSes. Also, the maximum rank. */

  std::set<mds_rank_t> in;              // currently defined cluster
  std::map<mds_rank_t,int32_t> inc;     // most recent incarnation.
  // which ranks are failed, stopped, damaged (i.e. not held by a daemon)
  std::set<mds_rank_t> failed, stopped, damaged;
  std::map<mds_rank_t, mds_gid_t> up;        // who is in those roles


  epoch_t get_last_failure() const { return last_failure; }
  epoch_t get_last_failure_osd_epoch() const { return last_failure_osd_epoch; }

  mds_rank_t get_max_mds() const { return max_mds; }
  void set_max_mds(mds_rank_t m) { max_mds = m; }

  uint64_t get_max_filesize() { return max_file_size; }
  mds_rank_t get_tableserver() const { return tableserver; }
  mds_rank_t get_root() const { return root; }

  const std::set<int64_t> &get_data_pools() const { return data_pools; }
  int64_t get_first_data_pool() const { return *data_pools.begin(); }
  int64_t get_cas_pool() const { return cas_pool; }
  int64_t get_metadata_pool() const { return metadata_pool; }
  bool is_data_pool(int64_t poolid) const {
    return data_pools.count(poolid);
  }

  unsigned get_num_in_mds() const {
    return in.size();
  }
  unsigned get_num_up_mds() const {
    return up.size();
  }
  int get_num_failed_mds() const {
    return failed.size();
  }

  // data pools
  void add_data_pool(int64_t poolid) {
    data_pools.insert(poolid);
  }
  int remove_data_pool(int64_t poolid) {
    std::set<int64_t>::iterator p = data_pools.find(poolid);
    if (p == data_pools.end()) {
      return -ENOENT;
    } else {
      data_pools.erase(p);
      return 0;
    }
  }

  // sets
  void get_mds_set(std::set<mds_rank_t>& s) const {
    s = in;
  }
  void get_up_mds_set(std::set<mds_rank_t>& s) const {
    for (std::map<mds_rank_t, mds_gid_t>::const_iterator p = up.begin();
	 p != up.end();
	 ++p)
      s.insert(p->first);
  }

  void get_failed_mds_set(std::set<mds_rank_t>& s) const {
    s = failed;
  }

  void get_stopped_mds_set(std::set<mds_rank_t>& s) const {
    s = stopped;
  }

  bool is_down(mds_rank_t m) const { return up.count(m) == 0; }
  bool is_up(mds_rank_t m) const { return up.count(m); }
  bool is_in(mds_rank_t m) const { return up.count(m) || failed.count(m); }
  bool is_out(mds_rank_t m) const { return !is_in(m); }

  bool is_failed(mds_rank_t m) const   { return failed.count(m); }
  bool is_stopped(mds_rank_t m) const    { return stopped.count(m); }

  bool is_dne(mds_rank_t m) const      { return in.count(m) == 0; }

  mds_rank_t get_random_up_mds() const {
    if (up.empty())
      return -1;
    auto p = up.begin();
    for (int n = rand() % up.size(); n; n--)
      ++p;
    return p->first;
  }

  /**
   * Get MDS ranks which are in but not up.
   */
  void get_down_mds_set(std::set<mds_rank_t> *s) const
  {
    assert(s != NULL);
    s->insert(failed.begin(), failed.end());
    s->insert(damaged.begin(), damaged.end());
  }


  bool is_any_failed() {
    return failed.size();
  }

  bool is_stopped() {
    return up.empty();
  }
  // cluster states
  bool is_full() const {
    return mds_rank_t(in.size()) >= max_mds;
  }

  int get_flags() const { return flags; }
  int test_flag(int f) const { return flags & f; }
  void set_flag(int f) { flags |= f; }
  void clear_flag(int f) { flags &= ~f; }

  bool get_inline_data_enabled() const { return inline_data_enabled; }
  void set_inline_data_enabled(bool enabled) { inline_data_enabled = enabled; }

  void set_snaps_allowed() {
    set_flag(CEPH_MDSMAP_ALLOW_SNAPS);
    ever_allowed_snaps = true;
    explicitly_allowed_snaps = true;
  }
  bool allows_snaps() const { return test_flag(CEPH_MDSMAP_ALLOW_SNAPS); }
  void clear_snaps_allowed() { clear_flag(CEPH_MDSMAP_ALLOW_SNAPS); }

  void dump(Formatter *f) const;


  void get_health(std::list<pair<health_status_t,string> >& summary,
                  std::list<pair<health_status_t,string> > *detail) const;

  void encode(bufferlist& bl) const;
  void decode(bufferlist::iterator& p);

  Filesystem()
    :

      fs_name(MDS_FS_NAME_DEFAULT),
      ns(MDS_NAMESPACE_NONE),
      last_failure(0),
      last_failure_osd_epoch(0),
      tableserver(0), root(0),
      cas_pool(-1),
      metadata_pool(0),
      ever_allowed_snaps(false),
      explicitly_allowed_snaps(false),
      inline_data_enabled(false),
      max_file_size(0),
      flags(0),
      max_mds(0)
  {
    std::cerr << "Filesystem::Filesystem " << fs_name << std::endl;
  }
};
WRITE_CLASS_ENCODER(Filesystem)

class FSMap {
public:
#if 0
  /* These states are the union of the set of possible states of an MDS daemon,
   * and the set of possible states of an MDS rank */
  typedef enum {
    // States of an MDS daemon not currently holding a rank
    // ====================================================
    MDSMap::STATE_NULL     =   CEPH_MDS_STATE_NULL,                                  // null value for fns returning this type.
    MDSMap::STATE_BOOT     =   CEPH_MDS_STATE_BOOT,                // up, boot announcement.  destiny unknown.
    MDSMap::STATE_STANDBY  =   CEPH_MDS_STATE_STANDBY,             // up, idle.  waiting for assignment by monitor.
    MDSMap::STATE_STANDBY_REPLAY = CEPH_MDS_STATE_STANDBY_REPLAY,  // up, replaying active node, ready to take over.

    // States of an MDS rank, and of any MDS daemon holding that rank
    // ==============================================================
    MDSMap::STATE_STOPPED  =   CEPH_MDS_STATE_STOPPED,        // down, once existed, but no subtrees. empty log.  may not be held by a daemon.
    MDSMap::STATE_ONESHOT_REPLAY = CEPH_MDS_STATE_REPLAYONCE, // up, replaying active node journal to verify it, then shutting down

    MDSMap::STATE_CREATING  =  CEPH_MDS_STATE_CREATING,       // up, creating MDS instance (new journal, idalloc..).
    MDSMap::STATE_STARTING  =  CEPH_MDS_STATE_STARTING,       // up, starting prior stopped MDS instance.

    MDSMap::STATE_REPLAY    =  CEPH_MDS_STATE_REPLAY,         // up, starting prior failed instance. scanning journal.
    MDSMap::STATE_RESOLVE   =  CEPH_MDS_STATE_RESOLVE,        // up, disambiguating distributed operations (import, rename, etc.)
    MDSMap::STATE_RECONNECT =  CEPH_MDS_STATE_RECONNECT,      // up, reconnect to clients
    MDSMap::STATE_REJOIN    =  CEPH_MDS_STATE_REJOIN,         // up, replayed journal, rejoining distributed cache
    MDSMap::STATE_CLIENTREPLAY = CEPH_MDS_STATE_CLIENTREPLAY, // up, active
    MDSMap::STATE_ACTIVE =     CEPH_MDS_STATE_ACTIVE,         // up, active
    MDSMap::STATE_STOPPING  =  CEPH_MDS_STATE_STOPPING,       // up, exporting metadata (-> standby or out)
    MDSMap::STATE_DNE       =  CEPH_MDS_STATE_DNE,             // down, rank does not exist

    // State which a daemon may send to MDSMonitor in its beacon
    // to indicate that offline repair is required.  Daemon must stop
    // immediately after indicating this state.
    MDSMap::STATE_DAMAGED   = CEPH_MDS_STATE_DAMAGED

    /*
     * In addition to explicit states, an MDS rank implicitly in state:
     *  - STOPPED if it is not currently associated with an MDS daemon gid but it
     *    is in FSMap::stopped
     *  - FAILED if it is not currently associated with an MDS daemon gid but it
     *    is in FSMap::failed
     *  - DNE if it is not currently associated with an MDS daemon gid and it is
     *    missing from both FSMap::failed and FSMap::stopped
     */
  } DaemonState;
#endif

  // indicate startup standby preferences for MDS
  // of course, if they have a specific rank to follow, they just set that!
  static const mds_rank_t MDS_NO_STANDBY_PREF; // doesn't have instructions to do anything
  static const mds_rank_t MDS_STANDBY_ANY;     // is instructed to be standby-replay, may
                                               // or may not have specific name to follow
  static const mds_rank_t MDS_STANDBY_NAME;    // standby for a named MDS
  static const mds_rank_t MDS_MATCHED_ACTIVE;  // has a matched standby, which if up
                                               // it should follow, but otherwise should
                                               // be assigned a rank

  struct mds_info_t {
    mds_gid_t global_id;
    std::string name;
    mds_role_t role;
    int32_t inc;
    MDSMap::DaemonState state;
    version_t state_seq;
    entity_addr_t addr;
    utime_t laggy_since;
    mds_rank_t standby_for_rank;
    std::string standby_for_name;
    std::set<mds_rank_t> export_targets;

    mds_info_t() : global_id(MDS_GID_NONE), inc(0), state(MDSMap::STATE_STANDBY), state_seq(0),
		   standby_for_rank(MDS_NO_STANDBY_PREF)
	{ }

    bool laggy() const { return !(laggy_since == utime_t()); }
    void clear_laggy() { laggy_since = utime_t(); }

    entity_inst_t get_inst() const { return entity_inst_t(entity_name_t::MDS(role.rank), addr); }

    void encode(bufferlist& bl, uint64_t features) const {
      assert(features & CEPH_FEATURE_MDSENC);  // JCSP Hack
      if ((features & CEPH_FEATURE_MDSENC) == 0 ) encode_unversioned(bl);
      else encode_versioned(bl, features);
    }
    void decode(bufferlist::iterator& p);
    void dump(Formatter *f) const;
    static void generate_test_instances(list<mds_info_t*>& ls);
  private:
    void encode_versioned(bufferlist& bl, uint64_t features) const;
    void encode_unversioned(bufferlist& bl) const;
  };


protected:
  // base map
  epoch_t epoch;
  utime_t created, modified;

  __u32 session_timeout;
  __u32 session_autoclose;



  std::map<mds_gid_t, mds_info_t> mds_info;


  std::map<mds_namespace_t, std::shared_ptr<Filesystem> > filesystems;

public:
  CompatSet compat;

  uint64_t next_filesystem_id;
  mds_namespace_t legacy_client_namespace;

  friend class MDSMonitor;

  FSMap() 
    : epoch(0),
      session_timeout(0),
      session_autoclose(0),
      next_filesystem_id(MDS_NAMESPACE_ANONYMOUS + 1),
      legacy_client_namespace(MDS_NAMESPACE_NONE)
  { }

  FSMap(const FSMap &rhs)
    :
      epoch(rhs.epoch),
      created(rhs.created),
      modified(rhs.modified),
      session_timeout(rhs.session_timeout),
      session_autoclose(rhs.session_autoclose),
      mds_info(rhs.mds_info)
  {
    for (auto &i : rhs.filesystems) {
      auto fs = i.second;
      filesystems[fs->ns] = std::make_shared<Filesystem>(
          *fs
          );
    }
  }

  const std::map<mds_namespace_t, std::shared_ptr<Filesystem> > get_filesystems() const
  {
    return filesystems;
  }
  bool any_filesystems() const {return !filesystems.empty(); }
  bool filesystem_exists(mds_namespace_t ns) const
    {return filesystems.count(ns) > 0;}

  utime_t get_session_timeout() {
    return utime_t(session_timeout,0);
  }

  epoch_t get_epoch() const { return epoch; }
  void inc_epoch() { epoch++; }

  const utime_t& get_created() const { return created; }
  void set_created(utime_t ct) { modified = created = ct; }
  const utime_t& get_modified() const { return modified; }
  void set_modified(utime_t mt) { modified = mt; }

  std::shared_ptr<Filesystem> get_filesystem(mds_namespace_t ns) const
  {
    return filesystems.at(ns);
  }

  int parse_filesystem(
      std::string const &ns_str,
      std::shared_ptr<Filesystem> *result
      ) const;

  /**
   * Return true if this pool is in use by any of the filesystems
   */
  bool pool_in_use(int64_t poolid) const {
    for (auto const &i : filesystems) {
      if (i.second->is_data_pool(poolid) || i.second->metadata_pool == poolid) {
        return true;
      }
    }
    return false;
  }

  const std::map<mds_gid_t,mds_info_t>& get_mds_info() { return mds_info; }
  const mds_info_t& get_mds_info(mds_role_t role) {
    assert(filesystems.count(role.ns));
    const auto &fs = filesystems.at(role.ns); 
    assert(fs->up.count(role.rank) && mds_info.count(fs->up[role.rank]));
    return mds_info[fs->up[role.rank]];
  }

  mds_gid_t find_mds_gid_by_name(const std::string& s) {
    for (std::map<mds_gid_t,mds_info_t>::const_iterator p = mds_info.begin();
	 p != mds_info.end();
	 ++p) {
      if (p->second.name == s) {
	return p->first;
      }
    }
    return MDS_GID_NONE;
  }

  const mds_info_t* find_by_name(const std::string& name) const {
    for (std::map<mds_gid_t, mds_info_t>::const_iterator p = mds_info.begin();
	 p != mds_info.end();
	 ++p) {
      if (p->second.name == name)
	return &p->second;
    }
    return NULL;
  }

  // FIXME: standby_for_rank is bogus now because it doesn't say which filesystem
  // this fn needs reworking to respect filesystem priorities and not have
  // a lower priority filesystem stealing MDSs needed by a higher priority
  // filesystem
  //
  // Speaking of... as well as having some filesystems higher priority, we need
  // policies like "I require at least one standby MDS", so that even when a
  // standby is available, a lower prio filesystem won't use it if that would
  // mean putting a higher priority filesystem into a non-redundant state.
  // Could be option of "require one MDS is standby" or "require one MDS
  // is exclusive standby" so as to distinguish the case where two filesystems
  // means two standbys vs where two filesystems means one standby
  mds_gid_t find_standby_for(mds_rank_t mds, std::string& name) const {
    std::map<mds_gid_t, mds_info_t>::const_iterator generic_standby
      = mds_info.end();
    for (std::map<mds_gid_t, mds_info_t>::const_iterator p = mds_info.begin();
	 p != mds_info.end();
	 ++p) {
      if ((p->second.state != MDSMap::STATE_STANDBY && p->second.state != MDSMap::STATE_STANDBY_REPLAY) ||
	  p->second.laggy() ||
	  p->second.role.rank >= 0)
	continue;
      if (p->second.standby_for_rank == mds || (name.length() && p->second.standby_for_name == name))
	return p->first;
      if (p->second.standby_for_rank < 0 && p->second.standby_for_name.length() == 0)
	generic_standby = p;
    }
    if (generic_standby != mds_info.end())
      return generic_standby->first;
    return MDS_GID_NONE;
  }

  mds_gid_t find_unused_for(mds_rank_t mds, std::string& name,
                            bool force_standby_active) const {
    for (std::map<mds_gid_t,mds_info_t>::const_iterator p = mds_info.begin();
         p != mds_info.end();
         ++p) {
      if (p->second.state != MDSMap::STATE_STANDBY ||
          p->second.laggy() ||
          p->second.role.rank >= 0)
        continue;
      if ((p->second.standby_for_rank == MDS_NO_STANDBY_PREF ||
           p->second.standby_for_rank == MDS_MATCHED_ACTIVE ||
           (p->second.standby_for_rank == MDS_STANDBY_ANY && force_standby_active))) {
        return p->first;
      }
    }
    return MDS_GID_NONE;
  }

  mds_gid_t find_replacement_for(mds_rank_t mds, std::string& name,
                                 bool force_standby_active) const {
    const mds_gid_t standby = find_standby_for(mds, name);
    if (standby)
      return standby;
    else
      return find_unused_for(mds, name, force_standby_active);
  }

  void get_health(list<pair<health_status_t,std::string> >& summary,
		  list<pair<health_status_t,std::string> > *detail) const;

  typedef enum
  {
    AVAILABLE = 0,
    TRANSIENT_UNAVAILABLE = 1,
    STUCK_UNAVAILABLE = 2

  } availability_t;

  /**
   * Return indication of whether cluster is available.  This is a
   * heuristic for clients to see if they should bother waiting to talk to
   * MDSs, or whether they should error out at startup/mount.
   *
   * A TRANSIENT_UNAVAILABLE result indicates that the cluster is in a
   * transition state like replaying, or is potentially about the fail over.
   * Clients should wait for an updated map before making a final decision
   * about whether the filesystem is mountable.
   *
   * A STUCK_UNAVAILABLE result indicates that we can't see a way that
   * the cluster is about to recover on its own, so it'll probably require
   * administrator intervention: clients should probaly not bother trying
   * to mount.
   */
  availability_t is_cluster_available() const;

  // mds states
  bool is_dne_gid(mds_gid_t gid) const
  {
    return mds_info.count(gid) == 0;
  }

  /**
   * Get MDS daemon status by GID
   */
  MDSMap::DaemonState get_state_gid(mds_gid_t gid) const {
    std::map<mds_gid_t,mds_info_t>::const_iterator i = mds_info.find(gid);
    if (i == mds_info.end())
      return MDSMap::STATE_NULL;
    return i->second.state;
  }

  mds_info_t& get_info_gid(mds_gid_t gid) { assert(mds_info.count(gid)); return mds_info[gid]; }

  //mds_info_t& get_info(mds_rank_t m) { assert(up.count(m)); return mds_info[up[m]]; }

  bool is_laggy_gid(mds_gid_t gid) const {
    if (!mds_info.count(gid))
      return false;
    std::map<mds_gid_t,mds_info_t>::const_iterator p = mds_info.find(gid);
    return p->second.laggy();
  }

  bool is_degraded(mds_namespace_t ns) const {   // degraded = some recovery in process.  fixes active membership and recovery_set.
    if (filesystems.count(ns) == 0) {
      // This namespace doesn't exist, so it isn't degraded
      return false;
    }

    if (!filesystems.at(ns)->failed.empty() || !filesystems.at(ns)->damaged.empty()) {
      return true;
    }
    for (auto &p : mds_info) {
      if (p.second.role.ns == ns && p.second.state >= MDSMap::STATE_REPLAY && p.second.state <= MDSMap::STATE_CLIENTREPLAY) {
        return true;
      }
    }

    return false;
  }

  std::shared_ptr<Filesystem> get_filesystem(const std::string &name)
  {
    for (auto &i : filesystems) {
      if (i.second->fs_name == name) {
        return i.second;
      }
    }

    return nullptr;
  }

  /**
   * Get whether a rank is 'up', i.e. has
   * an MDS daemon's entity_inst_t associated
   * with it.
   */
  bool have_inst(mds_role_t r) const {
    const auto fs = filesystems.at(r.ns);
    return fs->up.count(r.rank);
  }

  /**
   * Get the MDS daemon entity_inst_t for a rank
   * known to be up.
   */
  const entity_inst_t get_inst(mds_role_t r) const {
    const auto fs = filesystems.at(r.ns);
    return mds_info.at(fs->up.at(r.rank)).get_inst();
  }
  const entity_addr_t get_addr(mds_role_t r) const {
    const auto fs = filesystems.at(r.ns);
    assert(fs->up.count(r.rank));
    return mds_info.at(fs->up.at(r.rank)).addr;
  }

  /**
   * Get MDS rank state if the rank is up, else MDSMap::STATE_NULL
   */
  MDSMap::DaemonState get_state(mds_role_t r) const {
    const auto fs = get_filesystem(r.ns);
    const auto u = fs->up.find(r.rank);
    if (u == fs->up.end()) {
      return MDSMap::STATE_NULL;
    }
    return get_state_gid(u->second);
  }

  void get_recovery_mds_set(mds_namespace_t ns, std::set<mds_rank_t>& s) const {
    std::shared_ptr<const Filesystem> fs = filesystems.at(ns);
    s = fs->failed;
    for (const auto i : fs->up) {
      auto &gid = i.second;
      auto &info = mds_info.at(gid);
      if (info.role.ns == ns && info.state >= MDSMap::STATE_REPLAY
                             && info.state <= MDSMap::STATE_STOPPING) {
        s.insert(info.role.rank);
      }
    }
  }

  void get_clientreplay_or_active_or_stopping_mds_set(mds_namespace_t ns,
      std::set<mds_rank_t>& s) const {
    for (auto p : mds_info) {
      auto info = p.second;
      if (info.role.ns == ns && info.state >= MDSMap::STATE_CLIENTREPLAY
                             && info.state <= MDSMap::STATE_STOPPING) {
        s.insert(info.role.rank);
      }
    }
  }
  void get_mds_set(mds_namespace_t ns,
      std::set<mds_rank_t>& s, MDSMap::DaemonState state) const {
    for (auto p : mds_info) {
      auto info = p.second;
      if (info.role.ns == ns && info.state == state) {
        s.insert(info.role.rank);
      }
    }
  } 
  void get_active_mds_set(mds_namespace_t ns, std::set<mds_rank_t>& s) const {
    get_mds_set(ns, s, MDSMap::STATE_ACTIVE);
  }

  /**
   * How many of the MDSs assigned to this namespace are in this state?
   */
  unsigned get_num_mds(std::shared_ptr<const Filesystem> fs, int state) const {
    unsigned n = 0;
    for (const auto & p : fs->up) {
      const auto &gid = p.second;
      const auto &info = mds_info.at(gid);
      if (info.state == state) {
        ++n;
      }
    }
    return n;
  }

  bool is_resolving(mds_namespace_t ns) const {
    const auto &fs = filesystems.at(ns);
    return
      get_num_mds(fs, MDSMap::STATE_RESOLVE) > 0 &&
      get_num_mds(fs, MDSMap::STATE_REPLAY) == 0 &&
      fs->failed.empty();
  }

  bool is_rejoining(mds_namespace_t ns) const {  
    const auto &fs = filesystems.at(ns);
    // nodes are rejoining cache state
    return 
      get_num_mds(fs, MDSMap::STATE_REJOIN) > 0 &&
      get_num_mds(fs, MDSMap::STATE_REPLAY) == 0 &&
      get_num_mds(fs, MDSMap::STATE_RECONNECT) == 0 &&
      get_num_mds(fs, MDSMap::STATE_RESOLVE) == 0 &&
      fs->failed.empty();
  }


  bool is_boot(mds_role_t r) const { return get_state(r) == MDSMap::STATE_BOOT; }
  bool is_creating(mds_role_t r) const { return get_state(r) == MDSMap::STATE_CREATING; }
  bool is_starting(mds_role_t r) const { return get_state(r) == MDSMap::STATE_STARTING; }
  bool is_replay(mds_role_t r) const   { return get_state(r) == MDSMap::STATE_REPLAY; }
  bool is_resolve(mds_role_t r) const  { return get_state(r) == MDSMap::STATE_RESOLVE; }
  bool is_reconnect(mds_role_t r) const { return get_state(r) == MDSMap::STATE_RECONNECT; }
  bool is_rejoin(mds_role_t r) const   { return get_state(r) == MDSMap::STATE_REJOIN; }
  bool is_clientreplay(mds_role_t r) const { return get_state(r) == MDSMap::STATE_CLIENTREPLAY; }
  bool is_active(mds_role_t r) const  { return get_state(r) == MDSMap::STATE_ACTIVE; }
  bool is_stopping(mds_role_t r) const { return get_state(r) == MDSMap::STATE_STOPPING; }
  bool is_active_or_stopping(mds_role_t r) const {
    return is_active(r) || is_stopping(r);
  }
  bool is_clientreplay_or_active_or_stopping(mds_role_t r) const {
    return is_clientreplay(r) || is_active(r) || is_stopping(r);
  }

  bool is_followable(mds_role_t r) const {
    return (is_resolve(r) ||
	    is_replay(r) ||
	    is_rejoin(r) ||
	    is_clientreplay(r) ||
	    is_active(r) ||
	    is_stopping(r));
  }

  mds_role_t get_role_gid(mds_gid_t gid) {
    if (mds_info.count(gid))
      return mds_info[gid].role;
    return mds_role_t();
  }

  int get_inc_gid(mds_gid_t gid) {
    if (mds_info.count(gid))
      return mds_info[gid].inc;
    return -1;
  }
  void encode(bufferlist& bl, uint64_t features) const;
  void decode(bufferlist::iterator& p);
  void decode(bufferlist& bl) {
    bufferlist::iterator p = bl.begin();
    decode(p);
  }


  void print(ostream& out);
  void print_summary(Formatter *f, ostream *out);

  void dump(Formatter *f) const;
  static void generate_test_instances(list<FSMap*>& ls);
};
WRITE_CLASS_ENCODER_FEATURES(FSMap::mds_info_t)
WRITE_CLASS_ENCODER_FEATURES(FSMap)

inline ostream& operator<<(ostream& out, FSMap& m) {
  m.print_summary(NULL, &out);
  return out;
}

#endif
