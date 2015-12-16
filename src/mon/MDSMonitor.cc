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

#include <sstream>
#include <boost/utility.hpp>

#include "MDSMonitor.h"
#include "Monitor.h"
#include "MonitorDBStore.h"
#include "OSDMonitor.h"

#include "common/strtol.h"
#include "common/ceph_argparse.h"
#include "common/perf_counters.h"
#include "common/Timer.h"
#include "common/config.h"
#include "common/cmdparse.h"

#include "messages/MMDSMap.h"
#include "messages/MFSMap.h"
#include "messages/MMDSBeacon.h"
#include "messages/MMDSLoadTargets.h"
#include "messages/MMonCommand.h"
#include "messages/MGenericMessage.h"

#include "include/assert.h"
#include "include/str_list.h"

#include "mds/mdstypes.h"

#define dout_subsys ceph_subsys_mon
#undef dout_prefix
#define dout_prefix _prefix(_dout, mon, fsmap)
static ostream& _prefix(std::ostream *_dout, Monitor *mon, FSMap const& fsmap) {
  return *_dout << "mon." << mon->name << "@" << mon->rank
		<< "(" << mon->get_state_name()
		<< ").mds e" << fsmap.get_epoch() << " ";
}


/*
 * Specialized implementation of cmd_getval to allow us to parse
 * out strongly-typedef'd types
 */
template<> bool cmd_getval(CephContext *cct, const cmdmap_t& cmdmap,
                std::string k, mds_gid_t &val)
{
  return cmd_getval(cct, cmdmap, k, (int64_t&)val);
}

template<> bool cmd_getval(CephContext *cct, const cmdmap_t& cmdmap,
                std::string k, mds_rank_t &val)
{
  return cmd_getval(cct, cmdmap, k, (int64_t&)val);
}

template<> bool cmd_getval(CephContext *cct, const cmdmap_t& cmdmap,
                std::string k, MDSMap::DaemonState &val)
{
  return cmd_getval(cct, cmdmap, k, (int64_t&)val);
}

static const string MDS_METADATA_PREFIX("mds_metadata");


// my methods

void MDSMonitor::print_map(FSMap &m, int dbl)
{
  dout(dbl) << "print_map\n";
  m.print(*_dout);
  *_dout << dendl;
}

void MDSMonitor::create_new_fs(FSMap &m, const std::string &name, int metadata_pool, int data_pool)
{
  auto fs = std::make_shared<Filesystem>();
  fs->fs_name = name;
  fs->max_mds = g_conf->max_mds;
  fs->data_pools.insert(data_pool);
  fs->metadata_pool = metadata_pool;
  fs->cas_pool = -1;
  fs->ns = m.next_filesystem_id++;
  fs->max_file_size = g_conf->mds_max_file_size;
  m.filesystems[fs->ns] = fs;

  // ANONYMOUS is only for upgrades from legacy mdsmaps, we should
  // have initialized next_filesystem_id such that it's never used here.
  assert(fs->ns != MDS_NAMESPACE_ANONYMOUS);

  // Created first filesystem?  Set it as the one
  // for legacy clients to use
  if (m.filesystems.size() == 1) {
    m.legacy_client_namespace = fs->ns;
  }

  print_map(m);
}


// service methods
void MDSMonitor::create_initial()
{
  dout(10) << "create_initial" << dendl;
}


void MDSMonitor::update_from_paxos(bool *need_bootstrap)
{
  version_t version = get_last_committed();
  if (version == fsmap.epoch)
    return;
  assert(version >= fsmap.epoch);

  dout(10) << __func__ << " version " << version
	   << ", my e " << fsmap.epoch << dendl;

  // read and decode
  mdsmap_bl.clear();
  int err = get_version(version, mdsmap_bl);
  assert(err == 0);

  assert(mdsmap_bl.length() > 0);
  dout(10) << __func__ << " got " << version << dendl;
  fsmap.decode(mdsmap_bl);

  // new map
  dout(4) << "new map" << dendl;
  print_map(fsmap, 0);

  check_subs();
  update_logger();
}

void MDSMonitor::init()
{
  (void)load_metadata(pending_metadata);
}

void MDSMonitor::create_pending()
{
  pending_fsmap = fsmap;
  pending_fsmap.epoch++;

  if (pending_fsmap.get_epoch() == 1) {
    // First update to the map, initialize some things from settings
    pending_fsmap.created = ceph_clock_now(g_ceph_context);
    pending_fsmap.session_timeout = g_conf->mds_session_timeout;
    pending_fsmap.session_autoclose = g_conf->mds_session_autoclose;
    pending_fsmap.compat = get_mdsmap_compat_set_default();
  }

  dout(10) << "create_pending e" << pending_fsmap.epoch << dendl;
}

void MDSMonitor::encode_pending(MonitorDBStore::TransactionRef t)
{
  dout(10) << "encode_pending e" << pending_fsmap.epoch << dendl;

  pending_fsmap.modified = ceph_clock_now(g_ceph_context);

  // print map iff 'debug mon = 30' or higher
  print_map(pending_fsmap, 30);

  // apply to paxos
  assert(get_last_committed() + 1 == pending_fsmap.epoch);
  bufferlist mdsmap_bl;
  pending_fsmap.encode(mdsmap_bl, mon->get_quorum_features());

  /* put everything in the transaction */
  put_version(t, pending_fsmap.epoch, mdsmap_bl);
  put_last_committed(t, pending_fsmap.epoch);

  // Encode MDSHealth data
  for (std::map<uint64_t, MDSHealth>::iterator i = pending_daemon_health.begin();
      i != pending_daemon_health.end(); ++i) {
    bufferlist bl;
    i->second.encode(bl);
    t->put(MDS_HEALTH_PREFIX, stringify(i->first), bl);
  }

  for (std::set<uint64_t>::iterator i = pending_daemon_health_rm.begin();
      i != pending_daemon_health_rm.end(); ++i) {
    t->erase(MDS_HEALTH_PREFIX, stringify(*i));
  }
  pending_daemon_health_rm.clear();
  remove_from_metadata(t);
}

version_t MDSMonitor::get_trim_to()
{
  version_t floor = 0;
  if (g_conf->mon_mds_force_trim_to > 0 &&
      g_conf->mon_mds_force_trim_to < (int)get_last_committed()) {
    floor = g_conf->mon_mds_force_trim_to;
    dout(10) << __func__ << " explicit mon_mds_force_trim_to = "
             << floor << dendl;
  }

  unsigned max = g_conf->mon_max_mdsmap_epochs;
  version_t last = get_last_committed();

  if (last - get_first_committed() > max && floor < last - max)
    return last - max;
  return floor;
}

void MDSMonitor::update_logger()
{
  dout(10) << "update_logger" << dendl;

  uint64_t up = 0;
  uint64_t in = 0;
  uint64_t failed = 0;
  for (auto i : fsmap.filesystems) {
    up += i.second->get_num_up_mds();
    in += i.second->get_num_in_mds();
    failed += i.second->get_num_failed_mds();
  }
  mon->cluster_logger->set(l_cluster_num_mds_up, up);
  mon->cluster_logger->set(l_cluster_num_mds_in, in);
  mon->cluster_logger->set(l_cluster_num_mds_failed, failed);
  mon->cluster_logger->set(l_cluster_mds_epoch, fsmap.get_epoch());
}

bool MDSMonitor::preprocess_query(MonOpRequestRef op)
{
  op->mark_mdsmon_event(__func__);
  PaxosServiceMessage *m = static_cast<PaxosServiceMessage*>(op->get_req());
  dout(10) << "preprocess_query " << *m << " from " << m->get_orig_source_inst() << dendl;

  switch (m->get_type()) {
    
  case MSG_MDS_BEACON:
    return preprocess_beacon(op);
    
  case MSG_MON_COMMAND:
    return preprocess_command(op);

  case MSG_MDS_OFFLOAD_TARGETS:
    return preprocess_offload_targets(op);

  default:
    assert(0);
    return true;
  }
}

void MDSMonitor::_note_beacon(MMDSBeacon *m)
{
  mds_gid_t gid = mds_gid_t(m->get_global_id());
  version_t seq = m->get_seq();

  dout(15) << "_note_beacon " << *m << " noting time" << dendl;
  last_beacon[gid].stamp = ceph_clock_now(g_ceph_context);  
  last_beacon[gid].seq = seq;
}

bool MDSMonitor::preprocess_beacon(MonOpRequestRef op)
{
  op->mark_mdsmon_event(__func__);
  MMDSBeacon *m = static_cast<MMDSBeacon*>(op->get_req());
  MDSMap::DaemonState state = m->get_state();
  mds_gid_t gid = m->get_global_id();
  version_t seq = m->get_seq();
  FSMap::mds_info_t info;

  // check privileges, ignore if fails
  MonSession *session = m->get_session();
  assert(session);
  if (!session->is_capable("mds", MON_CAP_X)) {
    dout(0) << "preprocess_beacon got MMDSBeacon from entity with insufficient privileges "
	    << session->caps << dendl;
    goto ignore;
  }

  if (m->get_fsid() != mon->monmap->fsid) {
    dout(0) << "preprocess_beacon on fsid " << m->get_fsid() << " != " << mon->monmap->fsid << dendl;
    goto ignore;
  }

  dout(12) << "preprocess_beacon " << *m
	   << " from " << m->get_orig_source_inst()
	   << " " << m->get_compat()
	   << dendl;

  // make sure the address has a port
  if (m->get_orig_source_addr().get_port() == 0) {
    dout(1) << " ignoring boot message without a port" << dendl;
    goto ignore;
  }

  // check compat
  if (!m->get_compat().writeable(fsmap.compat)) {
    dout(1) << " mds " << m->get_source_inst() << " can't write to fsmap " << fsmap.compat << dendl;
    goto ignore;
  }

  // fw to leader?
  if (!mon->is_leader())
    return false;

  // booted, but not in map?
  if (pending_fsmap.is_dne_gid(gid)) {
    if (state != MDSMap::STATE_BOOT) {
      dout(7) << "mds_beacon " << *m << " is not in fsmap (state "
              << ceph_mds_state_name(state) << ")" << dendl;

      MDSMap *generated = generate_mds_map(MDS_NAMESPACE_NONE);
      mon->send_reply(op, new MMDSMap(mon->monmap->fsid, generated));
      delete generated;
      return true;
    } else {
      return false;  // not booted yet.
    }
  }
  info = pending_fsmap.get_info_gid(gid);

  // old seq?
  if (info.state_seq > seq) {
    dout(7) << "mds_beacon " << *m << " has old seq, ignoring" << dendl;
    goto ignore;
  }

  if (fsmap.get_epoch() != m->get_last_epoch_seen()) {
    dout(10) << "mds_beacon " << *m
	     << " ignoring requested state, because mds hasn't seen latest map" << dendl;
    goto reply;
  }

  if (info.laggy()) {
    _note_beacon(m);
    return false;  // no longer laggy, need to update map.
  }
  if (state == MDSMap::STATE_BOOT) {
    // ignore, already booted.
    goto ignore;
  }
  // is there a state change here?
  if (info.state != state) {
    // legal state change?
    if ((info.state == MDSMap::STATE_STANDBY ||
	 info.state == MDSMap::STATE_STANDBY_REPLAY ||
	 info.state == MDSMap::STATE_ONESHOT_REPLAY) && state > 0) {
      dout(10) << "mds_beacon mds can't activate itself (" << ceph_mds_state_name(info.state)
	       << " -> " << ceph_mds_state_name(state) << ")" << dendl;
      goto reply;
    }

    if ((state == MDSMap::STATE_STANDBY || state == MDSMap::STATE_STANDBY_REPLAY)
        && info.role.rank != MDS_RANK_NONE)
    {
      dout(4) << "mds_beacon MDS can't go back into standby after taking rank: "
                 "held rank " << info.role.rank << " while requesting state "
              << ceph_mds_state_name(state) << dendl;
      goto reply;
    }
    

    // FIXME: for standby-replay for rank we should be told know
    // by the MDS daemon which filesystem it wants.  i.e. this shouldn't
    // be standby-for-rank it should be standby-for-role
    // ALSO: don't let someone go into standby replay if cluster DOWN flag set
#if 0
    mds_namespace_t ns = MDS_NAMESPACE_ANONYMOUS;
    if (info.state == MDSMap::STATE_STANDBY &&
	(state == MDSMap::STATE_STANDBY_REPLAY ||
	    state == MDSMap::STATE_ONESHOT_REPLAY) &&
	(pending_fsmap.is_degraded() ||
	 ((m->get_standby_for_rank() >= 0) &&
	     pending_fsmap.get_state(m->get_standby_for_rank()) < MDSMap::STATE_ACTIVE))) {
      dout(10) << "mds_beacon can't standby-replay mds." << m->get_standby_for_rank() << " at this time (cluster degraded, or mds not active)" << dendl;
      dout(10) << "pending_fsmap.is_degraded()==" << pending_fsmap.is_degraded()
          << " rank state: " << ceph_mds_state_name(pending_fsmap.get_state(m->get_standby_for_rank())) << dendl;
      goto reply;
    }
#endif
    _note_beacon(m);
    return false;  // need to update map
  }

  // Comparing known daemon health with m->get_health()
  // and return false (i.e. require proposal) if they
  // do not match, to update our stored
  if (!(pending_daemon_health[gid] == m->get_health())) {
    dout(20) << __func__ << " health metrics for gid " << gid << " were updated" << dendl;
    return false;
  }

 reply:
  // note time and reply
  _note_beacon(m);
  mon->send_reply(op,
		  new MMDSBeacon(mon->monmap->fsid, m->get_global_id(), m->get_name(),
				 fsmap.get_epoch(), state, seq));
  return true;

 ignore:
  // I won't reply this beacon, drop it.
  mon->no_reply(op);
  return true;
}

bool MDSMonitor::preprocess_offload_targets(MonOpRequestRef op)
{
  op->mark_mdsmon_event(__func__);
  MMDSLoadTargets *m = static_cast<MMDSLoadTargets*>(op->get_req());
  dout(10) << "preprocess_offload_targets " << *m << " from " << m->get_orig_source() << dendl;
  mds_gid_t gid;
  
  // check privileges, ignore message if fails
  MonSession *session = m->get_session();
  if (!session)
    goto done;
  if (!session->is_capable("mds", MON_CAP_X)) {
    dout(0) << "preprocess_offload_targets got MMDSLoadTargets from entity with insufficient caps "
	    << session->caps << dendl;
    goto done;
  }

  gid = m->global_id;
  if (fsmap.mds_info.count(gid) &&
      m->targets == fsmap.mds_info[gid].export_targets)
    goto done;

  return false;

 done:
  return true;
}


bool MDSMonitor::prepare_update(MonOpRequestRef op)
{
  op->mark_mdsmon_event(__func__);
  PaxosServiceMessage *m = static_cast<PaxosServiceMessage*>(op->get_req());
  dout(7) << "prepare_update " << *m << dendl;

  switch (m->get_type()) {
    
  case MSG_MDS_BEACON:
    return prepare_beacon(op);

  case MSG_MON_COMMAND:
    return prepare_command(op);

  case MSG_MDS_OFFLOAD_TARGETS:
    return prepare_offload_targets(op);
  
  default:
    assert(0);
  }

  return true;
}



bool MDSMonitor::prepare_beacon(MonOpRequestRef op)
{
  op->mark_mdsmon_event(__func__);
  MMDSBeacon *m = static_cast<MMDSBeacon*>(op->get_req());
  // -- this is an update --
  dout(12) << "prepare_beacon " << *m << " from " << m->get_orig_source_inst() << dendl;
  entity_addr_t addr = m->get_orig_source_inst().addr;
  mds_gid_t gid = m->get_global_id();
  MDSMap::DaemonState state = m->get_state();
  version_t seq = m->get_seq();

  // TODO for older MMDSBeacons, where the client won't
  // be able to handle an FSMap with no filesystems in
  // it: drop this message if there is no filesystem
  // configured as legacy_client_namespace

  // Store health
  dout(20) << __func__ << " got health from gid " << gid << " with " << m->get_health().metrics.size() << " metrics." << dendl;
  pending_daemon_health[gid] = m->get_health();

  // boot?
  if (state == MDSMap::STATE_BOOT) {
    // zap previous instance of this name?
    if (g_conf->mds_enforce_unique_name) {
      bool failed_mds = false;
      while (mds_gid_t existing = pending_fsmap.find_mds_gid_by_name(m->get_name())) {
        if (!mon->osdmon()->is_writeable()) {
          mon->osdmon()->wait_for_writeable(op, new C_RetryMessage(this, op));
          return false;
        }
	fail_mds_gid(existing);
        failed_mds = true;
      }
      if (failed_mds) {
        assert(mon->osdmon()->is_writeable());
        request_proposal(mon->osdmon());
      }
    }

    // add
    FSMap::mds_info_t& info = pending_fsmap.mds_info[gid];
    info.global_id = gid;
    info.name = m->get_name();
    info.addr = addr;
    info.state = MDSMap::STATE_STANDBY;
    info.state_seq = seq;
    info.standby_for_rank = m->get_standby_for_rank();
    info.standby_for_name = m->get_standby_for_name();

    // FIXME: reinstate standby_for_rank
#if 0
    if (!info.standby_for_name.empty()) {
      const FSMap::mds_info_t *leaderinfo = fsmap.find_by_name(info.standby_for_name);
      if (leaderinfo && (leaderinfo->role.rank >= 0)) {
        info.standby_for_rank =
            fsmap.find_by_name(info.standby_for_name)->rank;
        if (fsmap.is_followable(info.standby_for_rank)) {
          info.state = MDSMap::STATE_STANDBY_REPLAY;
        }
      }
    }
#endif

    // initialize the beacon timer
    last_beacon[gid].stamp = ceph_clock_now(g_ceph_context);
    last_beacon[gid].seq = seq;

    // new incompat?
    if (!pending_fsmap.compat.writeable(m->get_compat())) {
      dout(10) << " fsmap " << pending_fsmap.compat << " can't write to new mds' " << m->get_compat()
	       << ", updating fsmap and killing old mds's"
	       << dendl;
      pending_fsmap.compat = m->get_compat();
    }

    update_metadata(m->get_global_id(), m->get_sys_info());
  } else {
    // state change
    FSMap::mds_info_t& info = pending_fsmap.get_info_gid(gid);

    if (info.state == MDSMap::STATE_STOPPING && state != MDSMap::STATE_STOPPED ) {
      // we can't transition to any other states from STOPPING
      dout(0) << "got beacon for MDS in STATE_STOPPING, ignoring requested state change"
	       << dendl;
      _note_beacon(m);
      return true;
    }

    if (info.laggy()) {
      dout(10) << "prepare_beacon clearing laggy flag on " << addr << dendl;
      info.clear_laggy();
    }
  
    dout(10) << "prepare_beacon mds." << info.role
	     << " " << ceph_mds_state_name(info.state)
	     << " -> " << ceph_mds_state_name(state)
	     << "  standby_for_rank=" << m->get_standby_for_rank()
	     << dendl;
    if (state == MDSMap::STATE_STOPPED) {
      auto fs = pending_fsmap.filesystems.at(info.role.ns);
      fs->up.erase(info.role.rank);
      fs->in.erase(info.role.rank);
      fs->stopped.insert(info.role.rank);
      pending_fsmap.mds_info.erase(gid);  // last! info is a ref into this map
      last_beacon.erase(gid);
    } else if (state == MDSMap::STATE_STANDBY_REPLAY) {
      // FIXME reinstate standby replay
#if 0
      if (m->get_standby_for_rank() == FSMap::MDS_STANDBY_NAME) {
        /* convert name to rank. If we don't have it, do nothing. The
	 mds will stay in standby and keep requesting the state change */
        dout(20) << "looking for mds " << m->get_standby_for_name()
                  << " to STANDBY_REPLAY for" << dendl;
        const FSMap::mds_info_t *found_mds = NULL;
        if ((found_mds = fsmap.find_by_name(m->get_standby_for_name())) &&
            (found_mds->rank >= 0) &&
	    fsmap.is_followable(found_mds->rank)) {
          info.standby_for_rank = found_mds->rank;
          dout(10) <<" found mds " << m->get_standby_for_name()
                       << "; it has rank " << info.standby_for_rank << dendl;
          info.state = MDSMap::STATE_STANDBY_REPLAY;
          info.state_seq = seq;
        } else {
          return false;
        }
      } else if (m->get_standby_for_rank() >= 0 &&
		 fsmap.is_followable(m->get_standby_for_rank())) {
        /* switch to standby-replay for this MDS*/
        info.state = MDSMap::STATE_STANDBY_REPLAY;
        info.state_seq = seq;
        info.standby_for_rank = m->get_standby_for_rank();
      } else { //it's a standby for anybody, and is already in the list
        assert(pending_fsmap.get_mds_info().count(info.global_id));
        return false;
      }
#endif
    } else if (state == MDSMap::STATE_DAMAGED) {
      if (!mon->osdmon()->is_writeable()) {
        dout(4) << __func__ << ": DAMAGED from rank " << info.role
                << " waiting for osdmon writeable to blacklist it" << dendl;
        mon->osdmon()->wait_for_writeable(op, new C_RetryMessage(this, op));
        return false;
      }

      // Record this MDS rank as damaged, so that other daemons
      // won't try to run it.
      dout(4) << __func__ << ": marking rank "
              << info.role << " damaged" << dendl;

      // Blacklist this MDS daemon
      auto fs = pending_fsmap.filesystems.at(info.role.ns);
      const utime_t until = ceph_clock_now(g_ceph_context);
      fs->last_failure_osd_epoch = mon->osdmon()->blacklist(
          info.addr, until);
      request_proposal(mon->osdmon());

      // Clear out daemon state and add rank to damaged list
      fs->up.erase(info.role.rank);
      fs->damaged.insert(info.role.rank);
      last_beacon.erase(gid);

      // Call erase() last because the `info` reference becomes invalid
      // after we remove the instance from the map.
      pending_fsmap.mds_info.erase(gid);

      // Respond to MDS, so that it knows it can continue to shut down
      mon->send_reply(op, new MMDSBeacon(mon->monmap->fsid, m->get_global_id(),
                    m->get_name(), fsmap.get_epoch(), state, seq));
    } else if (state == MDSMap::STATE_DNE) {
      if (!mon->osdmon()->is_writeable()) {
        dout(4) << __func__ << ": DNE from rank " << info.role
                << " waiting for osdmon writeable to blacklist it" << dendl;
        mon->osdmon()->wait_for_writeable(op, new C_RetryMessage(this, op));
        return false;
      }

      fail_mds_gid(gid);
      assert(mon->osdmon()->is_writeable());
      request_proposal(mon->osdmon());

      // Respond to MDS, so that it knows it can continue to shut down
      mon->send_reply(op, new MMDSBeacon(mon->monmap->fsid, m->get_global_id(),
                    m->get_name(), fsmap.get_epoch(), state, seq));
    } else {
      info.state = state;
      info.state_seq = seq;
    }
  }

  dout(7) << "prepare_beacon pending map now:" << dendl;
  print_map(pending_fsmap);
  
  wait_for_finished_proposal(op, new C_Updated(this, op));

  return true;
}

bool MDSMonitor::prepare_offload_targets(MonOpRequestRef op)
{
  op->mark_mdsmon_event(__func__);
  MMDSLoadTargets *m = static_cast<MMDSLoadTargets*>(op->get_req());
  mds_gid_t gid = m->global_id;
  if (pending_fsmap.mds_info.count(gid)) {
    dout(10) << "prepare_offload_targets " << gid << " " << m->targets << dendl;
    pending_fsmap.mds_info[gid].export_targets = m->targets;
  } else {
    dout(10) << "prepare_offload_targets " << gid << " not in map" << dendl;
  }
  return true;
}

bool MDSMonitor::should_propose(double& delay)
{
  // delegate to PaxosService to assess whether we should propose
  return PaxosService::should_propose(delay);
}

void MDSMonitor::_updated(MonOpRequestRef op)
{
  op->mark_mdsmon_event(__func__);
  MMDSBeacon *m = static_cast<MMDSBeacon*>(op->get_req());
  dout(10) << "_updated " << m->get_orig_source() << " " << *m << dendl;
  mon->clog->info() << m->get_orig_source_inst() << " "
	  << ceph_mds_state_name(m->get_state()) << "\n";

  if (m->get_state() == MDSMap::STATE_STOPPED) {
    // send the map manually (they're out of the map, so they won't get it automatic)
    MDSMap *generated = generate_mds_map(MDS_NAMESPACE_NONE);
    mon->send_reply(op, new MMDSMap(mon->monmap->fsid, generated));
    delete generated;
  } else {
    mon->send_reply(op, new MMDSBeacon(mon->monmap->fsid,
				      m->get_global_id(),
				      m->get_name(),
				      fsmap.get_epoch(),
				      m->get_state(),
				      m->get_seq()));
  }
}

void MDSMonitor::on_active()
{
  tick();
  update_logger();

  if (mon->is_leader())
    mon->clog->info() << "fsmap " << fsmap << "\n";
}

void MDSMonitor::get_health(list<pair<health_status_t, string> >& summary,
			    list<pair<health_status_t, string> > *detail) const
{
  fsmap.get_health(summary, detail);

  // For each MDS GID...
  for (std::map<mds_gid_t, FSMap::mds_info_t>::const_iterator i = fsmap.mds_info.begin();
      i != fsmap.mds_info.end(); ++i) {
    // Decode MDSHealth
    bufferlist bl;
    mon->store->get(MDS_HEALTH_PREFIX, stringify(i->first), bl);
    if (!bl.length()) {
      derr << "Missing health data for MDS " << i->first << dendl;
      continue;
    }
    MDSHealth health;
    bufferlist::iterator bl_i = bl.begin();
    health.decode(bl_i);

    for (std::list<MDSHealthMetric>::iterator j = health.metrics.begin(); j != health.metrics.end(); ++j) {
      int const rank = i->second.role.rank;
      std::ostringstream message;
      message << "mds" << rank << ": " << j->message;
      summary.push_back(std::make_pair(j->sev, message.str()));

      if (detail) {
        // There is no way for us to clealy associate detail entries with summary entries (#7192), so
        // we duplicate the summary message in the detail string and tag the metadata on.
        std::ostringstream detail_message;
        detail_message << message.str();
        if (j->metadata.size()) {
          detail_message << "(";
          std::map<std::string, std::string>::iterator k = j->metadata.begin();
          while (k != j->metadata.end()) {
            detail_message << k->first << ": " << k->second;
            if (boost::next(k) != j->metadata.end()) {
              detail_message << ", ";
            }
            ++k;
          }
          detail_message << ")";
        }
        detail->push_back(std::make_pair(j->sev, detail_message.str()));
      }
    }
  }
}

void MDSMonitor::dump_info(Formatter *f)
{
  f->open_object_section("fsmap");
  fsmap.dump(f);
  f->close_section();

  f->dump_unsigned("mdsmap_first_committed", get_first_committed());
  f->dump_unsigned("mdsmap_last_committed", get_last_committed());
}

bool MDSMonitor::preprocess_command(MonOpRequestRef op)
{
  op->mark_mdsmon_event(__func__);
  MMonCommand *m = static_cast<MMonCommand*>(op->get_req());
  int r = -1;
  bufferlist rdata;
  stringstream ss, ds;

  map<string, cmd_vartype> cmdmap;
  if (!cmdmap_from_json(m->cmd, &cmdmap, ss)) {
    // ss has reason for failure
    string rs = ss.str();
    mon->reply_command(op, -EINVAL, rs, rdata, get_last_committed());
    return true;
  }

  string prefix;
  cmd_getval(g_ceph_context, cmdmap, "prefix", prefix);
  string format;
  cmd_getval(g_ceph_context, cmdmap, "format", format, string("plain"));
  boost::scoped_ptr<Formatter> f(Formatter::create(format));

  MonSession *session = m->get_session();
  if (!session) {
    mon->reply_command(op, -EACCES, "access denied", rdata, get_last_committed());
    return true;
  }

  if (prefix == "mds stat") {
    if (f) {
      f->open_object_section("mds_stat");
      dump_info(f.get());
      f->close_section();
      f->flush(ds);
    } else {
      ds << fsmap;
    }
    r = 0;
  } else if (prefix == "mds dump") {
    int64_t epocharg;
    epoch_t epoch;

    FSMap *p = &fsmap;
    if (cmd_getval(g_ceph_context, cmdmap, "epoch", epocharg)) {
      epoch = epocharg;
      bufferlist b;
      int err = get_version(epoch, b);
      if (err == -ENOENT) {
	p = 0;
	r = -ENOENT;
      } else {
	assert(err == 0);
	assert(b.length());
	p = new FSMap;
	p->decode(b);
      }
    }
    if (p) {
      stringstream ds;
      MDSMap *mdsmap = generate_mds_map(fsmap.legacy_client_namespace);
      if (f != NULL) {
	f->open_object_section("mdsmap");
	mdsmap->dump(f.get());
	f->close_section();
	f->flush(ds);
	r = 0;
      } else {
	mdsmap->print(ds);
	r = 0;
      } 
      delete mdsmap;
      if (r == 0) {
	rdata.append(ds);
	ss << "dumped fsmap epoch " << p->get_epoch();
      }
      if (p != &fsmap)
	delete p;
    }
  } else if (prefix == "fs dump") {
    int64_t epocharg;
    epoch_t epoch;

    FSMap *p = &fsmap;
    if (cmd_getval(g_ceph_context, cmdmap, "epoch", epocharg)) {
      epoch = epocharg;
      bufferlist b;
      int err = get_version(epoch, b);
      if (err == -ENOENT) {
	p = 0;
	r = -ENOENT;
      } else {
	assert(err == 0);
	assert(b.length());
	p = new FSMap;
	p->decode(b);
      }
    }
    if (p) {
      stringstream ds;
      if (f != NULL) {
	f->open_object_section("fsmap");
	p->dump(f.get());
	f->close_section();
	f->flush(ds);
	r = 0;
      } else {
	p->print(ds);
	r = 0;
      } 
      if (r == 0) {
	rdata.append(ds);
	ss << "dumped fsmap epoch " << p->get_epoch();
      }
      if (p != &fsmap)
	delete p;
    }
  } else if (prefix == "mds metadata") {
    string who;
    cmd_getval(g_ceph_context, cmdmap, "who", who);
    if (!f)
      f.reset(Formatter::create("json-pretty"));
    f->open_object_section("mds_metadata");
    r = dump_metadata(who, f.get(), ss);
    f->close_section();
    f->flush(ds);
  } else if (prefix == "mds getmap") {
    epoch_t e;
    int64_t epocharg;
    bufferlist b;
    if (cmd_getval(g_ceph_context, cmdmap, "epoch", epocharg)) {
      e = epocharg;
      int err = get_version(e, b);
      if (err == -ENOENT) {
	r = -ENOENT;
      } else {
	assert(err == 0);
	assert(b.length());
	FSMap mm;
	mm.decode(b);
	mm.encode(rdata, m->get_connection()->get_features());
	ss << "got fsmap epoch " << mm.get_epoch();
	r = 0;
      }
    } else {
      fsmap.encode(rdata, m->get_connection()->get_features());
      ss << "got fsmap epoch " << fsmap.get_epoch();
      r = 0;
    }
  } else if (prefix == "mds tell") {
    string whostr;
    cmd_getval(g_ceph_context, cmdmap, "who", whostr);
    vector<string>args_vec;
    cmd_getval(g_ceph_context, cmdmap, "args", args_vec);

    if (whostr == "*") {
      r = -ENOENT;
      const map<mds_gid_t, FSMap::mds_info_t> mds_info = fsmap.get_mds_info();
      for (map<mds_gid_t, FSMap::mds_info_t>::const_iterator i = mds_info.begin();
	   i != mds_info.end();
	   ++i) {
	m->cmd = args_vec;
	mon->send_command(i->second.get_inst(), m->cmd);
	r = 0;
      }
      if (r == -ENOENT) {
	ss << "no mds active";
      } else {
	ss << "ok";
      }
    } else {
      if (fsmap.legacy_client_namespace) {
        auto fs = fsmap.filesystems.at(fsmap.legacy_client_namespace);
        errno = 0;
        long who_l = strtol(whostr.c_str(), 0, 10);
        if (!errno && who_l >= 0) {
          mds_rank_t who = mds_rank_t(who_l);
          if (fs->is_up(who)) {
            m->cmd = args_vec;
            mon->send_command(fsmap.get_inst(mds_role_t(fs->ns, who)), m->cmd);
            r = 0;
            ss << "ok";
          } else {
            ss << "mds." << who << " not up";
            r = -ENOENT;
          }
        } else {
          ss << "specify mds number or *";
        }
      } else {
        ss << "no legacy filesystem set";
      }
    }
  } else if (prefix == "mds compat show") {
      if (f) {
	f->open_object_section("mds_compat");
	fsmap.compat.dump(f.get());
	f->close_section();
	f->flush(ds);
      } else {
	ds << fsmap.compat;
      }
      r = 0;
  } else if (prefix == "fs ls") {
    if (f) {
      f->open_array_section("filesystems");
      {
        for (const auto i : fsmap.filesystems) {
          const auto fs = i.second;
          f->open_object_section("filesystem");
          {
            f->dump_string("name", fs->fs_name);
            const string &md_pool_name = mon->osdmon()->osdmap.get_pool_name(fs->metadata_pool);
            /* Output both the names and IDs of pools, for use by
             * humans and machines respectively */
            f->dump_string("metadata_pool", md_pool_name);
            f->dump_int("metadata_pool_id", fs->metadata_pool);
            f->open_array_section("data_pool_ids");
            {
              for (auto dpi = fs->data_pools.begin();
                   dpi != fs->data_pools.end(); ++dpi) {
                f->dump_int("data_pool_id", *dpi);
              }
            }
            f->close_section();

            f->open_array_section("data_pools");
            {
                for (auto dpi = fs->data_pools.begin();
                   dpi != fs->data_pools.end(); ++dpi) {
                  const string &pool_name = mon->osdmon()->osdmap.get_pool_name(*dpi);
                  f->dump_string("data_pool", pool_name);
                }
            }

            f->close_section();
          }
          f->close_section();
        }
      }
      f->close_section();
      f->flush(ds);
    } else {
      for (const auto i : fsmap.filesystems) {
        const auto fs = i.second;
        const string &md_pool_name = mon->osdmon()->osdmap.get_pool_name(fs->metadata_pool);
        
        ds << "name: " << fs->fs_name << ", metadata pool: " << md_pool_name << ", data pools: [";
        for (std::set<int64_t>::iterator dpi = fs->data_pools.begin();
           dpi != fs->data_pools.end(); ++dpi) {
          const string &pool_name = mon->osdmon()->osdmap.get_pool_name(*dpi);
          ds << pool_name << " ";
        }
        ds << "]" << std::endl;
      }

      if (fsmap.filesystems.empty()) {
        ds << "No filesystems enabled" << std::endl;
      }
    }
    r = 0;
  }

  if (r != -1) {
    rdata.append(ds);
    string rs;
    getline(ss, rs);
    mon->reply_command(op, r, rs, rdata, get_last_committed());
    return true;
  } else
    return false;
}

void MDSMonitor::fail_mds_gid(mds_gid_t gid)
{
  assert(pending_fsmap.mds_info.count(gid));
  FSMap::mds_info_t& info = pending_fsmap.mds_info[gid];
  dout(10) << "fail_mds_gid " << gid << " mds." << info.name << " role " << info.role << dendl;

  utime_t until = ceph_clock_now(g_ceph_context);
  until += g_conf->mds_blacklist_interval;

  if (info.role.rank >= 0) {
    auto fs = pending_fsmap.filesystems.at(info.role.ns);
    fs->last_failure_osd_epoch = mon->osdmon()->blacklist(info.addr, until);
    if (info.state == MDSMap::STATE_CREATING) {
      // If this gid didn't make it past CREATING, then forget
      // the rank ever existed so that next time it's handed out
      // to a gid it'll go back into CREATING.
      fs->in.erase(info.role.rank);
    } else {
      // Put this rank into the failed list so that the next available STANDBY will
      // pick it up.
      fs->failed.insert(info.role.rank);
    }
    fs->up.erase(info.role.rank);
  }

  pending_fsmap.mds_info.erase(gid);

  last_beacon.erase(gid);
}

// TODO: additionally support some a syntax for specifying filesystem
// name plus rank like myfs:2
mds_gid_t MDSMonitor::gid_from_arg(const std::string& arg, std::ostream &ss)
{
  std::string err;
  unsigned long long rank_or_gid = strict_strtoll(arg.c_str(), 10, &err);
  if (!err.empty()) {
    // Try to interpret the arg as an MDS name
    const FSMap::mds_info_t *mds_info = fsmap.find_by_name(arg);
    if (!mds_info) {
      ss << "MDS named '" << arg
	 << "' does not exist, or is not up";
      return MDS_GID_NONE;
    }
    if (mds_info->role.rank >= 0) {
      dout(10) << __func__ << ": resolved MDS name '" << arg << "' to rank " << rank_or_gid << dendl;
      rank_or_gid = (unsigned long long)(mds_info->role.rank);
    } else {
      dout(10) << __func__ << ": resolved MDS name '" << arg << "' to GID " << rank_or_gid << dendl;
      rank_or_gid = mds_info->global_id;
    }
  } else {
    dout(10) << __func__ << ": treating MDS reference '" << arg
	     << "' as an integer " << rank_or_gid << dendl;
  }

  if (mon->is_leader()) {
    for (const auto &i : pending_fsmap.filesystems)
    {
      auto fs = i.second;
      if (fs->up.count(mds_rank_t(rank_or_gid))) {
        dout(10) << __func__ << ": validated rank/GID " << rank_or_gid
                 << " as a rank" << dendl;
        mds_gid_t gid = fs->up[mds_rank_t(rank_or_gid)];
        if (pending_fsmap.mds_info.count(gid)) {
          return gid;
        } else {
          dout(10) << __func__ << ": GID " << rank_or_gid << " was removed." << dendl;
          return MDS_GID_NONE;
        }
      }
    }

    if (pending_fsmap.mds_info.count(mds_gid_t(rank_or_gid))) {
      dout(10) << __func__ << ": validated rank/GID " << rank_or_gid
	       << " as a GID" << dendl;
      return mds_gid_t(rank_or_gid);
    }
  } else {
    // mon is a peon
    for (const auto &i : pending_fsmap.filesystems)
    {
      auto fs = i.second;
      if (fs->up.count(mds_rank_t(rank_or_gid))) {
        return fs->up[mds_rank_t(rank_or_gid)];
      }
    }
    if (fsmap.get_state_gid(mds_gid_t(rank_or_gid))) {
      return mds_gid_t(rank_or_gid);
    }
  }

  dout(1) << __func__ << ": rank/GID " << rank_or_gid
	  << " not a existent rank or GID" << dendl;
  return MDS_GID_NONE;
}

int MDSMonitor::fail_mds(std::ostream &ss, const std::string &arg)
{
  mds_gid_t gid = gid_from_arg(arg, ss);
  if (gid == MDS_GID_NONE) {
    return 0;
  }
  if (!mon->osdmon()->is_writeable()) {
    return -EAGAIN;
  }
  fail_mds_gid(gid);
  ss << "failed mds gid " << gid;
  assert(mon->osdmon()->is_writeable());
  request_proposal(mon->osdmon());
  return 0;
}

bool MDSMonitor::prepare_command(MonOpRequestRef op)
{
  op->mark_mdsmon_event(__func__);
  MMonCommand *m = static_cast<MMonCommand*>(op->get_req());
  int r = -EINVAL;
  stringstream ss;
  bufferlist rdata;

  map<string, cmd_vartype> cmdmap;
  if (!cmdmap_from_json(m->cmd, &cmdmap, ss)) {
    string rs = ss.str();
    mon->reply_command(op, -EINVAL, rs, rdata, get_last_committed());
    return true;
  }

  string prefix;
  cmd_getval(g_ceph_context, cmdmap, "prefix", prefix);

  /* Refuse access if message not associated with a valid session */
  MonSession *session = m->get_session();
  if (!session) {
    mon->reply_command(op, -EACCES, "access denied", rdata, get_last_committed());
    return true;
  }

  /* Execute filesystem add/remove, or pass through to filesystem_command */
  r = management_command(op, prefix, cmdmap, ss);
  if (r >= 0)
    goto out;
  
  if (r == -EAGAIN) {
    // message has been enqueued for retry; return.
    dout(4) << __func__ << " enqueue for retry by management_command" << dendl;
    return false;
  } else if (r != -ENOSYS) {
    // MDSMonitor::management_command() returns -ENOSYS if it knows nothing
    // about the command passed to it, in which case we will check whether
    // MDSMonitor::filesystem_command() knows about it.  If on the other hand
    // the error code is different from -ENOSYS, we will treat it as is and
    // behave accordingly.
    goto out;
  }

  r = filesystem_command(op, prefix, cmdmap, ss);
  if (r >= 0) {
    goto out;
  } else if (r == -EAGAIN) {
    // Do not reply, the message has been enqueued for retry
    dout(4) << __func__ << " enqueue for retry by filesystem_command" << dendl;
    return false;
  } else if (r != -ENOSYS) {
    goto out;
  }

  // Only handle legacy commands if there is a filesystem configured
  if (pending_fsmap.legacy_client_namespace == MDS_NAMESPACE_NONE) {
    if (pending_fsmap.filesystems.size() == 0) {
      ss << "No filesystem configured: use `ceph fs new` to create a filesystem";
    } else {
      ss << "No filesystem set for use with legacy commands";
    }
    r = -EINVAL;
    goto out;
  }

  r = legacy_filesystem_command(op, prefix, cmdmap, ss);

  if (r == -ENOSYS && ss.str().empty()) {
    ss << "unrecognized command";
  }

out:
  dout(4) << __func__ << " done, r=" << r << dendl;
  /* Compose response */
  string rs;
  getline(ss, rs);

  if (r >= 0) {
    // success.. delay reply
    wait_for_finished_proposal(op, new Monitor::C_Command(mon, op, r, rs,
					      get_last_committed() + 1));
    return true;
  } else {
    // reply immediately
    mon->reply_command(op, r, rs, rdata, get_last_committed());
    return false;
  }
}


/**
 * Return 0 if the pool is suitable for use with CephFS, or
 * in case of errors return a negative error code, and populate
 * the passed stringstream with an explanation.
 */
int MDSMonitor::_check_pool(
    const int64_t pool_id,
    std::stringstream *ss) const
{
  assert(ss != NULL);

  const pg_pool_t *pool = mon->osdmon()->osdmap.get_pg_pool(pool_id);
  if (!pool) {
    *ss << "pool id '" << pool_id << "' does not exist";
    return -ENOENT;
  }

  const string& pool_name = mon->osdmon()->osdmap.get_pool_name(pool_id);

  if (pool->is_erasure()) {
    // EC pools are only acceptable with a cache tier overlay
    if (!pool->has_tiers() || !pool->has_read_tier() || !pool->has_write_tier()) {
      *ss << "pool '" << pool_name << "' (id '" << pool_id << "')"
         << " is an erasure-code pool";
      return -EINVAL;
    }

    // That cache tier overlay must be writeback, not readonly (it's the
    // write operations like modify+truncate we care about support for)
    const pg_pool_t *write_tier = mon->osdmon()->osdmap.get_pg_pool(
        pool->write_tier);
    assert(write_tier != NULL);  // OSDMonitor shouldn't allow DNE tier
    if (write_tier->cache_mode == pg_pool_t::CACHEMODE_FORWARD
        || write_tier->cache_mode == pg_pool_t::CACHEMODE_READONLY) {
      *ss << "EC pool '" << pool_name << "' has a write tier ("
          << mon->osdmon()->osdmap.get_pool_name(pool->write_tier)
          << ") that is configured "
             "to forward writes.  Use a cache mode such as 'writeback' for "
             "CephFS";
      return -EINVAL;
    }
  }

  if (pool->is_tier()) {
    *ss << " pool '" << pool_name << "' (id '" << pool_id
      << "') is already in use as a cache tier.";
    return -EINVAL;
  }

  // Nothing special about this pool, so it is permissible
  return 0;
}


/**
 * Handle a command for creating or removing a filesystem.
 *
 * @retval 0        Command was successfully handled and has side effects
 * @retval -EAGAIN  Message has been queued for retry
 * @retval -ENOSYS  Unknown command
 * @retval < 0      An error has occurred; **ss** may have been set.
 */
int MDSMonitor::management_command(
    MonOpRequestRef op,
    std::string const &prefix,
    map<string, cmd_vartype> &cmdmap,
    std::stringstream &ss)
{
  op->mark_mdsmon_event(__func__);
  if (prefix == "mds newfs") {
    // FIXME: reinstate (or not?) legacy newfs in a way
    // that no longer wipes the FSMap entirely
    return 0;
#if 0
    /* Legacy `newfs` command, takes pool numbers instead of
     * names, assumes fs name to be MDS_FS_NAME_DEFAULT, and
     * can overwrite existing filesystem settings */
    FSMap newmap;
    int64_t metadata, data;

    if (!cmd_getval(g_ceph_context, cmdmap, "metadata", metadata)) {
      ss << "error parsing 'metadata' value '"
         << cmd_vartype_stringify(cmdmap["metadata"]) << "'";
      return -EINVAL;
    }
    if (!cmd_getval(g_ceph_context, cmdmap, "data", data)) {
      ss << "error parsing 'data' value '"
         << cmd_vartype_stringify(cmdmap["data"]) << "'";
      return -EINVAL;
    }
 
    int r = _check_pool(data, &ss);
    if (r < 0) {
      return r;
    }

    r = _check_pool(metadata, &ss);
    if (r < 0) {
      return r;
    }

    // be idempotent.. success if it already exists and matches
    if (fsmap.get_filesystem(MDS_FS_NAME_DEFAULT) != nullptr) {
      auto fs = fsmap.get_filesystem(MDS_FS_NAME_DEFAULT);
      if (fs->get_metadata_pool() == metadata &&
          fs->get_first_data_pool() == data) {
        ss << "filesystem '" << MDS_FS_NAME_DEFAULT << "' already exists";
        return 0;
      }
    }

    string sure;
    cmd_getval(g_ceph_context, cmdmap, "sure", sure);
    if (pending_fsmap.get_enabled() && sure != "--yes-i-really-mean-it") {
      ss << "this is DANGEROUS and will wipe out the fsmap's fs, and may clobber data in the new pools you specify.  add --yes-i-really-mean-it if you do.";
      return -EPERM;
    } else {
      newmap.inc = pending_fsmap.inc;
      pending_fsmap = newmap;
      pending_fsmap.epoch = fsmap.epoch + 1;
      create_new_fs(pending_fsmap, MDS_FS_NAME_DEFAULT, metadata, data);
      ss << "new fs with metadata pool " << metadata << " and data pool " << data;
      return 0;
    }
#endif
  } else if (prefix == "fs new") {
    string metadata_name;
    cmd_getval(g_ceph_context, cmdmap, "metadata", metadata_name);
    int64_t metadata = mon->osdmon()->osdmap.lookup_pg_pool_name(metadata_name);
    if (metadata < 0) {
      ss << "pool '" << metadata_name << "' does not exist";
      return -ENOENT;
    }

    string data_name;
    cmd_getval(g_ceph_context, cmdmap, "data", data_name);
    int64_t data = mon->osdmon()->osdmap.lookup_pg_pool_name(data_name);
    if (data < 0) {
      ss << "pool '" << data_name << "' does not exist";
      return -ENOENT;
    }
   
    string fs_name;
    cmd_getval(g_ceph_context, cmdmap, "fs_name", fs_name);
    if (fs_name.empty()) {
        // Ensure fs name is not empty so that we can implement
        // commmands that refer to FS by name in future.
        ss << "Filesystem name may not be empty";
        return -EINVAL;
    }

    if (pending_fsmap.get_filesystem(fs_name)) {
      auto fs = pending_fsmap.get_filesystem(fs_name);
      if (*(fs->data_pools.begin()) == data
          && fs->metadata_pool == metadata) {
        // Identical FS created already, this is a no-op
        ss << "filesystem '" << fs_name << "' already exists";
        return 0;
      } else {
        ss << "filesystem already exists with name '" << fs_name << "'";
        return -EINVAL;
      }
    }

    pg_pool_t const *data_pool = mon->osdmon()->osdmap.get_pg_pool(data);
    assert(data_pool != NULL);  // Checked it existed above
    pg_pool_t const *metadata_pool = mon->osdmon()->osdmap.get_pg_pool(metadata);
    assert(metadata_pool != NULL);  // Checked it existed above

    // we must make these checks before we even allow ourselves to *think*
    // about requesting a proposal to the osdmonitor and bail out now if
    // we believe we must.  bailing out *after* we request the proposal is
    // bad business as we could have changed the osdmon's state and ending up
    // returning an error to the user.
    int r = _check_pool(data, &ss);
    if (r < 0) {
      return r;
    }

    r = _check_pool(metadata, &ss);
    if (r < 0) {
      return r;
    }

    // Automatically set crash_replay_interval on data pool if it
    // isn't already set.
    if (data_pool->get_crash_replay_interval() == 0) {
      // We will be changing osdmon's state and requesting the osdmon to
      // propose.  We thus need to make sure the osdmon is writeable before
      // we do this, waiting if it's not.
      if (!mon->osdmon()->is_writeable()) {
        mon->osdmon()->wait_for_writeable(op, new C_RetryMessage(this, op));
        return -EAGAIN;
      }

      r = mon->osdmon()->set_crash_replay_interval(data, g_conf->osd_default_data_pool_replay_window);
      assert(r == 0);  // We just did get_pg_pool so it must exist and be settable
      request_proposal(mon->osdmon());
    }

    // All checks passed, go ahead and create.
    create_new_fs(pending_fsmap, fs_name, metadata, data);
    ss << "new fs with metadata pool " << metadata << " and data pool " << data;
    return 0;
  } else if (prefix == "fs rm") {
    // Check caller has correctly named the FS to delete
    // (redundant while there is only one FS, but command
    //  syntax should apply to multi-FS future)
    string fs_name;
    cmd_getval(g_ceph_context, cmdmap, "fs_name", fs_name);
    auto fs = pending_fsmap.get_filesystem(fs_name);
    if (fs == nullptr) {
        // Consider absence success to make deletes idempotent
        ss << "filesystem '" << fs_name << "' does not exist";
        return 0;
    }

    // Check that no MDS daemons are active
    if (!fs->up.empty()) {
      ss << "all MDS daemons must be inactive before removing filesystem";
      return -EINVAL;
    }

    // Check for confirmation flag
    string sure;
    cmd_getval(g_ceph_context, cmdmap, "sure", sure);
    if (sure != "--yes-i-really-mean-it") {
      ss << "this is a DESTRUCTIVE operation and will make data in your filesystem permanently" \
            "inaccessible.  Add --yes-i-really-mean-it if you are sure you wish to continue.";
      return -EPERM;
    }

    if (pending_fsmap.legacy_client_namespace == fs->ns) {
      pending_fsmap.legacy_client_namespace = MDS_NAMESPACE_NONE;
    }

    pending_fsmap.filesystems.erase(fs->ns);

    return 0;
  } else if (prefix == "fs reset") {
    string fs_name;
    cmd_getval(g_ceph_context, cmdmap, "fs_name", fs_name);
    auto fs = pending_fsmap.get_filesystem(fs_name);
    if (fs == nullptr) {
        ss << "filesystem '" << fs_name << "' does not exist";
        // Unlike fs rm, we consider this case an error
        return -ENOENT;
    }

    // Check that no MDS daemons are active
    if (!fs->up.empty()) {
      ss << "all MDS daemons must be inactive before resetting filesystem: set the cluster_down flag"
            " and use `ceph mds fail` to make this so";
      return -EINVAL;
    }

    // Check for confirmation flag
    string sure;
    cmd_getval(g_ceph_context, cmdmap, "sure", sure);
    if (sure != "--yes-i-really-mean-it") {
      ss << "this is a potentially destructive operation, only for use by experts in disaster recovery.  "
        "Add --yes-i-really-mean-it if you are sure you wish to continue.";
      return -EPERM;
    }

    FSMap newmap;

    auto new_fs = std::make_shared<Filesystem>();

    // Populate rank 0 as existing (so don't go into CREATING)
    // but failed (so that next available MDS is assigned the rank)
    new_fs->in.insert(mds_rank_t(0));
    new_fs->failed.insert(mds_rank_t(0));

    // Carry forward what makes sense
    new_fs->ns = fs->ns;
    new_fs->data_pools = fs->data_pools;
    new_fs->metadata_pool = fs->metadata_pool;
    new_fs->cas_pool = fs->cas_pool;
    new_fs->fs_name = fs->fs_name;
    new_fs->inc = fs->inc;
    new_fs->inline_data_enabled = fs->inline_data_enabled;

    // Persist the new FSMap
    pending_fsmap.filesystems[new_fs->ns] = new_fs;
    return 0;
  } else {
    return -ENOSYS;
  }
}


/**
 * Given one of the following forms:
 *   <fs name>:<rank>
 *   <fs id>:<rank>
 *   <rank>
 *
 * Parse into a mds_role_t.  The rank-only form is only valid
 * if legacy_client_ns is set.
 */
int MDSMonitor::parse_role(const std::string &role_str, mds_role_t *role)
{
  auto colon_pos = role_str.find(":");

  if (colon_pos != std::string::npos) {
    auto fs_part = role_str.substr(0, colon_pos);
    auto rank_part = role_str.substr(colon_pos);

    std::string err;
    mds_namespace_t fs_id = MDS_NAMESPACE_NONE;
    long fs_id_i = strict_strtol(fs_part.c_str(), 10, &err);
    if (fs_id_i < 0 || !err.empty()) {
      // Try resolving as name
      auto fs = pending_fsmap.get_filesystem(fs_part);
      if (fs == nullptr) {
        return -EINVAL;
      } else {
        fs_id = fs->ns;
      }
    } else {
      fs_id = fs_id_i;
    }

    mds_rank_t rank;
    long rank_i = strict_strtol(rank_part.c_str(), 10, &err);
    if (rank_i < 0 || !err.empty()) {
      return -EINVAL;
    } else {
      rank = rank_i;
    }

    *role = mds_role_t(fs_id, rank);
    return 0;
  } else {
    std::string err;
    long who_i = strict_strtol(role_str.c_str(), 10, &err);
    if (who_i < 0 || !err.empty()) {
      return -EINVAL;
    }

    if (pending_fsmap.legacy_client_namespace == MDS_NAMESPACE_NONE) {
      return -ENOENT;
    } else {
      *role = mds_role_t(pending_fsmap.legacy_client_namespace, who_i);
      return 0;
    }
  }
}

int MDSMonitor::filesystem_command(
    MonOpRequestRef op,
    std::string const &prefix,
    map<string, cmd_vartype> &cmdmap,
    std::stringstream &ss)
{
  dout(4) << __func__ << " prefix='" << prefix << "'" << dendl;
  op->mark_mdsmon_event(__func__);
  MMonCommand *m = static_cast<MMonCommand*>(op->get_req());
  int r = 0;
  string whostr;
  cmd_getval(g_ceph_context, cmdmap, "who", whostr);

  if (prefix == "mds stop" ||
      prefix == "mds deactivate") {

    mds_role_t role;
    r = parse_role(whostr, &role);
    if (r < 0 ) {
      return r;
    }
    auto fs = pending_fsmap.get_filesystem(role.ns);

    if (!pending_fsmap.is_active(role)) {
      r = -EEXIST;
      ss << "mds." << role << " not active (" 
	 << ceph_mds_state_name(pending_fsmap.get_state(role)) << ")";
    } else if (fs->get_root() == role.rank ||
		fs->get_tableserver() == role.rank) {
      r = -EINVAL;
      ss << "can't tell the root (" << fs->get_root()
	 << ") or tableserver (" << fs->get_tableserver()
	 << ") to deactivate";
    } else if (fs->get_num_in_mds() <= size_t(fs->get_max_mds())) {
      r = -EBUSY;
      ss << "must decrease max_mds or else MDS will immediately reactivate";
    } else {
      r = 0;
      mds_gid_t gid = fs->up.at(role.rank);
      ss << "telling mds." << role << " " << pending_fsmap.mds_info[gid].addr << " to deactivate";
      pending_fsmap.mds_info[gid].state = MDSMap::STATE_STOPPING;
    }
  } else if (prefix == "mds setmap") {
    FSMap map;
    map.decode(m->get_data());
    epoch_t e = 0;
    int64_t epochnum;
    if (cmd_getval(g_ceph_context, cmdmap, "epoch", epochnum))
      e = epochnum;

    if (pending_fsmap.epoch == e) {
      map.epoch = pending_fsmap.epoch;  // make sure epoch is correct
      pending_fsmap = map;
      ss << "set mds map";
    } else {
      ss << "next fsmap epoch " << pending_fsmap.epoch << " != " << e;
      return -EINVAL;
    }
  } else if (prefix == "mds set_state") {
    mds_gid_t gid;
    if (!cmd_getval(g_ceph_context, cmdmap, "gid", gid)) {
      ss << "error parsing 'gid' value '"
         << cmd_vartype_stringify(cmdmap["gid"]) << "'";
      return -EINVAL;
    }
    MDSMap::DaemonState state;
    if (!cmd_getval(g_ceph_context, cmdmap, "state", state)) {
      ss << "error parsing 'state' string value '"
         << cmd_vartype_stringify(cmdmap["state"]) << "'";
      return -EINVAL;
    }
    if (!pending_fsmap.is_dne_gid(gid)) {
      FSMap::mds_info_t& info = pending_fsmap.get_info_gid(gid);
      info.state = state;
      stringstream ss;
      ss << "set mds gid " << gid << " to state " << state << " " << ceph_mds_state_name(state);
      return 0;
    }
  } else if (prefix == "mds fail") {
    string who;
    cmd_getval(g_ceph_context, cmdmap, "who", who);
    r = fail_mds(ss, who);
    if (r < 0 && r == -EAGAIN) {
      mon->osdmon()->wait_for_writeable(op, new C_RetryMessage(this, op));
      return -EAGAIN; // don't propose yet; wait for message to be retried
    }
  } else if (prefix == "mds rm") {
    mds_gid_t gid;
    if (!cmd_getval(g_ceph_context, cmdmap, "gid", gid)) {
      ss << "error parsing 'gid' value '"
         << cmd_vartype_stringify(cmdmap["gid"]) << "'";
      return -EINVAL;
    }
    int state = pending_fsmap.get_state_gid(gid);
    if (state == 0) {
      ss << "mds gid " << gid << " dne";
      r = 0;
    } else if (state > 0) {
      ss << "cannot remove active mds." << pending_fsmap.get_info_gid(gid).name
	 << " rank " << pending_fsmap.get_info_gid(gid).role;
      return -EBUSY;
    } else {
      pending_fsmap.mds_info.erase(gid);
      stringstream ss;
      ss << "removed mds gid " << gid;
      return 0;
    }
  } else if (prefix == "mds rmfailed") {
    std::string role_str;
    cmd_getval(g_ceph_context, cmdmap, "who", role_str);
    mds_role_t role;
    int r = parse_role(role_str, &role);
    if (r < 0) {
      ss << "invalid role '" << role_str << "'";
      return -EINVAL;
    }

    std::shared_ptr<Filesystem> fs = pending_fsmap.get_filesystem(role.ns);
    if (fs == nullptr) {
      ss << "filesystem not found: " << role.ns;
      return -ENOENT;
    }

    fs->failed.erase(role.rank);
    stringstream ss;
    ss << "removed failed mds." << role;
    return 0;
  } else if (prefix == "mds compat rm_compat") {
    int64_t f;
    if (!cmd_getval(g_ceph_context, cmdmap, "feature", f)) {
      ss << "error parsing feature value '"
         << cmd_vartype_stringify(cmdmap["feature"]) << "'";
      return -EINVAL;
    }
    if (pending_fsmap.compat.compat.contains(f)) {
      ss << "removing compat feature " << f;
      pending_fsmap.compat.compat.remove(f);
    } else {
      ss << "compat feature " << f << " not present in " << pending_fsmap.compat;
    }
    r = 0;
  } else if (prefix == "mds compat rm_incompat") {
    int64_t f;
    if (!cmd_getval(g_ceph_context, cmdmap, "feature", f)) {
      ss << "error parsing feature value '"
         << cmd_vartype_stringify(cmdmap["feature"]) << "'";
      return -EINVAL;
    }
    if (pending_fsmap.compat.incompat.contains(f)) {
      ss << "removing incompat feature " << f;
      pending_fsmap.compat.incompat.remove(f);
    } else {
      ss << "incompat feature " << f << " not present in " << pending_fsmap.compat;
    }
    r = 0;
  } else if (prefix == "mds repaired") {
    std::string role_str;
    cmd_getval(g_ceph_context, cmdmap, "rank", role_str);
    mds_role_t role;
    r = parse_role(role_str, &role);
    if (r < 0) {
      ss << "invalid role " << role_str;
      return r;
    }
    auto fs = pending_fsmap.get_filesystem(role.ns);
    if (fs == nullptr) {
      ss << "filesystem not found " << role.ns;
      return -ENOENT;
    }

    if (fs->damaged.count(role.rank)) {
      dout(4) << "repaired: restoring rank " << role << dendl;
      fs->damaged.erase(role.rank);
      fs->failed.insert(role.rank);
    } else {
      dout(4) << "repaired: no-op on rank " << role << dendl;
    }
    r = 0;
  } else {
    return -ENOSYS;
  }

  return r;
}

/**
 * Handle a command that affects the filesystem (i.e. a filesystem
 * must exist for the command to act upon).
 *
 * @retval 0        Command was successfully handled and has side effects
 * @retval -EAGAIN  Messages has been requeued for retry
 * @retval -ENOSYS  Unknown command
 * @retval < 0      An error has occurred; **ss** may have been set.
 */
int MDSMonitor::legacy_filesystem_command(
    MonOpRequestRef op,
    std::string const &prefix,
    map<string, cmd_vartype> &cmdmap,
    std::stringstream &ss)
{
  dout(4) << __func__ << " prefix='" << prefix << "'" << dendl;
  op->mark_mdsmon_event(__func__);
  int r = 0;
  string whostr;
  cmd_getval(g_ceph_context, cmdmap, "who", whostr);

  assert (pending_fsmap.legacy_client_namespace != MDS_NAMESPACE_NONE);
  auto fs = pending_fsmap.get_filesystem(pending_fsmap.legacy_client_namespace);

  if (prefix == "mds set_max_mds") {
    // NOTE: see also "mds set max_mds", which can modify the same field.
    int64_t maxmds;
    if (!cmd_getval(g_ceph_context, cmdmap, "maxmds", maxmds) || maxmds < 0) {
      return -EINVAL;
    }
    if (maxmds > MAX_MDS) {
      ss << "may not have more than " << MAX_MDS << " MDS ranks";
      return -EINVAL;
    }
    fs->max_mds = maxmds;
    r = 0;
    ss << "max_mds = " << fs->max_mds;
  } else if (prefix == "mds set") {
    string var;
    if (!cmd_getval(g_ceph_context, cmdmap, "var", var) || var.empty()) {
      ss << "Invalid variable";
      return -EINVAL;
    }
    string val;
    string interr;
    int64_t n = 0;
    if (!cmd_getval(g_ceph_context, cmdmap, "val", val)) {
      return -EINVAL;
    }
    // we got a string.  see if it contains an int.
    n = strict_strtoll(val.c_str(), 10, &interr);
    if (var == "max_mds") {
      // NOTE: see also "mds set_max_mds", which can modify the same field.
      if (interr.length()) {
	return -EINVAL;
      }
      if (n > MAX_MDS) {
        ss << "may not have more than " << MAX_MDS << " MDS ranks";
        return -EINVAL;
      }
      fs->max_mds = n;
    } else if (var == "inline_data") {
      if (val == "true" || val == "yes" || (!interr.length() && n == 1)) {
	string confirm;
	if (!cmd_getval(g_ceph_context, cmdmap, "confirm", confirm) ||
	    confirm != "--yes-i-really-mean-it") {
	  ss << "inline data is new and experimental; you must specify --yes-i-really-mean-it";
	  return -EPERM;
	}
	ss << "inline data enabled";
        fs->set_inline_data_enabled(true);
	pending_fsmap.compat.incompat.insert(MDS_FEATURE_INCOMPAT_INLINE);
      } else if (val == "false" || val == "no" ||
		 (!interr.length() && n == 0)) {
	ss << "inline data disabled";
        fs->set_inline_data_enabled(false);
      } else {
	ss << "value must be false|no|0 or true|yes|1";
	return -EINVAL;
      }
    } else if (var == "max_file_size") {
      if (interr.length()) {
	ss << var << " requires an integer value";
	return -EINVAL;
      }
      if (n < CEPH_MIN_STRIPE_UNIT) {
	ss << var << " must at least " << CEPH_MIN_STRIPE_UNIT;
	return -ERANGE;
      }
      fs->max_file_size = n;
    } else if (var == "allow_new_snaps") {
      if (val == "false" || val == "no" || (interr.length() == 0 && n == 0)) {
        fs->clear_snaps_allowed();
	ss << "disabled new snapshots";
      } else if (val == "true" || val == "yes" || (interr.length() == 0 && n == 1)) {
	string confirm;
	if (!cmd_getval(g_ceph_context, cmdmap, "confirm", confirm) ||
	    confirm != "--yes-i-really-mean-it") {
	  ss << "Snapshots are unstable and will probably break your FS! Set to --yes-i-really-mean-it if you are sure you want to enable them";
	  return -EPERM;
	}
        fs->set_snaps_allowed();
	ss << "enabled new snapshots";
      } else {
	ss << "value must be true|yes|1 or false|no|0";
	return -EINVAL;
      }
    } else {
      ss << "unknown variable " << var;
      return -EINVAL;
    }
    r = 0;
  } else if (prefix == "mds cluster_down") {
    fs->set_flag(CEPH_MDSMAP_DOWN);
    ss << "marked fsmap DOWN";
    r = 0;
  } else if (prefix == "mds cluster_up") {
    fs->clear_flag(CEPH_MDSMAP_DOWN);
    ss << "unmarked fsmap DOWN";
    r = 0;
  } else if (prefix == "mds add_data_pool") {
    string poolname;
    cmd_getval(g_ceph_context, cmdmap, "pool", poolname);
    int64_t poolid = mon->osdmon()->osdmap.lookup_pg_pool_name(poolname);
    if (poolid < 0) {
      string err;
      poolid = strict_strtol(poolname.c_str(), 10, &err);
      if (err.length()) {
	poolid = -1;
	ss << "pool '" << poolname << "' does not exist";
	return -ENOENT;
      }
    }

    r = _check_pool(poolid, &ss);
    if (r != 0) {
      return r;
    }

    fs->add_data_pool(poolid);
    ss << "added data pool " << poolid << " to fsmap";
  } else if (prefix == "mds remove_data_pool") {
    string poolname;
    cmd_getval(g_ceph_context, cmdmap, "pool", poolname);
    int64_t poolid = mon->osdmon()->osdmap.lookup_pg_pool_name(poolname);
    if (poolid < 0) {
      string err;
      poolid = strict_strtol(poolname.c_str(), 10, &err);
      if (err.length()) {
	r = -ENOENT;
	poolid = -1;
	ss << "pool '" << poolname << "' does not exist";
      }
    }

    if (fs->get_first_data_pool() == poolid) {
      r = -EINVAL;
      poolid = -1;
      ss << "cannot remove default data pool";
    }

    if (poolid >= 0) {
      r = fs->remove_data_pool(poolid);
      if (r == -ENOENT)
	r = 0;
      if (r == 0)
	ss << "removed data pool " << poolid << " from fsmap";
    }
  } else {
    return -ENOSYS;
  }

  return r;
}


void MDSMonitor::check_subs()
{
  std::list<std::string> types;

  // Subscriptions may be to "fsmap" (MDS and legacy clients),
  // "fsmap.<namespace>", or to "fsmap" for the full state of all
  // filesystems.  Build a list of all the types we service
  // subscriptions for.
  types.push_back("mdsmap");
  types.push_back("fsmap");
  for (const auto &i : fsmap.filesystems) {
    auto ns = i.first;
    std::ostringstream oss;
    oss << "fsmap." << ns;
    types.push_back(oss.str());
  }

  for (const auto &type : types) {
    if (mon->session_map.subs.count(type) == 0)
      return;
    xlist<Subscription*>::iterator p = mon->session_map.subs[type]->begin();
    while (!p.end()) {
      Subscription *sub = *p;
      ++p;
      check_sub(sub);
    }
  }
}

MDSMap *MDSMonitor::generate_mds_map(
    mds_namespace_t ns)
{
  // Important: this is a blacklisting rather than whitelisting: we are
  // showing all MDSs *except* those explicitly assigned to *another*
  // filesystem: we are liberal about exposing standby MDSs.  This is
  // because standbys don't currently have a namespace affinity, so
  // all are pseudo-part of the cluster for all filesystems.
  // Currently there isn't security preventing from an MDS assigned
  // to one namespace from talking an MDS assigned to another namespace.
  // TODO: get global_id during verify_authorizer in MDS, and refuse
  // to talk to anyone whose GID isn't in the FSMap for my cluster.
  // TODO: cache the map for each filesystem so that when there
  // are multiple filesystems, we don't keep rebuilding it.

  MDSMap *crafted = new MDSMap();

  std::set<mds_gid_t> cull_gids;
  for (const auto &i : fsmap.mds_info) {
    if (i.second.role.ns != MDS_NAMESPACE_NONE && i.second.role.ns != ns) {
      cull_gids.insert(i.first);
    }
  }

  // Populate crafted mds_info
  for (const auto &i : fsmap.mds_info) {
    const auto &source_info = i.second;
    if (cull_gids.count(source_info.global_id)) {
      continue;
    }

    MDSMap::mds_info_t crafted_info;
    crafted_info.global_id = source_info.global_id;
    crafted_info.name = source_info.name;
    crafted_info.rank = source_info.role.rank;
    crafted_info.inc = source_info.inc;
    crafted_info.state = source_info.state;
    crafted_info.state_seq = source_info.state_seq;
    crafted_info.addr = source_info.addr;
    crafted_info.laggy_since = source_info.laggy_since;
    crafted_info.standby_for_rank = source_info.standby_for_rank;
    crafted_info.standby_for_name = source_info.standby_for_name;
    crafted_info.export_targets = source_info.export_targets;

    crafted->mds_info[crafted_info.global_id] = crafted_info;
  }

  crafted->epoch = fsmap.epoch;
  crafted->created = fsmap.created;
  crafted->modified = fsmap.modified;
  crafted->session_timeout = fsmap.session_timeout;
  crafted->session_autoclose = fsmap.session_autoclose;
  crafted->compat = fsmap.compat;

  if (ns != MDS_NAMESPACE_NONE) {
    std::shared_ptr<Filesystem> fs = fsmap.get_filesystem(ns);

    crafted->last_failure = fs->last_failure;
    crafted->last_failure_osd_epoch = fs->last_failure_osd_epoch;
    crafted->tableserver = fs->tableserver;
    crafted->root = fs->root;
    crafted->cas_pool = fs->cas_pool;
    crafted->metadata_pool = fs->metadata_pool;
    crafted->data_pools = fs->data_pools;
    crafted->ever_allowed_snaps = fs->ever_allowed_snaps;
    crafted->explicitly_allowed_snaps = fs->explicitly_allowed_snaps;
    crafted->inline_data_enabled = fs->inline_data_enabled;
    crafted->max_file_size = fs->max_file_size;
    crafted->flags = fs->flags;
    crafted->max_mds = fs->max_mds;

    crafted->damaged = fs->damaged;
    crafted->failed = fs->failed;
    crafted->stopped = fs->stopped;
    crafted->in = fs->in;
    crafted->inc = fs->inc;
    crafted->up = fs->up;

    crafted->enabled = true;
  } else {
    crafted->enabled = false;
  }

  return crafted;
}


void MDSMonitor::check_sub(Subscription *sub)
{
  dout(20) << __func__ << ": " << sub->type << dendl;

  if (sub->type == "fsmap") {
    if (sub->next <= fsmap.get_epoch()) {
      sub->session->con->send_message(new MFSMap(mon->monmap->fsid, &fsmap));
      if (sub->onetime) {
        mon->session_map.remove_sub(sub);
      } else {
        sub->next = fsmap.get_epoch() + 1;
      }
    }
  } else {
    if (sub->next <= fsmap.get_epoch()) {
      const bool is_mds = sub->session->inst.name.is_mds();
      mds_namespace_t ns = MDS_NAMESPACE_NONE;
      if (is_mds) {
        // What (if any) namespace are you assigned to?
        auto mds_info = fsmap.get_mds_info();
        for (const auto &i : mds_info) {
          if (i.second.addr == sub->session->inst.addr) {
            ns = i.second.role.ns;
          }
        }
      } else {
        // You're a client.  Did you request a particular
        // namespace?
        if (sub->type.find("mdsmap.") == 0) {
          auto namespace_id_str = sub->type.substr(std::string("mdsmap.").size());
          dout(10) << __func__ << ": namespace_id " << namespace_id_str << dendl;
          std::string err;
          ns = strict_strtoll(namespace_id_str.c_str(), 10, &err);
          if (!err.empty()) {
            // Client asked for a non-existent namespace, send them nothing
            dout(1) << "Invalid client subscription '" << sub->type
                    << "'" << dendl;
            return;
          }
          if (fsmap.filesystems.count(ns) == 0) {
            // Client asked for a non-existent namespace, send them nothing
            // TODO: something more graceful for when a client has a filesystem
            // mounted, and the fileysstem is deleted.  Add a "shut down you fool"
            // flag to MMDSMap?
            dout(1) << "Client subscribed to non-existent namespace '" <<
                    ns << "'" << dendl;
            return;
          }
        } else {
          assert(sub->type == std::string("mdsmap"));
          if (fsmap.legacy_client_namespace != MDS_NAMESPACE_NONE) {
            ns = fsmap.legacy_client_namespace;
          } else {
            dout(1) << "Client subscribed for legacy filesystem but "
                       "none is configured" << dendl;
            return;
          }
        }
      }
      dout(10) << __func__ << ": is_mds=" << is_mds << ", ns= " << ns << dendl;
      MDSMap *crafted = generate_mds_map(ns);
      auto msg = new MMDSMap(mon->monmap->fsid, crafted);
      delete crafted;

      sub->session->con->send_message(msg);
      if (sub->onetime) {
        mon->session_map.remove_sub(sub);
      } else {
        sub->next = fsmap.get_epoch() + 1;
      }
    }
  }
}


void MDSMonitor::update_metadata(mds_gid_t gid,
				 const map<string, string>& metadata)
{
  if (metadata.empty()) {
    return;
  }
  pending_metadata[gid] = metadata;

  MonitorDBStore::TransactionRef t = paxos->get_pending_transaction();
  bufferlist bl;
  ::encode(pending_metadata, bl);
  t->put(MDS_METADATA_PREFIX, "last_metadata", bl);
  paxos->trigger_propose();
}

void MDSMonitor::remove_from_metadata(MonitorDBStore::TransactionRef t)
{
  bool update = false;
  for (map<mds_gid_t, Metadata>::iterator i = pending_metadata.begin();
       i != pending_metadata.end(); ) {
    if (pending_fsmap.get_state_gid(i->first) == MDSMap::STATE_NULL) {
      pending_metadata.erase(i++);
      update = true;
    } else {
      ++i;
    }
  }
  if (!update)
    return;
  bufferlist bl;
  ::encode(pending_metadata, bl);
  t->put(MDS_METADATA_PREFIX, "last_metadata", bl);
}

int MDSMonitor::load_metadata(map<mds_gid_t, Metadata>& m)
{
  bufferlist bl;
  int r = mon->store->get(MDS_METADATA_PREFIX, "last_metadata", bl);
  if (r)
    return r;

  bufferlist::iterator it = bl.begin();
  ::decode(m, it);
  return 0;
}

int MDSMonitor::dump_metadata(const std::string &who, Formatter *f, ostream& err)
{
  assert(f);

  mds_gid_t gid = gid_from_arg(who, err);
  if (gid == MDS_GID_NONE) {
    return -EINVAL;
  }

  map<mds_gid_t, Metadata> metadata;
  if (int r = load_metadata(metadata)) {
    err << "Unable to load 'last_metadata'";
    return r;
  }

  if (!metadata.count(gid)) {
    return -ENOENT;
  }
  const Metadata& m = metadata[gid];
  for (Metadata::const_iterator p = m.begin(); p != m.end(); ++p) {
    f->dump_string(p->first.c_str(), p->second);
  }
  return 0;
}

int MDSMonitor::print_nodes(Formatter *f)
{
  assert(f);

  map<mds_gid_t, Metadata> metadata;
  if (int r = load_metadata(metadata)) {
    return r;
  }

  map<string, list<int> > mdses; // hostname => rank
  for (map<mds_gid_t, Metadata>::iterator it = metadata.begin();
       it != metadata.end(); ++it) {
    const Metadata& m = it->second;
    Metadata::const_iterator hostname = m.find("hostname");
    if (hostname == m.end()) {
      // not likely though
      continue;
    }
    const mds_gid_t gid = it->first;
    if (fsmap.get_state_gid(gid) == MDSMap::STATE_NULL) {
      dout(5) << __func__ << ": GID " << gid << " not existent" << dendl;
      continue;
    }
    const FSMap::mds_info_t& mds_info = fsmap.get_info_gid(gid);
    // FIXME: include filesystem name with rank here
    mdses[hostname->second].push_back(mds_info.role.rank);
  }

  dump_services(f, mdses, "mds");
  return 0;
}

/**
 * If a cluster is undersized (with respect to max_mds), then
 * attempt to find daemons to grow it.
 */
bool MDSMonitor::maybe_expand_cluster(std::shared_ptr<Filesystem> fs)
{
  bool do_propose = false;

  if (fs->test_flag(CEPH_MDSMAP_DOWN)) {
    return do_propose;
  }

  while (fs->get_num_in_mds() < size_t(fs->get_max_mds()) &&
	 !pending_fsmap.is_degraded(fs->ns)) {
    mds_rank_t mds = mds_rank_t(0);
    string name;
    while (fs->is_in(mds)) {
      mds++;
    }
    mds_gid_t newgid = pending_fsmap.find_replacement_for(mds, name,
                         g_conf->mon_force_standby_active);
    if (!newgid) {
      break;
    }

    FSMap::mds_info_t& info = pending_fsmap.mds_info[newgid];
    info.role = mds_role_t(fs->ns, mds);
    dout(1) << "adding standby " << info.addr << " as mds." << info.role << dendl;

    if (fs->stopped.count(mds)) {
      info.state = MDSMap::STATE_STARTING;
      fs->stopped.erase(mds);
    } else
      info.state = MDSMap::STATE_CREATING;
    info.inc = ++fs->inc[mds];
    fs->in.insert(mds);
    fs->up[mds] = newgid;
    do_propose = true;
  }

  return do_propose;
}


/**
 * If a daemon is laggy, and a suitable replacement
 * is available, fail this daemon (remove from map) and pass its
 * role to another daemon.
 */
void MDSMonitor::maybe_replace_gid(mds_gid_t gid,
    const beacon_info_t &beacon,
    bool *mds_propose, bool *osd_propose)
{
  assert(mds_propose != nullptr);
  assert(osd_propose != nullptr);

  FSMap::mds_info_t& info = pending_fsmap.mds_info[gid];

  dout(10) << "no beacon from " << gid << " " << info.addr << " mds."
    << info.role << "." << info.inc
    << " " << ceph_mds_state_name(info.state)
    << " since " << beacon.stamp << dendl;

  // are we in?
  // and is there a non-laggy standby that can take over for us?
  mds_gid_t sgid;
  if (info.role.rank >= 0 &&
      info.state != MDSMap::STATE_STANDBY &&
      info.state != MDSMap::STATE_STANDBY_REPLAY &&
      (sgid = pending_fsmap.find_replacement_for(info.role.rank, info.name,
                g_conf->mon_force_standby_active)) != MDS_GID_NONE) {
    FSMap::mds_info_t& si = pending_fsmap.mds_info[sgid];
    dout(10) << " replacing " << gid << " " << info.addr << " mds."
      << info.role << "." << info.inc
      << " " << ceph_mds_state_name(info.state)
      << " with " << sgid << "/" << si.name << " " << si.addr << dendl;
    switch (info.state) {
      case MDSMap::STATE_CREATING:
      case MDSMap::STATE_STARTING:
        si.state = info.state;
        break;
      case MDSMap::STATE_REPLAY:
      case MDSMap::STATE_RESOLVE:
      case MDSMap::STATE_RECONNECT:
      case MDSMap::STATE_REJOIN:
      case MDSMap::STATE_CLIENTREPLAY:
      case MDSMap::STATE_ACTIVE:
      case MDSMap::STATE_STOPPING:
      case MDSMap::STATE_DNE:
        si.state = MDSMap::STATE_REPLAY;
        break;
      default:
        assert(0);
    }

    auto fs = pending_fsmap.get_filesystem(info.role.ns);

    info.state_seq = beacon.seq;
    si.role = info.role;
    si.inc = ++fs->inc[info.role.rank];
    fs->up[info.role.rank] = sgid;
    if (si.state > 0) {
      fs->last_failure = pending_fsmap.epoch;
    }
    if (si.state > 0 ||
        si.state == MDSMap::STATE_CREATING ||
        si.state == MDSMap::STATE_STARTING) {
      // blacklist laggy mds
      utime_t until = ceph_clock_now(g_ceph_context);
      until += g_conf->mds_blacklist_interval;
      fs->last_failure_osd_epoch = mon->osdmon()->blacklist(info.addr, until);
      *osd_propose = true;
    }
    pending_fsmap.mds_info.erase(gid);
    pending_daemon_health.erase(gid);
    pending_daemon_health_rm.insert(gid);
    last_beacon.erase(gid);
    *mds_propose = true;
  } else if (info.state == MDSMap::STATE_STANDBY_REPLAY) {
    dout(10) << " failing " << gid << " " << info.addr << " mds." << info.role << "." << info.inc
      << " " << ceph_mds_state_name(info.state)
      << dendl;
    pending_fsmap.mds_info.erase(gid);
    pending_daemon_health.erase(gid);
    pending_daemon_health_rm.insert(gid);
    last_beacon.erase(gid);
    *mds_propose = true;
  } else {
    if (info.state == MDSMap::STATE_STANDBY ||
        info.state == MDSMap::STATE_STANDBY_REPLAY) {
      // remove it
      dout(10) << " removing " << gid << " " << info.addr << " mds." << info.role << "." << info.inc
        << " " << ceph_mds_state_name(info.state)
        << " (laggy)" << dendl;
      pending_fsmap.mds_info.erase(gid);
      pending_daemon_health.erase(gid);
      pending_daemon_health_rm.insert(gid);
      *mds_propose = true;
    } else if (!info.laggy()) {
      dout(10) << " marking " << gid << " " << info.addr << " mds." << info.role << "." << info.inc
        << " " << ceph_mds_state_name(info.state)
        << " laggy" << dendl;
      info.laggy_since = ceph_clock_now(g_ceph_context);
      *mds_propose = true;
    }
    last_beacon.erase(gid);
  }
}

bool MDSMonitor::maybe_promote_standby(std::shared_ptr<Filesystem> fs)
{
  bool do_propose = false;

  // have a standby take over?
  set<mds_rank_t> failed;
  fs->get_failed_mds_set(failed);
  if (!failed.empty() && !fs->test_flag(CEPH_MDSMAP_DOWN)) {
    set<mds_rank_t>::iterator p = failed.begin();
    while (p != failed.end()) {
      mds_rank_t f = *p++;
      string name;  // FIXME
      mds_gid_t sgid = pending_fsmap.find_replacement_for(f, name,
          g_conf->mon_force_standby_active);
      if (sgid) {
	FSMap::mds_info_t& si = pending_fsmap.mds_info[sgid];
	dout(0) << " taking over failed mds." << f << " with " << sgid << "/" << si.name << " " << si.addr << dendl;
	si.state = MDSMap::STATE_REPLAY;
	si.role = {fs->ns, f};
	si.inc = ++fs->inc[f];
	fs->in.insert(f);
	fs->up[f] = sgid;
	fs->failed.erase(f);
	do_propose = true;
      }
    }
  }

  // have a standby follow someone?
  if (failed.empty()) {
    for (map<mds_gid_t,FSMap::mds_info_t>::iterator j = pending_fsmap.mds_info.begin();
	 j != pending_fsmap.mds_info.end();
	 ++j) {
      FSMap::mds_info_t& info = j->second;
      
      if (info.state != MDSMap::STATE_STANDBY)
	continue;

      /*
       * This mds is standby but has no rank assigned.
       * See if we can find it somebody to shadow
       */
      dout(20) << "gid " << j->first << " is standby and following nobody" << dendl;
      
      // standby for someone specific?
      // FIXME: reinstate standby_for_rank
#if 0
      if (info.standby_for_rank >= 0) {
	if (pending_fsmap.is_followable(info.standby_for_rank) &&
	    try_standby_replay(info, pending_fsmap.mds_info[pending_fsmap.up[info.standby_for_rank]]))
	  do_propose = true;
	continue;
      }
#endif

      // check everyone
      for (map<mds_gid_t,FSMap::mds_info_t>::iterator i = pending_fsmap.mds_info.begin();
	   i != pending_fsmap.mds_info.end();
	   ++i) {
	if (i->second.role.rank >= 0 && pending_fsmap.is_followable(i->second.role)) {
	  if ((info.standby_for_name.length() && info.standby_for_name != i->second.name) ||
	      info.standby_for_rank >= 0)
	    continue;   // we're supposed to follow someone else

	  if (info.standby_for_rank == FSMap::MDS_STANDBY_ANY &&
	      try_standby_replay(info, i->second)) {
	    do_propose = true;
	    break;
	  }
	  continue;
	}
      }
    }
  }

  return do_propose;
}

void MDSMonitor::tick()
{
  // make sure mds's are still alive
  // ...if i am an active leader
  if (!is_active()) return;

  dout(10) << fsmap << dendl;

  bool do_propose = false;

  if (!mon->is_leader()) return;

  // expand mds cluster (add new nodes to @in)?
  for (auto i : pending_fsmap.filesystems) {
    do_propose |= maybe_expand_cluster(i.second);
  }

  // check beacon timestamps
  utime_t now = ceph_clock_now(g_ceph_context);
  utime_t cutoff = now;
  cutoff -= g_conf->mds_beacon_grace;

  // make sure last_beacon is fully populated
  for (auto p : pending_fsmap.mds_info) {
    auto &gid = p.first;
    auto &info = p.second;
    if (last_beacon.count(gid) == 0) {
      dout(10) << " adding " << info.addr << " mds." << info.role << "."
               << info.inc << " " << ceph_mds_state_name(info.state)
	       << " to last_beacon" << dendl;
      last_beacon[gid].stamp = ceph_clock_now(g_ceph_context);
      last_beacon[gid].seq = 0;
    }
  }

  // If the OSDMap is writeable, we can blacklist things, so we can
  // try failing any laggy MDS daemons.  Consider each one for failure.
  if (mon->osdmon()->is_writeable()) {
    bool propose_osdmap = false;

    map<mds_gid_t, beacon_info_t>::iterator p = last_beacon.begin();
    while (p != last_beacon.end()) {
      mds_gid_t gid = p->first;
      auto beacon_info = p->second;
      ++p;

      if (pending_fsmap.mds_info.count(gid) == 0) {
	// clean it out
	last_beacon.erase(gid);
	continue;
      }

      if (beacon_info.stamp < cutoff) {
        maybe_replace_gid(gid, beacon_info, &do_propose, &propose_osdmap);
      }
    }

    if (propose_osdmap) {
      request_proposal(mon->osdmon());
    }
  }

  for (auto i : pending_fsmap.filesystems) {
    auto fs = i.second;
    do_propose |= maybe_promote_standby(fs);
  }

  if (do_propose) {
    propose_pending();
  }
}

bool MDSMonitor::try_standby_replay(FSMap::mds_info_t& finfo, FSMap::mds_info_t& ainfo)
{
  // someone else already following?
  mds_gid_t lgid = pending_fsmap.find_standby_for(ainfo.role.rank, ainfo.name);
  if (lgid) {
    FSMap::mds_info_t& sinfo = pending_fsmap.mds_info[lgid];
    dout(20) << " mds." << ainfo.role
	     << " standby gid " << lgid << " with state "
	     << ceph_mds_state_name(sinfo.state)
	     << dendl;
    if (sinfo.state == MDSMap::STATE_STANDBY_REPLAY) {
      dout(20) << "  skipping this MDS since it has a follower!" << dendl;
      return false; // this MDS already has a standby
    }
  }

  // hey, we found an MDS without a standby. Pair them!
  finfo.standby_for_rank = ainfo.role.rank;
  dout(10) << "  setting to shadow mds rank " << finfo.standby_for_rank << dendl;
  finfo.state = MDSMap::STATE_STANDBY_REPLAY;
  return true;
}
