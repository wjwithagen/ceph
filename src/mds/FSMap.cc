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


#include "FSMap.h"

#include <sstream>
using std::stringstream;


const mds_rank_t FSMap::MDS_NO_STANDBY_PREF(-1);
const mds_rank_t FSMap::MDS_STANDBY_ANY(-2);
const mds_rank_t FSMap::MDS_STANDBY_NAME(-3);
const mds_rank_t FSMap::MDS_MATCHED_ACTIVE(-4);

void FSMap::mds_info_t::dump(Formatter *f) const
{
  f->dump_unsigned("gid", global_id);
  f->dump_string("name", name);
  f->dump_int("rank", role.rank);
  f->dump_int("ns", role.ns);
  f->dump_int("incarnation", inc);
  f->dump_stream("state") << ceph_mds_state_name(state);
  f->dump_int("state_seq", state_seq);
  f->dump_stream("addr") << addr;
  if (laggy_since != utime_t())
    f->dump_stream("laggy_since") << laggy_since;
  
  f->dump_int("standby_for_rank", standby_for_rank);
  f->dump_string("standby_for_name", standby_for_name);
  f->open_array_section("export_targets");
  for (set<mds_rank_t>::iterator p = export_targets.begin();
       p != export_targets.end(); ++p) {
    f->dump_int("mds", *p);
  }
  f->close_section();
}

void FSMap::mds_info_t::generate_test_instances(list<mds_info_t*>& ls)
{
  mds_info_t *sample = new mds_info_t();
  ls.push_back(sample);
  sample = new mds_info_t();
  sample->global_id = 1;
  sample->name = "test_instance";
  sample->role.rank = 1;
  sample->role.ns = 2;
  ls.push_back(sample);
}

void Filesystem::dump(Formatter *f) const
{
  f->dump_int("tableserver", tableserver);
  f->dump_int("root", root);
  f->dump_int("last_failure", last_failure);
  f->dump_int("last_failure_osd_epoch", last_failure_osd_epoch);
  f->dump_int("max_mds", max_mds);
  f->open_array_section("in");
  for (set<mds_rank_t>::const_iterator p = in.begin(); p != in.end(); ++p)
    f->dump_int("mds", *p);
  f->close_section();
  f->open_object_section("up");
  for (map<mds_rank_t,mds_gid_t>::const_iterator p = up.begin(); p != up.end(); ++p) {
    char s[14];
    sprintf(s, "mds_%d", int(p->first));
    f->dump_int(s, p->second);
  }
  f->close_section();
  f->open_array_section("failed");
  for (set<mds_rank_t>::const_iterator p = failed.begin(); p != failed.end(); ++p)
    f->dump_int("mds", *p);
  f->close_section();
  f->open_array_section("damaged");
  for (set<mds_rank_t>::const_iterator p = damaged.begin(); p != damaged.end(); ++p)
    f->dump_int("mds", *p);
  f->close_section();
  f->open_array_section("stopped");
  for (set<mds_rank_t>::const_iterator p = stopped.begin(); p != stopped.end(); ++p)
    f->dump_int("mds", *p);
  f->close_section();

  f->open_array_section("data_pools");
  for (set<int64_t>::const_iterator p = data_pools.begin(); p != data_pools.end(); ++p)
    f->dump_int("pool", *p);
  f->close_section();
  f->dump_int("metadata_pool", metadata_pool);
  f->dump_string("fs_name", fs_name);
  f->dump_int("id", ns);

  f->dump_unsigned("flags", flags);
  f->dump_int("max_file_size", max_file_size);
}

void FSMap::dump(Formatter *f) const
{
  f->dump_int("epoch", epoch);
  f->dump_stream("created") << created;
  f->dump_stream("modified") << modified;
  f->dump_int("session_timeout", session_timeout);
  f->dump_int("session_autoclose", session_autoclose);

  f->open_object_section("compat");
  compat.dump(f);
  f->close_section();

  f->open_object_section("info");
  for (map<mds_gid_t,mds_info_t>::const_iterator p = mds_info.begin(); p != mds_info.end(); ++p) {
    char s[25]; // 'gid_' + len(str(ULLONG_MAX)) + '\0'
    sprintf(s, "gid_%llu", (long long unsigned)p->first);
    f->open_object_section(s);
    p->second.dump(f);
    f->close_section();
  }
  f->close_section();

  f->open_object_section("filesystems");
  for (const auto fs : filesystems) {
    fs.second->dump(f);
  }
  f->close_section();
}

void FSMap::generate_test_instances(list<FSMap*>& ls)
{
  FSMap *m = new FSMap();
  m->compat = get_mdsmap_compat_set_all();

  auto fs = std::make_shared<Filesystem>();
  fs->ns = 20;
  fs->max_mds = 1;
  fs->data_pools.insert(0);
  fs->metadata_pool = 1;
  fs->cas_pool = 2;
  fs->max_file_size = 1<<24;
  m->filesystems[fs->ns] = fs;

  // these aren't the defaults, just in case anybody gets confused
  m->session_timeout = 61;
  m->session_autoclose = 301;
  ls.push_back(m);
}

void FSMap::print(ostream& out) 
{
  JSONFormatter f;
  dump(&f);
  f.flush(out);

#if 0
  out << "epoch\t" << epoch << "\n";
  out << "flags\t" << hex << flags << dec << "\n";
  out << "created\t" << created << "\n";
  out << "modified\t" << modified << "\n";
  out << "tableserver\t" << tableserver << "\n";
  out << "root\t" << root << "\n";
  out << "session_timeout\t" << session_timeout << "\n"
      << "session_autoclose\t" << session_autoclose << "\n";
  out << "max_file_size\t" << max_file_size << "\n";
  out << "last_failure\t" << last_failure << "\n"
      << "last_failure_osd_epoch\t" << last_failure_osd_epoch << "\n";
  out << "compat\t" << compat << "\n";
  out << "max_mds\t" << max_mds << "\n";
  out << "in\t" << in << "\n"
      << "up\t" << up << "\n"
      << "failed\t" << failed << "\n"
      << "damaged\t" << damaged << "\n"
      << "stopped\t" << stopped << "\n";
  out << "data_pools\t" << data_pools << "\n";
  out << "metadata_pool\t" << metadata_pool << "\n";
  out << "inline_data\t" << (inline_data_enabled ? "enabled" : "disabled") << "\n";

  multimap< pair<mds_rank_t, unsigned>, mds_gid_t > foo;
  for (map<mds_gid_t,mds_info_t>::iterator p = mds_info.begin();
       p != mds_info.end();
       ++p)
    foo.insert(std::make_pair(std::make_pair(p->second.rank, p->second.inc-1), p->first));

  for (multimap< pair<mds_rank_t, unsigned>, mds_gid_t >::iterator p = foo.begin();
       p != foo.end();
       ++p) {
    mds_info_t& info = mds_info[p->second];
    
    out << p->second << ":\t"
	<< info.addr
	<< " '" << info.name << "'"
	<< " mds." << info.rank
	<< "." << info.inc
	<< " " << ceph_mds_state_name(info.state)
	<< " seq " << info.state_seq;
    if (info.laggy())
      out << " laggy since " << info.laggy_since;
    if (info.standby_for_rank != -1 ||
	!info.standby_for_name.empty()) {
      out << " (standby for";
      //if (info.standby_for_rank >= 0)
	out << " rank " << info.standby_for_rank;
      if (!info.standby_for_name.empty())
	out << " '" << info.standby_for_name << "'";
      out << ")";
    }
    if (!info.export_targets.empty())
      out << " export_targets=" << info.export_targets;
    out << "\n";    
  }
#endif
}



// FIXME: revisit this (in the overall health reporting) to
// give a per-filesystem health.
void FSMap::print_summary(Formatter *f, ostream *out)
{
  map<mds_role_t,string> by_rank;
  map<string,int> by_state;

  if (f) {
    f->dump_unsigned("epoch", get_epoch());
    for (auto i : filesystems) {
      auto fs = i.second;
      f->dump_unsigned("id", fs->ns);
      f->dump_unsigned("up", fs->up.size());
      f->dump_unsigned("in", fs->in.size());
      f->dump_unsigned("max", fs->max_mds);
    }
  } else {
    *out << "e" << get_epoch() << ":";
    if (filesystems.size() == 1) {
      auto fs = filesystems.begin()->second;
      *out << " " << fs->up.size() << "/" << fs->in.size() << "/"
           << fs->max_mds << " up";
    } else {
      for (auto i : filesystems) {
        auto fs = i.second;
        *out << " " << fs->fs_name << "-" << fs->up.size() << "/"
             << fs->in.size() << "/" << fs->max_mds << " up";
      }
    }
  }

  if (f)
    f->open_array_section("by_rank");
  for (map<mds_gid_t,mds_info_t>::iterator p = mds_info.begin();
       p != mds_info.end();
       ++p) {
    string s = ceph_mds_state_name(p->second.state);
    if (p->second.laggy())
      s += "(laggy or crashed)";

    if (p->second.role.rank != MDS_RANK_NONE) {
      if (f) {
	f->open_object_section("mds");
	f->dump_unsigned("filesystem_id", p->second.role.ns);
	f->dump_unsigned("rank", p->second.role.rank);
	f->dump_string("name", p->second.name);
	f->dump_string("status", s);
	f->close_section();
      } else {
	by_rank[p->second.role] = p->second.name + "=" + s;
      }
    } else {
      by_state[s]++;
    }
  }
  if (f) {
    f->close_section();
  } else {
    if (!by_rank.empty()) {
      if (filesystems.size() > 0) {
        // Disambiguate filesystems
        std::map<std::string, std::string> pretty;
        for (auto i : by_rank) {
          std::ostringstream o;
          // WTF?  How is by_rank getting populated with invalid-namespace
          // things?
#if 0
          std::cerr << i.first.ns << std::endl;
          std::cerr << i.first.rank << std::endl;
          std::cerr << i.second;
          o << filesystems.at(i.first.ns)->fs_name << "-" << i.first.rank;
#endif
          o << "[" << i.first.ns << " " << i.first.rank << "]";
          pretty[o.str()] = i.second;
        }
        *out << " " << pretty;
      } else {
        *out << " " << by_rank;
      }
    }
  }

  for (map<string,int>::reverse_iterator p = by_state.rbegin(); p != by_state.rend(); ++p) {
    if (f) {
      f->dump_unsigned(p->first.c_str(), p->second);
    } else {
      *out << ", " << p->second << " " << p->first;
    }
  }

  size_t failed = 0;
  size_t damaged = 0;
  for (auto i : filesystems) {
    auto fs = i.second;
    failed += fs->failed.size();
    damaged += fs->damaged.size();
  }

  if (failed > 0) {
    if (f) {
      f->dump_unsigned("failed", failed);
    } else {
      *out << ", " << failed << " failed";
    }
  }

  if (damaged > 0) {
    if (f) {
      f->dump_unsigned("damaged", damaged);
    } else {
      *out << ", " << damaged << " damaged";
    }
  }
  //if (stopped.size())
  //out << ", " << stopped.size() << " stopped";
}

void Filesystem::get_health(list<pair<health_status_t,string> >& summary,
			list<pair<health_status_t,string> > *detail) const
{
  if (!failed.empty()) {
    std::ostringstream oss;
    oss << "mds rank"
	<< ((failed.size() > 1) ? "s ":" ")
	<< failed
	<< ((failed.size() > 1) ? " have":" has")
	<< " failed";
    summary.push_back(make_pair(HEALTH_ERR, oss.str()));
    if (detail) {
      for (set<mds_rank_t>::const_iterator p = failed.begin(); p != failed.end(); ++p) {
	std::ostringstream oss;
	oss << "mds." << *p << " has failed";
	detail->push_back(make_pair(HEALTH_ERR, oss.str()));
      }
    }
  }

  if (!damaged.empty()) {
    std::ostringstream oss;
    oss << "mds rank"
	<< ((damaged.size() > 1) ? "s ":" ")
	<< damaged
	<< ((damaged.size() > 1) ? " are":" is")
	<< " damaged";
    summary.push_back(make_pair(HEALTH_ERR, oss.str()));
    if (detail) {
      for (set<mds_rank_t>::const_iterator p = damaged.begin(); p != damaged.end(); ++p) {
	std::ostringstream oss;
	oss << "mds." << *p << " is damaged";
	detail->push_back(make_pair(HEALTH_ERR, oss.str()));
      }
    }
  }


}


void FSMap::get_health(list<pair<health_status_t,string> >& summary,
			list<pair<health_status_t,string> > *detail) const
{
  for (auto i : filesystems) {
    auto fs = i.second;

    fs->get_health(summary, detail);

    if (is_degraded(fs->ns)) {
      summary.push_back(make_pair(HEALTH_WARN, "mds cluster is degraded"));
      if (detail) {
        detail->push_back(make_pair(HEALTH_WARN, "mds cluster is degraded"));
        for (mds_rank_t i = mds_rank_t(0); i < fs->get_max_mds(); i++) {
          if (!fs->is_up(i))
            continue;
          mds_gid_t gid = fs->up.find(i)->second;
          auto info = mds_info.at(gid);
          stringstream ss;
          if (is_resolve(mds_role_t(fs->ns, i)))
            ss << "mds." << info.name << " at " << info.addr
               << " rank " << i << " is resolving";
          if (is_replay(mds_role_t(fs->ns, i)))
            ss << "mds." << info.name << " at " << info.addr
               << " rank " << i << " is replaying journal";
          if (is_rejoin(mds_role_t(fs->ns, i)))
            ss << "mds." << info.name << " at " << info.addr
               << " rank " << i << " is rejoining";
          if (is_reconnect(mds_role_t(fs->ns, i)))
            ss << "mds." << info.name << " at " << info.addr
               << " rank " << i << " is reconnecting to clients";
          if (ss.str().length())
            detail->push_back(make_pair(HEALTH_WARN, ss.str()));
        }
      }
    }

    set<string> laggy;
    for (auto u : fs->up) {
      auto info = mds_info.at(u.second);
      if (info.laggy()) {
        laggy.insert(info.name);
        if (detail) {
          std::ostringstream oss;
          oss << "mds." << info.name << " at " << info.addr << " is laggy/unresponsive";
          detail->push_back(make_pair(HEALTH_WARN, oss.str()));
        }
      }
    }

    if (!laggy.empty()) {
      std::ostringstream oss;
      oss << "mds " << laggy
          << ((laggy.size() > 1) ? " are":" is")
          << " laggy";
      summary.push_back(make_pair(HEALTH_WARN, oss.str()));
    }
  }
}

void FSMap::mds_info_t::encode_versioned(bufferlist& bl, uint64_t features) const
{
  ENCODE_START(5, 4, bl);
  ::encode(global_id, bl);
  ::encode(name, bl);
  ::encode(role.rank, bl);
  ::encode(inc, bl);
  ::encode((int32_t)state, bl);
  ::encode(state_seq, bl);
  ::encode(addr, bl);
  ::encode(laggy_since, bl);
  ::encode(standby_for_rank, bl);
  ::encode(standby_for_name, bl);
  ::encode(export_targets, bl);
  ::encode(role.ns, bl);
  ENCODE_FINISH(bl);
}

void FSMap::mds_info_t::encode_unversioned(bufferlist& bl) const
{
  __u8 struct_v = 3;
  ::encode(struct_v, bl);
  ::encode(global_id, bl);
  ::encode(name, bl);
  ::encode(role.rank, bl);
  ::encode(inc, bl);
  ::encode((int32_t)state, bl);
  ::encode(state_seq, bl);
  ::encode(addr, bl);
  ::encode(laggy_since, bl);
  ::encode(standby_for_rank, bl);
  ::encode(standby_for_name, bl);
  ::encode(export_targets, bl);
}

void FSMap::mds_info_t::decode(bufferlist::iterator& bl)
{
  DECODE_START_LEGACY_COMPAT_LEN(5, 4, 4, bl);
  ::decode(global_id, bl);
  ::decode(name, bl);
  ::decode(role.rank, bl);
  ::decode(inc, bl);
  ::decode((int32_t&)(state), bl);
  ::decode(state_seq, bl);
  ::decode(addr, bl);
  ::decode(laggy_since, bl);
  ::decode(standby_for_rank, bl);
  ::decode(standby_for_name, bl);
  if (struct_v >= 2)
    ::decode(export_targets, bl);
  if (struct_v >= 5) {
    ::decode(role.ns, bl);
  } else {
    // Upgrading: if the daemon had a rank, assign it to the ANONYMOUS
    // namespace, the same namespace ID that's used in FSMap::decode
    // for synthesizing a Filesystem from a legacy map
    role.ns = MDS_NAMESPACE_ANONYMOUS;
  }
  if (role.rank >= 0) {
    assert(role.ns != MDS_NAMESPACE_NONE);
  }

  DECODE_FINISH(bl);
}



void FSMap::encode(bufferlist& bl, uint64_t features) const
{
  // Pull values from the legacy client namespace
  // for any clients using this ancient protocol.
  epoch_t last_failure = 0;
  epoch_t last_failure_osd_epoch = 0;
  mds_rank_t root = 0;
  mds_rank_t tableserver = 0;
  uint64_t max_file_size = 0;
  mds_rank_t max_mds = 0;
  std::set<int64_t> data_pools;
  int64_t cas_pool = 0;
  int64_t metadata_pool = 0;
  bool inline_data_enabled = false;
  std::string fs_name;
  uint32_t flags = 0;
  bool ever_allowed_snaps = false;
  bool explicitly_allowed_snaps = false;
  bool enabled = false;

  std::set<mds_rank_t> in, failed, stopped, damaged;
  std::map<mds_rank_t, mds_gid_t> up;
  std::map<mds_rank_t,int32_t> inc;

  if (legacy_client_namespace != MDS_NAMESPACE_NONE) {
    auto fs = filesystems.at(legacy_client_namespace);
    last_failure = fs->last_failure;
    last_failure_osd_epoch = fs->last_failure_osd_epoch;
    root = fs->root;
    tableserver = fs->tableserver;
    max_file_size = fs->max_file_size;
    max_mds = fs->max_mds;
    data_pools = fs->data_pools;
    cas_pool = fs->cas_pool;
    metadata_pool = fs->metadata_pool;

    in = fs->in;
    failed = fs->failed;
    stopped = fs->stopped;
    damaged = fs->damaged;
    up = fs->up;
    inc = fs->inc;
    ever_allowed_snaps = fs->ever_allowed_snaps;
    explicitly_allowed_snaps = fs->explicitly_allowed_snaps;

    inline_data_enabled = fs->inline_data_enabled;
    fs_name = fs->fs_name;
    flags = fs->flags;
    enabled = true;
  }

  if ((features & CEPH_FEATURE_PGID64) == 0) {
    __u16 v = 2;

    ::encode(v, bl);
    ::encode(epoch, bl);
    ::encode(flags, bl);
    ::encode(last_failure, bl);
    ::encode(root, bl);
    ::encode(session_timeout, bl);
    ::encode(session_autoclose, bl);
    ::encode(max_file_size, bl);
    ::encode(max_mds, bl);
    __u32 n = mds_info.size();
    ::encode(n, bl);
    for (map<mds_gid_t, mds_info_t>::const_iterator i = mds_info.begin();
	i != mds_info.end(); ++i) {
      ::encode(i->first, bl);
      ::encode(i->second, bl, features);
    }
    n = data_pools.size();
    ::encode(n, bl);
    for (set<int64_t>::const_iterator p = data_pools.begin(); p != data_pools.end(); ++p) {
      n = *p;
      ::encode(n, bl);
    }

    int32_t m = cas_pool;
    ::encode(m, bl);
    return;
  } else if ((features & CEPH_FEATURE_MDSENC) == 0) {
    __u16 v = 3;

    ::encode(v, bl);
    ::encode(epoch, bl);
    ::encode(flags, bl);
    ::encode(last_failure, bl);
    ::encode(root, bl);
    ::encode(session_timeout, bl);
    ::encode(session_autoclose, bl);
    ::encode(max_file_size, bl);
    ::encode(max_mds, bl);
    __u32 n = mds_info.size();
    ::encode(n, bl);
    for (map<mds_gid_t, mds_info_t>::const_iterator i = mds_info.begin();
	i != mds_info.end(); ++i) {
      ::encode(i->first, bl);
      ::encode(i->second, bl, features);
    }
    ::encode(data_pools, bl);
    ::encode(cas_pool, bl);

    // kclient ignores everything from here
    __u16 ev = 5;
    ::encode(ev, bl);
    ::encode(compat, bl);
    ::encode(metadata_pool, bl);
    ::encode(created, bl);
    ::encode(modified, bl);
    ::encode(tableserver, bl);
    ::encode(in, bl);
    ::encode(inc, bl);
    ::encode(up, bl);
    ::encode(failed, bl);
    ::encode(stopped, bl);
    ::encode(last_failure_osd_epoch, bl);
    return;
  }

  ENCODE_START(5, 4, bl);
  ::encode(epoch, bl);
  ::encode(flags, bl);
  ::encode(last_failure, bl);
  ::encode(root, bl);
  ::encode(session_timeout, bl);
  ::encode(session_autoclose, bl);
  ::encode(max_file_size, bl);
  ::encode(max_mds, bl);
  ::encode(mds_info, bl, features);
  ::encode(data_pools, bl);
  ::encode(cas_pool, bl);

  // kclient ignores everything from here
  __u16 ev = 10;
  ::encode(ev, bl);
  ::encode(compat, bl);
  ::encode(metadata_pool, bl);
  ::encode(created, bl);
  ::encode(modified, bl);
  ::encode(tableserver, bl);
  ::encode(in, bl);
  ::encode(inc, bl);
  ::encode(up, bl);
  ::encode(failed, bl);
  ::encode(stopped, bl);
  ::encode(last_failure_osd_epoch, bl);
  ::encode(ever_allowed_snaps, bl);
  ::encode(explicitly_allowed_snaps, bl);
  ::encode(inline_data_enabled, bl);
  ::encode(enabled, bl);
  ::encode(fs_name, bl);
  ::encode(damaged, bl);
  ::encode(next_filesystem_id, bl);
  ::encode(legacy_client_namespace, bl);
  std::vector<Filesystem> fs_list;
  for (auto i : filesystems) {
    fs_list.push_back(*(i.second));
  }
  ::encode(fs_list, bl);
  ENCODE_FINISH(bl);
}

void FSMap::decode(bufferlist::iterator& p)
{
  Filesystem legacy_fs;
  bool enabled = false;
  
  DECODE_START_LEGACY_COMPAT_LEN_16(5, 4, 4, p);
  ::decode(epoch, p);
  ::decode(legacy_fs.flags, p);
  ::decode(legacy_fs.last_failure, p);
  ::decode(legacy_fs.root, p);
  ::decode(session_timeout, p);
  ::decode(session_autoclose, p);
  ::decode(legacy_fs.max_file_size, p);
  ::decode(legacy_fs.max_mds, p);
  ::decode(mds_info, p);
  if (struct_v < 3) {
    __u32 n;
    ::decode(n, p);
    while (n--) {
      __u32 m;
      ::decode(m, p);
      legacy_fs.data_pools.insert(m);
    }
    __s32 s;
    ::decode(s, p);
    legacy_fs.cas_pool = s;
  } else {
    ::decode(legacy_fs.data_pools, p);
    ::decode(legacy_fs.cas_pool, p);
  }

  // kclient ignores everything from here
  __u16 ev = 1;
  if (struct_v >= 2)
    ::decode(ev, p);
  if (ev >= 3)
    ::decode(compat, p);
  else
    compat = get_mdsmap_compat_set_base();
  if (ev < 5) {
    __u32 n;
    ::decode(n, p);
    legacy_fs.metadata_pool = n;
  } else {
    ::decode(legacy_fs.metadata_pool, p);
  }
  ::decode(created, p);
  ::decode(modified, p);
  ::decode(legacy_fs.tableserver, p);
  ::decode(legacy_fs.in, p);
  ::decode(legacy_fs.inc, p);
  ::decode(legacy_fs.up, p);
  ::decode(legacy_fs.failed, p);
  ::decode(legacy_fs.stopped, p);
  if (ev >= 4)
    ::decode(legacy_fs.last_failure_osd_epoch, p);
  if (ev >= 6) {
    ::decode(legacy_fs.ever_allowed_snaps, p);
    ::decode(legacy_fs.explicitly_allowed_snaps, p);
  } else {
    legacy_fs.ever_allowed_snaps = true;
    legacy_fs.explicitly_allowed_snaps = false;
  }
  if (ev >= 7)
    ::decode(legacy_fs.inline_data_enabled, p);

  if (ev >= 8) {
    assert(struct_v >= 5);
    ::decode(enabled, p);
    ::decode(legacy_fs.fs_name, p);
  } else {
    if (epoch > 1) {
      // If an MDS has ever been started, epoch will be greater than 1,
      // assume filesystem is enabled.
      enabled = true;
    } else {
      // Upgrading from a cluster that never used an MDS, switch off
      // filesystem until it's explicitly enabled.
      enabled = false;
    }
  }

  if (ev >= 9) {
    ::decode(legacy_fs.damaged, p);
  }

  if (ev >= 10) {
    ::decode(next_filesystem_id, p);
    ::decode(legacy_client_namespace, p);
    std::vector<Filesystem> fs_list;
    ::decode(fs_list, p);
    std::cerr << "decoding new style " << fs_list.size() << std::endl;
    filesystems.clear();
    for (std::vector<Filesystem>::const_iterator fs = fs_list.begin(); fs != fs_list.end(); ++fs) {
      filesystems[fs->ns] = std::make_shared<Filesystem>(*fs);
    }
  } else if (ev < 10 && enabled) {
    // We're upgrading, populate fs_list from the legacy fields
    std::cerr << "decoding upgrade" << std::endl;
    assert(filesystems.empty());
    auto migrate_fs = std::make_shared<Filesystem>(); 

    *migrate_fs = legacy_fs;
    migrate_fs->ns = MDS_NAMESPACE_ANONYMOUS;
    migrate_fs->fs_name = "default";
    legacy_client_namespace = migrate_fs->ns;
  }

  DECODE_FINISH(p);
}


void Filesystem::encode(bufferlist& bl) const
{
  ENCODE_START(1, 1, bl);
  ::encode(fs_name, bl);
  ::encode(ns, bl);
  ::encode(last_failure, bl);
  ::encode(last_failure_osd_epoch, bl);
  ::encode(tableserver, bl);
  ::encode(root, bl);
  ::encode(data_pools, bl);
  ::encode(cas_pool, bl);
  ::encode(metadata_pool, bl);
  ::encode(ever_allowed_snaps, bl);
  ::encode(explicitly_allowed_snaps, bl);
  ::encode(inline_data_enabled, bl);
  ::encode(max_file_size, bl);
  ::encode(flags, bl);
  ::encode(max_mds, bl);
  ::encode(in, bl);
  ::encode(inc, bl);
  ::encode(failed, bl);
  ::encode(stopped, bl);
  ::encode(damaged, bl);
  ::encode(up, bl);
  std::cerr << "Filesystem::encode " << fs_name << std::endl;
  ENCODE_FINISH(bl);
}

void Filesystem::decode(bufferlist::iterator& p)
{
  DECODE_START(1, p);
  ::decode(fs_name, p);
  ::decode(ns, p);
  ::decode(last_failure, p);
  ::decode(last_failure_osd_epoch, p);
  ::decode(tableserver, p);
  ::decode(root, p);
  ::decode(data_pools, p);
  ::decode(cas_pool, p);
  ::decode(metadata_pool, p);
  ::decode(ever_allowed_snaps, p);
  ::decode(explicitly_allowed_snaps, p);
  ::decode(inline_data_enabled, p);
  ::decode(max_file_size, p);
  ::decode(flags, p);
  ::decode(max_mds, p);
  ::decode(in, p);
  ::decode(inc, p);
  ::decode(failed, p);
  ::decode(stopped, p);
  ::decode(damaged, p);
  ::decode(up, p);
  std::cerr << "Filesystem::decode " << fs_name << std::endl;
  DECODE_FINISH(p);
}

int FSMap::parse_filesystem(
      std::string const &ns_str,
      std::shared_ptr<Filesystem> *result
      ) const
{
  std::string ns_err;
  mds_namespace_t ns = strict_strtol(ns_str.c_str(), 10, &ns_err);
  if (!ns_err.empty() || filesystems.count(ns) == 0) {
    for (auto fs : filesystems) {
      if (fs.second->fs_name == ns_str) {
        *result = fs.second;
        return 0;
      }
    }
    return -ENOENT;
  } else {
    *result = get_filesystem(ns);
    return 0;
  }
}

