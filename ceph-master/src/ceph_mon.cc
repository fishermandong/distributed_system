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

#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>

#include <iostream>
#include <string>
using namespace std;

#include "common/config.h"
#include "include/ceph_features.h"

#include "mon/MonMap.h"
#include "mon/Monitor.h"
#include "mon/MonitorDBStore.h"
#include "mon/MonClient.h"

#include "msg/Messenger.h"

#include "include/CompatSet.h"

#include "common/ceph_argparse.h"
#include "common/pick_address.h"
#include "common/Timer.h"
#include "common/errno.h"
#include "common/Preforker.h"

#include "global/global_init.h"
#include "global/signal_handler.h"

#include "perfglue/heap_profiler.h"

#include "include/assert.h"

#include "erasure-code/ErasureCodePlugin.h"

#define dout_subsys ceph_subsys_mon

Monitor *mon = NULL;

void handle_mon_signal(int signum)
{
  if (mon)
    mon->handle_signal(signum);
}


int obtain_monmap(MonitorDBStore &store, bufferlist &bl)
{
  dout(10) << __func__ << dendl;
  /*
   * the monmap may be in one of three places:
   *  'monmap:<latest_version_no>' - the monmap we'd really like to have
   *  'mon_sync:latest_monmap'     - last monmap backed up for the last sync
   *  'mkfs:monmap'                - a monmap resulting from mkfs
   */

  if (store.exists("monmap", "last_committed")) {
    version_t latest_ver = store.get("monmap", "last_committed");
    if (store.exists("monmap", latest_ver)) {
      int err = store.get("monmap", latest_ver, bl);
      assert(err == 0);
      assert(bl.length() > 0);
      dout(10) << __func__ << " read last committed monmap ver "
               << latest_ver << dendl;
      return 0;
    }
  }

  if (store.exists("mon_sync", "in_sync")
      || store.exists("mon_sync", "force_sync")) {
    dout(10) << __func__ << " detected aborted sync" << dendl;
    if (store.exists("mon_sync", "latest_monmap")) {
      int err = store.get("mon_sync", "latest_monmap", bl);
      assert(err == 0);
      assert(bl.length() > 0);
      dout(10) << __func__ << " read backup monmap" << dendl;
      return 0;
    }
  }

  if (store.exists("mkfs", "monmap")) {
    dout(10) << __func__ << " found mkfs monmap" << dendl;
    int err = store.get("mkfs", "monmap", bl);
    assert(err == 0);
    assert(bl.length() > 0);
    return 0;
  }

  derr << __func__ << " unable to find a monmap" << dendl;
  return -ENOENT;
}

int check_mon_data_exists()
{
  string mon_data = g_conf->mon_data;
  struct stat buf;
  if (::stat(mon_data.c_str(), &buf)) {//系统函数，看名字为mon_data.c_str()的文件是否存在
    if (errno != ENOENT) {
      cerr << "stat(" << mon_data << ") " << cpp_strerror(errno) << std::endl;
    }
    return -errno;
  }
  return 0;
}

/** Check whether **mon data** is empty.
 *
 * Being empty means mkfs has not been run and there's no monitor setup
 * at **g_conf->mon_data**.   注意其含义
 *
 * If the directory g_conf->mon_data is not empty we will return -ENOTEMPTY.
 * Otherwise we will return 0.  Any other negative returns will represent
 * a failure to be handled by the caller.
 *
 * @return **0** on success, -ENOTEMPTY if not empty or **-errno** otherwise.
 */
int check_mon_data_empty()
{
  string mon_data = g_conf->mon_data;

  DIR *dir = ::opendir(mon_data.c_str());
  if (!dir) {
    cerr << "opendir(" << mon_data << ") " << cpp_strerror(errno) << std::endl;
    return -errno;
  }
  char buf[offsetof(struct dirent, d_name) + PATH_MAX + 1];

  int code = 0;
  struct dirent *de;
  errno = 0;
  while (!::readdir_r(dir, reinterpret_cast<struct dirent*>(buf), &de)) {
    if (!de) {
      if (errno) {
	cerr << "readdir(" << mon_data << ") " << cpp_strerror(errno) << std::endl;
	code = -errno;
      }
      break;
    }
    if (string(".") != de->d_name &&
	string("..") != de->d_name) {
      code = -ENOTEMPTY;
      break;
    }
  }

  ::closedir(dir);

  return code;
}

void usage()
{
  cerr << "usage: ceph-mon -i monid [flags]" << std::endl;
  cerr << "  --debug_mon n\n";
  cerr << "        debug monitor level (e.g. 10)\n";
  cerr << "  --mkfs\n";
  cerr << "        build fresh monitor fs\n";
  cerr << "  --force-sync\n";
  cerr << "        force a sync from another mon by wiping local data (BE CAREFUL)\n";
  cerr << "  --yes-i-really-mean-it\n";
  cerr << "        mandatory safeguard for --force-sync\n";
  cerr << "  --compact\n";
  cerr << "        compact the monitor store\n";
  cerr << "  --osdmap <filename>\n";
  cerr << "        only used when --mkfs is provided: load the osdmap from <filename>\n";
  cerr << "  --inject-monmap <filename>\n";
  cerr << "        write the <filename> monmap to the local monitor store and exit\n";
  cerr << "  --extract-monmap <filename>\n";
  cerr << "        extract the monmap from the local monitor store and exit\n";
  cerr << "  --mon-data <directory>\n";
  cerr << "        where the mon store and keyring are located\n";
  generic_server_usage();
}

int preload_erasure_code()
{
  string directory = g_conf->osd_pool_default_erasure_code_directory;
  string plugins = g_conf->osd_erasure_code_plugins;
  stringstream ss;
  int r = ErasureCodePluginRegistry::instance().preload(plugins,
							directory,
							&ss);
  if (r)
    derr << ss.str() << dendl;
  else
    dout(10) << ss.str() << dendl;
  return r;
}

int main(int argc, const char **argv) 
{
  int err;

  bool mkfs = false;
  bool compact = false;
  bool force_sync = false;
  bool yes_really = false;
  std::string osdmapfn, inject_monmap, extract_monmap;

  vector<const char*> args;
  argv_to_vec(argc, argv, args);
  env_to_vec(args);

  // We need to specify some default values that may be overridden by the
  // user, that are specific to the monitor.  The options we are overriding
  // are also used on the OSD (or in any other component that uses leveldb),
  // so changing them directly in common/config_opts.h is not an option.
  // This is not the prettiest way of doing this, especially since it has us
  // having a different place than common/config_opts.h defining default
  // values, but it's not horribly wrong enough to prevent us from doing it :)
  //
  // NOTE: user-defined options will take precedence over ours.
  //
  //  leveldb_write_buffer_size = 32*1024*1024  = 33554432  // 32MB
  //  leveldb_cache_size        = 512*1024*1204 = 536870912 // 512MB
  //  leveldb_block_size        = 64*1024       = 65536     // 64KB
  //  leveldb_compression       = false
  //  leveldb_log               = ""
  vector<const char*> def_args;
  def_args.push_back("--leveldb-write-buffer-size=33554432");
  def_args.push_back("--leveldb-cache-size=536870912");
  def_args.push_back("--leveldb-block-size=65536");
  def_args.push_back("--leveldb-compression=false");
  def_args.push_back("--leveldb-log=");

  int flags = 0;
  {
    vector<const char*> args_copy = args;
    std::string val;
    for (std::vector<const char*>::iterator i = args_copy.begin();
	 i != args_copy.end(); ) {
      if (ceph_argparse_double_dash(args_copy, i)) {
	break;
      } else if (ceph_argparse_flag(args_copy, i, "--mkfs", (char*)NULL)) {//mkfs, inject, extract，都不是正常的daemon运行，属于单次命令执行
	flags |= CINIT_FLAG_NO_DAEMON_ACTIONS;
      } else if (ceph_argparse_witharg(args_copy, i, &val, "--inject_monmap", (char*)NULL)) {
	flags |= CINIT_FLAG_NO_DAEMON_ACTIONS;
      } else if (ceph_argparse_witharg(args_copy, i, &val, "--extract-monmap", (char*)NULL)) {
	flags |= CINIT_FLAG_NO_DAEMON_ACTIONS;
      } else {
	++i;
      }
    }
  }

  global_init(&def_args, args,
              CEPH_ENTITY_TYPE_MON, CODE_ENVIRONMENT_DAEMON, flags);
  ceph_heap_profiler_init();

  uuid_d fsid;
  std::string val;
  for (std::vector<const char*>::iterator i = args.begin(); i != args.end(); ) {
    if (ceph_argparse_double_dash(args, i)) {
      break;
    } else if (ceph_argparse_flag(args, i, "-h", "--help", (char*)NULL)) {
      usage();
      exit(0);
    } else if (ceph_argparse_flag(args, i, "--mkfs", (char*)NULL)) {
      mkfs = true;
    } else if (ceph_argparse_flag(args, i, "--compact", (char*)NULL)) {
      compact = true;
    } else if (ceph_argparse_flag(args, i, "--force-sync", (char*)NULL)) {
      force_sync = true;
    } else if (ceph_argparse_flag(args, i, "--yes-i-really-mean-it", (char*)NULL)) {
      yes_really = true;
    } else if (ceph_argparse_witharg(args, i, &val, "--osdmap", (char*)NULL)) {
      osdmapfn = val;
    } else if (ceph_argparse_witharg(args, i, &val, "--inject_monmap", (char*)NULL)) {
      inject_monmap = val;
    } else if (ceph_argparse_witharg(args, i, &val, "--extract-monmap", (char*)NULL)) {
      extract_monmap = val;
    } else {
      ++i;
    }
  }
  if (!args.empty()) {
    cerr << "too many arguments: " << args << std::endl;
    usage();
  }

  if (force_sync && !yes_really) {
    cerr << "are you SURE you want to force a sync?  this will erase local data and may\n"
	 << "break your mon cluster.  pass --yes-i-really-mean-it if you do." << std::endl;
    exit(1);
  }

  if (g_conf->mon_data.empty()) {
    cerr << "must specify '--mon-data=foo' data path" << std::endl;
    usage();
  }

  if (g_conf->name.get_id().empty()) {
    cerr << "must specify id (--id <id> or --name mon.<id>)" << std::endl;
    usage();
  }

  // -- mkfs --
  if (mkfs) {//按照http://docs.ceph.com/docs/v0.94/install/manual-deployment/的解释，这个要调用Monitor::mkfs。
//而不是实际运行daemon
    int err = check_mon_data_exists();
    if (err == -ENOENT) {
      if (::mkdir(g_conf->mon_data.c_str(), 0755)) {
	cerr << "mkdir(" << g_conf->mon_data << ") : "
	     << cpp_strerror(errno) << std::endl;
	exit(1);
      }
    } else if (err < 0) {
      cerr << "error opening '" << g_conf->mon_data << "': "
           << cpp_strerror(-err) << std::endl;
      exit(-err);
    }

    err = check_mon_data_empty();//检查 mon data 目录是否为空，以确认是否运行过mkfs.
    if (err == -ENOTEMPTY) {//以前运行过mkfs，不能再运行
      // Mon may exist.  Let the user know and exit gracefully.
      cerr << "'" << g_conf->mon_data << "' already exists and is not empty"
           << ": monitor may already exist" << std::endl;
      exit(0);
    } else if (err < 0) {//出现了其他错误
      cerr << "error checking if '" << g_conf->mon_data << "' is empty: "
           << cpp_strerror(-err) << std::endl;
      exit(-err);
    }

    // resolve public_network -> public_addr
    pick_addresses(g_ceph_context, CEPH_PICK_ADDRESS_PUBLIC);

    common_init_finish(g_ceph_context, flags);

    bufferlist monmapbl, osdmapbl;
    std::string error;
    MonMap monmap;

    // load or generate monmap
    if (g_conf->monmap.length()) {//相当于从配置文件获取？
      int err = monmapbl.read_file(g_conf->monmap.c_str(), &error);//读配置文件
      if (err < 0) {
	cerr << argv[0] << ": error reading " << g_conf->monmap << ": " << error << std::endl;
	exit(1);
      }
      try {
	monmap.decode(monmapbl);

	// always mark seed/mkfs monmap as epoch 0 //啥意思？
	monmap.set_epoch(0);//因为这个是刚刚mkfs，所以epoch设置为0
      }
      catch (const buffer::error& e) {
	cerr << argv[0] << ": error decoding monmap " << g_conf->monmap << ": " << e.what() << std::endl;
	exit(1);
      }      
    } else {//根据下面的配置，是没有配置monitor?
      int err = monmap.build_initial(g_ceph_context, cerr);//我感觉这个才是读文件
      if (err < 0) {
	cerr << argv[0] << ": warning: no initial monitors; must use admin socket to feed hints" << std::endl;
      }

      // am i part of the initial quorum?
      if (monmap.contains(g_conf->name.get_id())) {
	// hmm, make sure the ip listed exists on the current host?
	// maybe later.
      } else if (!g_conf->public_addr.is_blank_ip()) {
	entity_addr_t a = g_conf->public_addr;
	if (a.get_port() == 0)
	  a.set_port(CEPH_MON_PORT);
	if (monmap.contains(a)) {
	  string name;
	  monmap.get_addr_name(a, name);
	  monmap.rename(name, g_conf->name.get_id());
	  cout << argv[0] << ": renaming mon." << name << " " << a
	       << " to mon." << g_conf->name.get_id() << std::endl;
	}
      } else {
	// is a local address listed without a name?  if so, name myself.
	list<entity_addr_t> ls;
	monmap.list_addrs(ls);
	entity_addr_t local;

	if (have_local_addr(g_ceph_context, ls, &local)) {
	  string name;
	  monmap.get_addr_name(local, name);

	  if (name.find("noname-") == 0) {//从noname-xxx，修改为mon.id
	    cout << argv[0] << ": mon." << name << " " << local
		 << " is local, renaming to mon." << g_conf->name.get_id() << std::endl;
	    monmap.rename(name, g_conf->name.get_id());
	  } else {
	    cout << argv[0] << ": mon." << name << " " << local
		 << " is local, but not 'noname-' + something; not assuming it's me" << std::endl;
	  }
	}
      }
    }

    if (!g_conf->fsid.is_zero()) {
      monmap.fsid = g_conf->fsid;
      cout << argv[0] << ": set fsid to " << g_conf->fsid << std::endl;
    }
    
    if (monmap.fsid.is_zero()) {
      cerr << argv[0] << ": generated monmap has no fsid; use '--fsid <uuid>'" << std::endl;
      exit(10);
    }

    //monmap.print(cout);

    // osdmap
    if (osdmapfn.length()) {//读取osdmap文件
      err = osdmapbl.read_file(osdmapfn.c_str(), &error);
      if (err < 0) {
	cerr << argv[0] << ": error reading " << osdmapfn << ": "
	     << error << std::endl;
	exit(1);
      }
    }

    // go
    MonitorDBStore store(g_conf->mon_data);
    int r = store.create_and_open(cerr);
    if (r < 0) {
      cerr << argv[0] << ": error opening mon data directory at '"
           << g_conf->mon_data << "': " << cpp_strerror(r) << std::endl;
      exit(1);
    }
    assert(r == 0);

    Monitor mon(g_ceph_context, g_conf->name.get_id(), &store, 0, &monmap);
    r = mon.mkfs(osdmapbl);
    if (r < 0) {
      cerr << argv[0] << ": error creating monfs: " << cpp_strerror(r) << std::endl;
      exit(1);
    }
    store.close();
    cout << argv[0] << ": created monfs at " << g_conf->mon_data 
	 << " for " << g_conf->name << std::endl;
    return 0;
  }

  err = check_mon_data_exists();
  if (err < 0 && err == -ENOENT) {//啰嗦
    cerr << "monitor data directory at '" << g_conf->mon_data << "'"
         << " does not exist: have you run 'mkfs'?" << std::endl;
    exit(1);
  } else if (err < 0) {
    cerr << "error accessing monitor data directory at '"
         << g_conf->mon_data << "': " << cpp_strerror(-err) << std::endl;
    exit(1);
  }

  err = check_mon_data_empty();//走到这个位置，就不是mkfs，认为mon_data这些内容都配置好了，直接读取。
  if (err == 0) {
    derr << "monitor data directory at '" << g_conf->mon_data
      << "' is empty: have you run 'mkfs'?" << dendl;
    exit(1);
  } else if (err < 0 && err != -ENOTEMPTY) {
    // we don't want an empty data dir by now
    cerr << "error accessing '" << g_conf->mon_data << "': "
         << cpp_strerror(-err) << std::endl;
    exit(1);
  }

  {
    // check fs stats. don't start if it's critically close to full.
    ceph_data_stats_t stats;
    int err = get_fs_stats(stats, g_conf->mon_data.c_str());//调用系统调用statfs，获取对应文件系统的空间使用情况
    if (err < 0) {
      cerr << "error checking monitor data's fs stats: " << cpp_strerror(err)
           << std::endl;
      exit(-err);
    }
    if (stats.avail_percent <= g_conf->mon_data_avail_crit) {
      cerr << "error: monitor data filesystem reached concerning levels of"
           << " available storage space (available: "
           << stats.avail_percent << "% " << prettybyte_t(stats.byte_avail)
           << ")\nyou may adjust 'mon data avail crit' to a lower value"
           << " to make this go away (default: " << g_conf->mon_data_avail_crit
           << "%)\n" << std::endl;
      exit(ENOSPC);
    }
  }

  // we fork early to prevent leveldb's environment static state from
  // screwing us over
  Preforker prefork;
  if (!(flags & CINIT_FLAG_NO_DAEMON_ACTIONS)) {//需要变成daemon
    if (global_init_prefork(g_ceph_context, 0) >= 0) {//这个是成功了，实际上不会  >0，只会 ==0
      string err_msg;
      err = prefork.prefork(err_msg);//为什么要做个fork? 上面解释了leveldb的环境变量覆盖问题
      if (err < 0) {
        cerr << err_msg << std::endl;
        prefork.exit(err);
      }
      if (prefork.is_parent()) {//父进程
        err = prefork.parent_wait(err_msg);//这个函数内部，已经等待子进程写东西才往下走的
        if (err < 0)
          cerr << err_msg << std::endl;
        prefork.exit(err);//父进程到此就退出去了？ 目的是什么？
      }
      global_init_postfork_start(g_ceph_context);
    }
    common_init_finish(g_ceph_context); //这个是子进程都执行？ 感觉做了不少活，还启动了线程
    global_init_chdir(g_ceph_context);
    if (preload_erasure_code() < 0)
      prefork.exit(1);
  }

  MonitorDBStore *store = new MonitorDBStore(g_conf->mon_data);
  err = store->open(std::cerr);
  if (err < 0) {
    derr << "error opening mon data directory at '"
         << g_conf->mon_data << "': " << cpp_strerror(err) << dendl;
    prefork.exit(1);
  }

  bufferlist magicbl;
  err = store->get(Monitor::MONITOR_NAME, "magic", magicbl);
  if (!magicbl.length()) {
    derr << "unable to read magic from mon data" << dendl;
    prefork.exit(1);
  }
  string magic(magicbl.c_str(), magicbl.length()-1);  // ignore trailing \n
  if (strcmp(magic.c_str(), CEPH_MON_ONDISK_MAGIC)) {
    derr << "mon fs magic '" << magic << "' != current '" << CEPH_MON_ONDISK_MAGIC << "'" << dendl;
    prefork.exit(1);
  }

  err = Monitor::check_features(store);
  if (err < 0) {
    derr << "error checking features: " << cpp_strerror(err) << dendl;
    prefork.exit(1);
  }

  // inject new monmap?
  if (!inject_monmap.empty()) {//inject也是一次性操作，非daemon。把指定文件的monmap追加进store即可，同时修改last_committed
    bufferlist bl;
    std::string error;
    int r = bl.read_file(inject_monmap.c_str(), &error);//内容在文件里面, inject_monmap是文件名
    if (r) {
      derr << "unable to read monmap from " << inject_monmap << ": "
	   << error << dendl;
      prefork.exit(1);
    }

    // get next version
    version_t v = store->get("monmap", "last_committed");
    dout(0) << "last committed monmap epoch is " << v << ", injected map will be " << (v+1)
            << dendl;
    v++;//修改了v，因为做了inject，不能是同一个epoch了

    // set the version
    MonMap tmp;
    tmp.decode(bl);
    if (tmp.get_epoch() != v) {
      dout(0) << "changing monmap epoch from " << tmp.get_epoch() //跟别的node怎么沟通v的改变？基于信任原则？非拜赞庭错误？
           << " to " << v << dendl;
      tmp.set_epoch(v);
    }
    bufferlist mapbl;
    tmp.encode(mapbl, CEPH_FEATURES_ALL);
    bufferlist final;
    ::encode(v, final);
    ::encode(mapbl, final);//v和mapbl都encode进入了final，含义呢？

    MonitorDBStore::TransactionRef t(new MonitorDBStore::Transaction);
    // save it
    t->put("monmap", v, mapbl);//对应epoch v的map数据
    t->put("monmap", "latest", final);
    t->put("monmap", "last_committed", v);
    store->apply_transaction(t);//这些作为一个transaction来执行

    dout(0) << "done." << dendl;
    prefork.exit(0); //退出了哪个进程？
  }

  // monmap?
  MonMap monmap;
  {
    // note that even if we don't find a viable monmap, we should go ahead
    // and try to build it up in the next if-else block.
    bufferlist mapbl;
    int err = obtain_monmap(*store, mapbl);
    if (err >= 0) {
      try {
        monmap.decode(mapbl);
      } catch (const buffer::error& e) {
        cerr << "can't decode monmap: " << e.what() << std::endl;
      }
    } else {
      derr << "unable to obtain a monmap: " << cpp_strerror(err) << dendl;
    }
    if (!extract_monmap.empty()) {//extract_monmap 就是文件名，是从命令行输入赋值的，表示要把map extract到该文件中。
      int r = mapbl.write_file(extract_monmap.c_str());
      if (r < 0) {
	r = -errno;
	derr << "error writing monmap to " << extract_monmap << ": " << cpp_strerror(r) << dendl;
	prefork.exit(1);
      }
      derr << "wrote monmap to " << extract_monmap << dendl;
      prefork.exit(0);
    }
  }

  // this is what i will bind to
  entity_addr_t ipaddr;
//下面一大段，就是获取和确认自己该bind的addr
  if (monmap.contains(g_conf->name.get_id())) {
    ipaddr = monmap.get_addr(g_conf->name.get_id());

    // print helpful warning if the conf file doesn't match
    entity_addr_t conf_addr;
    std::vector <std::string> my_sections;
    g_conf->get_my_sections(my_sections);
    std::string mon_addr_str;
    if (g_conf->get_val_from_conf_file(my_sections, "mon addr",
				       mon_addr_str, true) == 0) {//从这个里面再取出addr，check一致性
      if (conf_addr.parse(mon_addr_str.c_str()) && (ipaddr != conf_addr)) {
	derr << "WARNING: 'mon addr' config option " << conf_addr
	     << " does not match monmap file" << std::endl
	     << "         continuing with monmap configuration" << dendl;
      }
    }
  } else {//TODO: 不在monmap里面，join到底啥影响 ? 信任原则应该非常重要，才能保持运转
    dout(0) << g_conf->name << " does not exist in monmap, will attempt to join an existing cluster" << dendl;

    pick_addresses(g_ceph_context, CEPH_PICK_ADDRESS_PUBLIC);
    if (!g_conf->public_addr.is_blank_ip()) {
      ipaddr = g_conf->public_addr;
      if (ipaddr.get_port() == 0)
	ipaddr.set_port(CEPH_MON_PORT);
      dout(0) << "using public_addr " << g_conf->public_addr << " -> "
	      << ipaddr << dendl;
    } else {//空ip
      MonMap tmpmap;
      int err = tmpmap.build_initial(g_ceph_context, cerr);
      if (err < 0) {
	derr << argv[0] << ": error generating initial monmap: "
             << cpp_strerror(err) << dendl;
	usage();
	prefork.exit(1);
      }
      if (tmpmap.contains(g_conf->name.get_id())) {//跟前面很像，感觉代码写得啰嗦
	ipaddr = tmpmap.get_addr(g_conf->name.get_id());
      } else {
	derr << "no public_addr or public_network specified, and " << g_conf->name
	     << " not present in monmap or ceph.conf" << dendl;
	prefork.exit(1);
      }
    }
  }

  // bind
  int rank = monmap.get_rank(g_conf->name.get_id());
  Messenger *msgr = Messenger::create(g_ceph_context, g_conf->ms_type,
				      entity_name_t::MON(rank),
				      "mon",
				      0);
  msgr->set_cluster_protocol(CEPH_MON_PROTOCOL);
  msgr->set_default_send_priority(CEPH_MSG_PRIO_HIGH);

  uint64_t supported =
    CEPH_FEATURE_UID |
    CEPH_FEATURE_NOSRCADDR |
    CEPH_FEATURE_MONCLOCKCHECK |
    CEPH_FEATURE_PGID64 |
    CEPH_FEATURE_MSG_AUTH;
  msgr->set_default_policy(Messenger::Policy::stateless_server(supported, 0));
  msgr->set_policy(entity_name_t::TYPE_MON,
                   Messenger::Policy::lossless_peer_reuse(
                       supported,
                       CEPH_FEATURE_UID |
                       CEPH_FEATURE_PGID64 |
                       CEPH_FEATURE_MON_SINGLE_PAXOS));
  msgr->set_policy(entity_name_t::TYPE_OSD,
                   Messenger::Policy::stateless_server(
                       supported,
                       CEPH_FEATURE_PGID64 |
                       CEPH_FEATURE_OSDENC));
  msgr->set_policy(entity_name_t::TYPE_CLIENT,
                   Messenger::Policy::stateless_server(supported, 0));
  msgr->set_policy(entity_name_t::TYPE_MDS,
                   Messenger::Policy::stateless_server(supported, 0));

  // throttle client traffic
  Throttle *client_throttler = new Throttle(g_ceph_context, "mon_client_bytes",
					    g_conf->mon_client_bytes);
  msgr->set_policy_throttlers(entity_name_t::TYPE_CLIENT,
				     client_throttler, NULL);

  // throttle daemon traffic
  // NOTE: actual usage on the leader may multiply by the number of
  // monitors if they forward large update messages from daemons.
  Throttle *daemon_throttler = new Throttle(g_ceph_context, "mon_daemon_bytes",
					    g_conf->mon_daemon_bytes);
  msgr->set_policy_throttlers(entity_name_t::TYPE_OSD, daemon_throttler,
				     NULL);
  msgr->set_policy_throttlers(entity_name_t::TYPE_MDS, daemon_throttler,
				     NULL);

  dout(0) << "starting " << g_conf->name << " rank " << rank
       << " at " << ipaddr
       << " mon_data " << g_conf->mon_data
       << " fsid " << monmap.get_fsid()
       << dendl;

  err = msgr->bind(ipaddr);//前面获取ipaddr，比较费周折
  if (err < 0) {
    derr << "unable to bind monitor to " << ipaddr << dendl;
    prefork.exit(1);
  }

  cout << "starting " << g_conf->name << " rank " << rank
       << " at " << ipaddr
       << " mon_data " << g_conf->mon_data
       << " fsid " << monmap.get_fsid()
       << std::endl;

  // start monitor
  mon = new Monitor(g_ceph_context, g_conf->name.get_id(), store,
		    msgr, &monmap);

  if (force_sync) {
    derr << "flagging a forced sync ..." << dendl;
    mon->sync_force(NULL, cerr);
  }

  err = mon->preinit();//假设配置预先做好，各个monitor先后启动，过程是什么样？ 大家如何让monmap本身达到一致？ 还是过程中会随着各个mon的启动而变化？
  if (err < 0) {
    derr << "failed to initialize" << dendl;
    prefork.exit(1);
  }//preinit里面初始化了paxos，注册了admin cmd，按理到这里就可以执行admin cmd了。

  if (compact || g_conf->mon_compact_on_start) {
    derr << "compacting monitor store ..." << dendl;
    mon->store->compact();
    derr << "done compacting" << dendl;
  }

  if (g_conf->daemonize) {
    global_init_postfork_finish(g_ceph_context, 0);
    prefork.daemonize();
  }

  msgr->start();//已经开始接收消息了? 

  mon->init();

  // set up signal handlers, now that we've daemonized/forked.
  init_async_signal_handler(); //里面实现其实比较复杂，搞了个线程handle signal
  register_async_signal_handler(SIGHUP, sighup_handler);
  register_async_signal_handler_oneshot(SIGINT, handle_mon_signal);
  register_async_signal_handler_oneshot(SIGTERM, handle_mon_signal);

  if (g_conf->inject_early_sigterm)
    kill(getpid(), SIGTERM);

  msgr->wait();//这个函数应该会让主线程等待，让msgr去干活。参见asyncmessenger.wait()

  store->close();

  unregister_async_signal_handler(SIGHUP, sighup_handler);
  unregister_async_signal_handler(SIGINT, handle_mon_signal);
  unregister_async_signal_handler(SIGTERM, handle_mon_signal);
  shutdown_async_signal_handler();

  delete mon;
  delete store;
  delete msgr;
  delete client_throttler;
  delete daemon_throttler;
  g_ceph_context->put();

  // cd on exit, so that gmon.out (if any) goes into a separate directory for each node.
  char s[20];
  snprintf(s, sizeof(s), "gmon/%d", getpid());
  if ((mkdir(s, 0755) == 0) && (chdir(s) == 0)) {
    dout(0) << "ceph-mon: gmon.out should be in " << s << dendl;
  }

  prefork.signal_exit(0);
  return 0;
}

