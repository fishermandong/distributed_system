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
#include "Paxos.h"
#include "Monitor.h"
#include "MonitorDBStore.h"

#include "messages/MMonPaxos.h"

#include "common/config.h"
#include "include/assert.h"
#include "include/stringify.h"
#include "common/Formatter.h"

#define dout_subsys ceph_subsys_paxos
#undef dout_prefix
#define dout_prefix _prefix(_dout, mon, mon->name, mon->rank, paxos_name, state, first_committed, last_committed)
static ostream& _prefix(std::ostream *_dout, Monitor *mon, const string& name,
		        int rank, const string& paxos_name, int state,
			version_t first_committed, version_t last_committed)
{
  return *_dout << "mon." << name << "@" << rank
		<< "(" << mon->get_state_name() << ")"
		<< ".paxos(" << paxos_name << " " << Paxos::get_statename(state)
		<< " c " << first_committed << ".." << last_committed
		<< ") ";
}

MonitorDBStore *Paxos::get_store()
{
  return mon->store;
}

void Paxos::read_and_prepare_transactions(MonitorDBStore::TransactionRef tx,
					  version_t first, version_t last)
{
  dout(10) << __func__ << " first " << first << " last " << last << dendl;
  for (version_t v = first; v <= last; ++v) {
    dout(30) << __func__ << " apply version " << v << dendl;
    bufferlist bl;
    int err = get_store()->get(get_name(), v, bl);
    assert(err == 0);
    assert(bl.length());
    decode_append_transaction(tx, bl);
  }
  dout(15) << __func__ << " total versions " << (last-first) << dendl;
}

void Paxos::init()
{
  // load paxos variables from stable storage
  last_pn = get_store()->get(get_name(), "last_pn");
  accepted_pn = get_store()->get(get_name(), "accepted_pn");
  last_committed = get_store()->get(get_name(), "last_committed");
  first_committed = get_store()->get(get_name(), "first_committed");//这个是 paxos的 first_committed，并不是某个monitor的first_committed，各个monitor对应值可能都是不一样的。

  dout(10) << __func__ << " last_pn: " << last_pn << " accepted_pn: "
	   << accepted_pn << " last_committed: " << last_committed
	   << " first_committed: " << first_committed << dendl;

  dout(10) << "init" << dendl;
  assert(is_consistent());
}

void Paxos::init_logger()
{
  PerfCountersBuilder pcb(g_ceph_context, "paxos", l_paxos_first, l_paxos_last);
  pcb.add_u64_counter(l_paxos_start_leader, "start_leader", "Starts in leader role");
  pcb.add_u64_counter(l_paxos_start_peon, "start_peon", "Starts in peon role");
  pcb.add_u64_counter(l_paxos_restart, "restart", "Restarts");
  pcb.add_u64_counter(l_paxos_refresh, "refresh", "Refreshes");
  pcb.add_time_avg(l_paxos_refresh_latency, "refresh_latency", "Refresh latency");
  pcb.add_u64_counter(l_paxos_begin, "begin", "Started and handled begins");
  pcb.add_u64_avg(l_paxos_begin_keys, "begin_keys", "Keys in transaction on begin");
  pcb.add_u64_avg(l_paxos_begin_bytes, "begin_bytes", "Data in transaction on begin");
  pcb.add_time_avg(l_paxos_begin_latency, "begin_latency", "Latency of begin operation");
  pcb.add_u64_counter(l_paxos_commit, "commit",
      "Commits", "cmt");
  pcb.add_u64_avg(l_paxos_commit_keys, "commit_keys", "Keys in transaction on commit");
  pcb.add_u64_avg(l_paxos_commit_bytes, "commit_bytes", "Data in transaction on commit");
  pcb.add_time_avg(l_paxos_commit_latency, "commit_latency",
      "Commit latency", "clat");
  pcb.add_u64_counter(l_paxos_collect, "collect", "Peon collects");
  pcb.add_u64_avg(l_paxos_collect_keys, "collect_keys", "Keys in transaction on peon collect");
  pcb.add_u64_avg(l_paxos_collect_bytes, "collect_bytes", "Data in transaction on peon collect");
  pcb.add_time_avg(l_paxos_collect_latency, "collect_latency", "Peon collect latency");
  pcb.add_u64_counter(l_paxos_collect_uncommitted, "collect_uncommitted", "Uncommitted values in started and handled collects");
  pcb.add_u64_counter(l_paxos_collect_timeout, "collect_timeout", "Collect timeouts");
  pcb.add_u64_counter(l_paxos_accept_timeout, "accept_timeout", "Accept timeouts");
  pcb.add_u64_counter(l_paxos_lease_ack_timeout, "lease_ack_timeout", "Lease acknowledgement timeouts");
  pcb.add_u64_counter(l_paxos_lease_timeout, "lease_timeout", "Lease timeouts");
  pcb.add_u64_counter(l_paxos_store_state, "store_state", "Store a shared state on disk");
  pcb.add_u64_avg(l_paxos_store_state_keys, "store_state_keys", "Keys in transaction in stored state");
  pcb.add_u64_avg(l_paxos_store_state_bytes, "store_state_bytes", "Data in transaction in stored state");
  pcb.add_time_avg(l_paxos_store_state_latency, "store_state_latency", "Storing state latency");
  pcb.add_u64_counter(l_paxos_share_state, "share_state", "Sharings of state");
  pcb.add_u64_avg(l_paxos_share_state_keys, "share_state_keys", "Keys in shared state");
  pcb.add_u64_avg(l_paxos_share_state_bytes, "share_state_bytes", "Data in shared state");
  pcb.add_u64_counter(l_paxos_new_pn, "new_pn", "New proposal number queries");
  pcb.add_time_avg(l_paxos_new_pn_latency, "new_pn_latency", "New proposal number getting latency");
  logger = pcb.create_perf_counters();
  g_ceph_context->get_perfcounters_collection()->add(logger);
}

void Paxos::dump_info(Formatter *f)
{
  f->open_object_section("paxos");
  f->dump_unsigned("first_committed", first_committed);
  f->dump_unsigned("last_committed", last_committed);
  f->dump_unsigned("last_pn", last_pn);
  f->dump_unsigned("accepted_pn", accepted_pn);
  f->close_section();
}
// ---------------------------------
//我理解，这个应该对应made simple一书中，“The new leader, being a learner in all instances of the consensus algorithm, should know most of the commands”一段。只不过本实现的窗口就是1而已。
//不管如何，既然有窗口存在，执行phase 1就有可能出现那个待定的已经被以前某个proposer执行了phase 2，所以在如果handle_last发现返回的pn比自己大，那么用新的pn (In phase 1, an acceptor responds with more than a simple OK only if it has already received a phase 2 message from some proposer)
// PHASE 1
//这里明确写了phase 1，那么整个leader当选期间，其都是同一个proposal number? 或者说，一个proposal被采纳后，一系列的begin( phase 2?)
// leader   这个是leader_init调用的，即leader刚当选后调用
void Paxos::collect(version_t oldpn)
{
  // we're recoverying, it seems!
  state = STATE_RECOVERING;
  assert(mon->is_leader());

  // reset the number of lasts received
  uncommitted_v = 0;
  uncommitted_pn = 0;
  uncommitted_value.clear();//先clear
  peer_first_committed.clear();
  peer_last_committed.clear();

  // look for uncommitted value
  if (get_store()->exists(get_name(), last_committed+1)) {//用的是 last_commited+1，认为uncommitted_v只能比last_committed大1。因为任何时刻，只允许一个pending的
    version_t v = get_store()->get(get_name(), "pending_v");//上一行判断的exists，跟这个v，什么关系？ 上面一行，就是逝卸锨否存在某个版本作为key的值，不存在则没必要进来。而获取的v，代表的是pending_v这个key内容
    version_t pn = get_store()->get(get_name(), "pending_pn"); //参见Paxos::begin等函数中put "pending_v"等的操作
    if (v && pn && v == last_committed + 1) {//dhq: pending_v和pending_pn存在，并且pending_v ==last_committed+1，这样才认为有效。否则认为有问题。
      uncommitted_pn = pn;
    } else {
      dout(10) << "WARNING: no pending_pn on disk, using previous accepted_pn " << accepted_pn
	       << " and crossing our fingers" << dendl;//cross our fingers，词典翻译为: 祈祷
      uncommitted_pn = accepted_pn;
    }
    uncommitted_v = last_committed+1;// 上面if已经表示这个含义

    get_store()->get(get_name(), last_committed+1, uncommitted_value);//找到uncommitted_v对应的value
    assert(uncommitted_value.length()); //因为uncommitted_v存在，要求uncommitted_value必须存在。
    dout(10) << "learned uncommitted " << (last_committed+1)
	     << " pn " << uncommitted_pn
	     << " (" << uncommitted_value.length() << " bytes) from myself" 
	     << dendl;

    logger->inc(l_paxos_collect_uncommitted);
  }
  //为什么在begin()里面，不调用get_new_proposal_number，不修改accepted_pn?  我理解只是在leader election期间需要走标准流程，后面就是有leader的简单处理了，无需竞争。
  //"made simple"一文中：It then executes phase 1 of instances 135–137 and of all instances greater than 139. 即对上届未完成的和后续所有各个instance，都执行PN为accepted_pn的phase操作。
  // pick new pn  
  accepted_pn = get_new_proposal_number(MAX(accepted_pn, oldpn));//自己先接受了这个pn? 
  accepted_pn_from = last_committed;
  num_last = 1;
  dout(10) << "collect with pn " << accepted_pn << dendl;

  // send collect
  for (set<int>::const_iterator p = mon->get_quorum().begin();
       p != mon->get_quorum().end();
       ++p) {
    if (*p == mon->rank) continue;
    
    MMonPaxos *collect = new MMonPaxos(mon->get_epoch(), MMonPaxos::OP_COLLECT,
				       ceph_clock_now(g_ceph_context));//epoch的用意？ 如果别人已经发起了选举，那么发现本epoch不对，从而不响应？
    collect->last_committed = last_committed;
    collect->first_committed = first_committed;
    collect->pn = accepted_pn; //这个操作本身带的pn是刚生成的，相当于一个term的含义。
    mon->messenger->send_message(collect, mon->monmap->get_inst(*p));
  }

  // set timeout event
  collect_timeout_event = new C_CollectTimeout(this);
  mon->timer.add_event_after(g_conf->mon_accept_timeout, collect_timeout_event);
}


// peon
void Paxos::handle_collect(MonOpRequestRef op)
{
  op->mark_paxos_event("handle_collect");

  MMonPaxos *collect = static_cast<MMonPaxos*>(op->get_req());
  dout(10) << "handle_collect " << *collect << dendl;

  assert(mon->is_peon()); // mon epoch filter should catch strays
  //如果node收到了消息，并且认为自己是leader，那么就会assert报错了。
  // we're recoverying, it seems!
  state = STATE_RECOVERING;

  if (collect->first_committed > last_committed+1) {//我落后的太远，所以需要bootstrap
    dout(5) << __func__
            << " leader's lowest version is too high for our last committed"
            << " (theirs: " << collect->first_committed
            << "; ours: " << last_committed << ") -- bootstrap!" << dendl;
    op->mark_paxos_event("need to bootstrap");
    mon->bootstrap();
    return;
  }

  // reply
  MMonPaxos *last = new MMonPaxos(mon->get_epoch(), MMonPaxos::OP_LAST,
				  ceph_clock_now(g_ceph_context));
  last->last_committed = last_committed;//本地以往保持的两个committed，返回给leader，这个其实跟raft的election时看是不是最新的也基本相似
  last->first_committed = first_committed;
  
  version_t previous_pn = accepted_pn;//这个是本地记录的以前的accepted_pn

  // can we accept this pn?
  if (collect->pn > accepted_pn) {
    // ok, accept it
    accepted_pn = collect->pn;//下一步apply_transaction写盘，必须先持久化，再答复
    accepted_pn_from = collect->pn_from;
    dout(10) << "accepting pn " << accepted_pn << " from " 
	     << accepted_pn_from << dendl;
  
    MonitorDBStore::TransactionRef t(new MonitorDBStore::Transaction);
    t->put(get_name(), "accepted_pn", accepted_pn);//就持久化这一个东西

    dout(30) << __func__ << " transaction dump:\n";
    JSONFormatter f(true);
    t->dump(&f);
    f.flush(*_dout);
    *_dout << dendl;3

    logger->inc(l_paxos_collect);
    logger->inc(l_paxos_collect_keys, t->get_keys());
    logger->inc(l_paxos_collect_bytes, t->get_bytes());
    utime_t start = ceph_clock_now(NULL);

    get_store()->apply_transaction(t);

    utime_t end = ceph_clock_now(NULL);
    logger->tinc(l_paxos_collect_latency, end - start);
  } else {// 小于或者等于的情况，不accept，就不修改数据库。但是后面还是会答复，让对方知道它自己用了旧的pn
    // don't accept!
    dout(10) << "NOT accepting pn " << collect->pn << " from " << collect->pn_from
	     << ", we already accepted " << accepted_pn
	     << " from " << accepted_pn_from << dendl;
  }
  last->pn = accepted_pn;//如果不接受，也会告诉对方我的accepted_pn
  last->pn_from = accepted_pn_from;

  // share whatever committed values we have
  if (collect->last_committed < last_committed)//这个share干了啥，参见函数注释。好像不管前面pn的比较结果?
    share_state(last, collect->first_committed, collect->last_committed);//在回复(last)中直接包含了share_state的内容
//这里又在处理last_committed，handle_begin也在处理这些，啥关系 ? 
  // do we have an accepted but uncommitted value?
  //  (it'll be at last_committed+1)
  bufferlist bl;
  if (collect->last_committed <= last_committed &&
      get_store()->exists(get_name(), last_committed+1)) {//上面的share_state，针对的是 <的情况，last_committed对应的版本是已经committed，可以直接share。
    get_store()->get(get_name(), last_committed+1, bl);//这里针对的是 <=，并且存在 last_committed + 1 这个版本，属于未commit的，不能直接跟committed同样处理，告诉对方这些value需要commit
    assert(bl.length() > 0);//不用考虑last_commited+2或者更高版本，因为只允许一个版本在这个状态。
    dout(10) << " sharing our accepted but uncommitted value for " 
	     << last_committed+1 << " (" << bl.length() << " bytes)" << dendl;
    last->values[last_committed+1] = bl;//这个不是数组，map结构。对应last_committed+1的value

    version_t v = get_store()->get(get_name(), "pending_v");
    version_t pn = get_store()->get(get_name(), "pending_pn");//pending_pn跟accepted_pn可能是不同的？如何发生的？
    if (v && pn && v == last_committed + 1) {//我理解这个就是内部一致性检查, pending_v应该跟last_committed+1相等，并且pn应该存在。
      last->uncommitted_pn = pn; //如果有pending_pn，那么返回的 uncommitted_pn就是 pending_pn，否则就在下面直接用previous_pn代替了
    } else {
      // previously we didn't record which pn a value was accepted
      // under!  use the pn value we just had...  :(
      dout(10) << "WARNING: no pending_pn on disk, using previous accepted_pn " << previous_pn
	       << " and crossing our fingers" << dendl;
      last->uncommitted_pn = previous_pn;//不知道是哪个pn，给了上一个pn，但是可能是有问题的。
    }

    logger->inc(l_paxos_collect_uncommitted);
  }

  // send reply
  collect->get_connection()->send_message(last);
}
//share_state/store_state 的原理是什么？ 注意，share的不仅仅是两个committed变量，还包括中间的那些value。
/**对方的处理函数应该是： store_state。share的是两个last_committed之间的各个版本对应的value，那些对应于last_committed+1的，是不能这么share的。
 * @note This is Okay. We share our versions between peer_last_committed and
 *	 our last_committed (inclusive), and add their bufferlists to the
 *	 message. It will be the peer's job to apply them to its store, as
 *	 these bufferlists will contain raw transactions.
 *	 This function is called by both the Peon and the Leader. The Peon will
 *	 share the state with the Leader during handle_collect(), sharing any
 *	 values the leader may be missing (i.e., the leader's last_committed is
 *	 lower than the peon's last_committed). The Leader will share the state
 *	 with the Peon during handle_last(), if the peon's last_committed is
 *	 lower than the leader's last_committed.
 */
void Paxos::share_state(MMonPaxos *m, version_t peer_first_committed,
			version_t peer_last_committed)
{
  assert(peer_last_committed < last_committed);

  dout(10) << "share_state peer has fc " << peer_first_committed 
	   << " lc " << peer_last_committed << dendl;
  version_t v = peer_last_committed + 1;

  // include incrementals
  uint64_t bytes = 0;
  for ( ; v <= last_committed; v++) {//注意这里面并没有进行消息传递，只是把两个版本之间的内容给填写进去了。在调用函数中捎带发送报文。
    if (get_store()->exists(get_name(), v)) {
      get_store()->get(get_name(), v, m->values[v]);
      assert(m->values[v].length());
      dout(10) << " sharing " << v << " ("
	       << m->values[v].length() << " bytes)" << dendl;
      bytes += m->values[v].length() + 16;  // paxos_ + 10 digits = 16
    }
  }
  logger->inc(l_paxos_share_state);
  logger->inc(l_paxos_share_state_keys, m->values.size());
  logger->inc(l_paxos_share_state_bytes, bytes);

  m->last_committed = last_committed;
}

/**
 * Store on disk a state that was shared with us
 *
 * Basically, we received a set of version. Or just one. It doesn't matter.
 * What matters is that we have to stash it in the store. So, we will simply
 * write every single bufferlist into their own versions on our side (i.e.,
 * onto paxos-related keys), and then we will decode those same bufferlists
 * we just wrote and apply the transactions they hold. We will also update
 * our first and last committed values to point to the new values, if need
 * be. All all this is done tightly wrapped in a transaction to ensure we
 * enjoy the atomicity guarantees given by our awesome k/v store.
 */
bool Paxos::store_state(MMonPaxos *m)
{
  MonitorDBStore::TransactionRef t(new MonitorDBStore::Transaction);
  map<version_t,bufferlist>::iterator start = m->values.begin();
  bool changed = false;

  // build map of values to store
  // we want to write the range [last_committed, m->last_committed] only. 对方状态比我快太多，都不连续，没法更新
  if (start != m->values.end() &&
      start->first > last_committed + 1) {
    // ignore everything if values start in the future.
    dout(10) << "store_state ignoring all values, they start at " << start->first
	     << " > last_committed+1" << dendl;
    start = m->values.end();
  }

  // push forward the start position on the message's values iterator, up until
  // we run out of positions or we find a position matching 'last_committed'.
  while (start != m->values.end() && start->first <= last_committed) {//移到我的last_committed开始
    ++start;
  }

  // make sure we get the right interval of values to apply by pushing forward
  // the 'end' iterator until it matches the message's 'last_committed'.
  map<version_t,bufferlist>::iterator end = start;
  while (end != m->values.end() && end->first <= m->last_committed) {
    last_committed = end->first;
    ++end;
  }

  if (start == end) {
    dout(10) << "store_state nothing to commit" << dendl;
  } else {
    dout(10) << "store_state [" << start->first << ".." 
	     << last_committed << "]" << dendl;
    t->put(get_name(), "last_committed", last_committed);

    // we should apply the state here -- decode every single bufferlist in the
    // map and append the transactions to 't'.
    map<version_t,bufferlist>::iterator it;
    for (it = start; it != end; ++it) {
      // write the bufferlist as the version's value
      t->put(get_name(), it->first, it->second);//要store的value，先推入t
      // decode the bufferlist and append it to the transaction we will shortly
      // apply.
      decode_append_transaction(t, it->second);
    }

    // discard obsolete uncommitted value?
    if (uncommitted_v && uncommitted_v <= last_committed) {
      dout(10) << " forgetting obsolete uncommitted value " << uncommitted_v
	       << " pn " << uncommitted_pn << dendl;
      uncommitted_v = 0;
      uncommitted_pn = 0;
      uncommitted_value.clear();
    }
  }
  if (!t->empty()) {//t非空，说明有值要写
    dout(30) << __func__ << " transaction dump:\n";
    JSONFormatter f(true);
    t->dump(&f);
    f.flush(*_dout);
    *_dout << dendl;

    logger->inc(l_paxos_store_state);
    logger->inc(l_paxos_store_state_bytes, t->get_bytes());
    logger->inc(l_paxos_store_state_keys, t->get_keys());
    utime_t start = ceph_clock_now(NULL);

    get_store()->apply_transaction(t); //dhq: 事务提交，包括last_committed和众version及values。这个函数实际上会等待事务完成。

    utime_t end = ceph_clock_now(NULL);
    logger->tinc(l_paxos_store_state_latency, end - start);

    // refresh first_committed; this txn may have trimmed.
    first_committed = get_store()->get(get_name(), "first_committed");//first_committed可能在事务执行过程中trim被修改了，在apply_transaction完成才能修改这个变量。
    //我理解必须先持久化 first_committed ，才能修改变量。但是last_committed为什么不是这样？
    _sanity_check_store();
    changed = true;//说明有修改的值
  }

  remove_legacy_versions();//erase掉比first_committed更早的

  return changed;
}

void Paxos::remove_legacy_versions() //这个是从conversion_first开始erase的，含义？代码中没见到store这个key的地方
{
  if (get_store()->exists(get_name(), "conversion_first")) {
    MonitorDBStore::TransactionRef t(new MonitorDBStore::Transaction);
    version_t v = get_store()->get(get_name(), "conversion_first");
    dout(10) << __func__ << " removing pre-conversion paxos states from " << v
	     << " until " << first_committed << dendl;
    for (; v < first_committed; ++v) {
      t->erase(get_name(), v);
    }
    t->erase(get_name(), "conversion_first");
    get_store()->apply_transaction(t);
  }
}

void Paxos::_sanity_check_store()
{
  version_t lc = get_store()->get(get_name(), "last_committed");
  assert(lc == last_committed);
}


// leader;  leader.collect => peons.handle_collect => leader.handle_last
void Paxos::handle_last(MonOpRequestRef op)
{
  op->mark_paxos_event("handle_last");
  MMonPaxos *last = static_cast<MMonPaxos*>(op->get_req());
  bool need_refresh = false;
  int from = last->get_source().num();

  dout(10) << "handle_last " << *last << dendl;

  if (!mon->is_leader()) {
    dout(10) << "not leader, dropping" << dendl;
    return;
  }

  // note peer's first_ and last_committed, in case we learn a new
  // commit and need to push it to them.
  peer_first_committed[from] = last->first_committed;//本次返回的，作为map的一项。
  peer_last_committed[from] = last->last_committed;

  if (last->first_committed > last_committed + 1) {//跟别人相比落后很多，以至于别人也没有保留当时的各个版本的raw transaction信息。只有直接走bootstrap流程，从别人的状态机直接同步历史。
    dout(5) << __func__
            << " mon." << from
	    << " lowest version is too high for our last committed"
            << " (theirs: " << last->first_committed
            << "; ours: " << last_committed << ") -- bootstrap!" << dendl;
    op->mark_paxos_event("need to bootstrap");
    mon->bootstrap();//TODO: leader在做bootstrap后，重新开始选举吗 ?
    return;
  }

  assert(g_conf->paxos_kill_at != 1);

  // store any committed values if any are specified in the message
  need_refresh = store_state(last);//dhq: 对应handle_collect 内部的share_state，我理解就是在peon回应collect的同一个消息内部包含了share_state内容

  assert(g_conf->paxos_kill_at != 2);
  //我理解是上面的store_state已经改变了自己的last_committed和first_committed，即自己变新了，所以可能需要再share一次state给别的peer。
  // is everyone contiguous and up to date?
  for (map<int,version_t>::iterator p = peer_last_committed.begin();
       p != peer_last_committed.end();
       ++p) {
    if (p->second + 1 < first_committed && first_committed > 1) {
      dout(5) << __func__
	      << " peon " << p->first
	      << " last_committed (" << p->second
	      << ") is too low for our first_committed (" << first_committed
	      << ") -- bootstrap!" << dendl;
      op->mark_paxos_event("need to bootstrap");
      mon->bootstrap();//bootstrap内部，如何做同步？ 参见sync_start函数
      return;
    }
    if (p->second < last_committed) {//dhq: 对方不知道该version/instance已经被commit。注意commit是个单独操作，一旦开始，表明phase 2完成了。只是有的peon没收到而已，可以直接share
      // share committed values
      dout(10) << " sending commit to mon." << p->first << dendl;
      MMonPaxos *commit = new MMonPaxos(mon->get_epoch(),
					MMonPaxos::OP_COMMIT,//dhq: OP_COMMIT 是要干嘛？ 借助commit报文做share? 我理解起码会让peon修改accepted_pn，表示接受新leader当选的term
					ceph_clock_now(g_ceph_context));
      share_state(commit, peer_first_committed[p->first], p->second);//关于这个share_state，我理解是把那些peer不知道的版本对应值，告诉它。做个状态机同步？
      mon->messenger->send_message(commit, mon->monmap->get_inst(p->first));//这个是同步过程还是异步过程？ 会不会重复地对同一个peon调用share_state?
    }
  }

  // do they accept your pn?
  if (last->pn > accepted_pn) {//dhq: 这个 pn ，是本次collect用的，而不是 uncommitted_pn ，应该为两个东西。 比leader状态新的，在上一步已经通过share_state和store_state做了同步，所以下次应该会成功的。
    // no, try again.
    dout(10) << " they had a higher pn than us, picking a new one." << dendl;

    // cancel timeout event
    mon->timer.cancel_event(collect_timeout_event);
    collect_timeout_event = 0;
    //问题：为什么会出现last->pn大于我的pn? 是因为选举前我就落后了？ 会不会是两个人都认为自己是leader，最近进行了两次选举导致的？参见dispatch_op中，MSG_MON_PAXOS分支，会先比较epoch，如果又发生选举了，epoch肯定不同了。
    collect(last->pn);//dhq: 注意，这个pn作为参数，是oldpn，collect会产生一个新的，更大的。因为我是leader，只能一直尝试，直到成功。
  } else if (last->pn == accepted_pn) {//dhq: pn，不是 uncommitted_pn
    // yes, they accepted our pn.  great.
    num_last++;
    dout(10) << " they accepted our pn, we now have " 
	     << num_last << " peons" << dendl;

    // did this person send back an accepted but uncommitted value?
    if (last->uncommitted_pn) {//last_commited对应的是leader明确通知commit的(OP_COMMIT)。而uncommitted则是prepare过，但是未收到commit指令的。
      if (last->uncommitted_pn >= uncommitted_pn && //这一行比较pn，应该就是解决我下面的TODO疑问。即paxos中，选择回答中pn最大的那个value
	  last->last_committed >= last_committed &&//如果<，那么说明对方太旧了，其认为uncommitted的，实际上可能已经被commit了。需要被share_state
	  last->last_committed + 1 >= uncommitted_v) {//假定我们现在在处理Peon B的消息。之前可能有其他peon A回复时，我们修改了uncommitted_v。但是A本身相对B比较旧，那么之前那个就要废掉。A的状态，可以通过share_state更新到一致。
	uncommitted_v = last->last_committed+1; //dhq: 本实现，只允许一个版本处在未决状态，只能是last_committed+1
	uncommitted_pn = last->uncommitted_pn;//疑问: 下面的 uncommitted_value 赋值，在多个peon返回不同值的情况下，怎么办？==>参见上面关于uncommitted_pn的比较
	uncommitted_value = last->values[uncommitted_v];//这个是否就是当我们的proposal被接受，但是对方说已经accept了一个value的情况下
	dout(10) << "we learned an uncommitted value for " << uncommitted_v   //，我们先把这个value完成？即不是我确定的value，而是之前被accept的value
		 << " pn " << uncommitted_pn
		 << " " << uncommitted_value.length() << " bytes"
		 << dendl;
      } else {
	dout(10) << "ignoring uncommitted value for " << (last->last_committed+1)
		 << " pn " << last->uncommitted_pn
		 << " " << last->values[last->last_committed+1].length() << " bytes"
		 << dendl;
      }
    }
    
    // is that everyone?
    if (num_last == mon->get_quorum().size()) {//这里要求everyone，是指quorum里面的everyone。那些不能正常通信的monitor，不在quorunm里面。
      // cancel timeout event
      mon->timer.cancel_event(collect_timeout_event);
      collect_timeout_event = 0;
      peer_first_committed.clear();
      peer_last_committed.clear();

      // almost...

      // did we learn an old value?
      if (uncommitted_v == last_committed+1 &&  //dhq: 为什么是+1? 因为只允许一个pending的。这个uncommitted 属于上一个朝代的
	  uncommitted_value.length()) {//uncommitted_v可能是来自于peon在last 消息中携带的，上面刚刚赋了值
	dout(10) << "that's everyone.  begin on old learned value" << dendl;
	state = STATE_UPDATING_PREVIOUS;//TODO: collect结束，但是发现之前，有未commit的value，这些value已经被某些monitor记录，现在继续该value的过程
	begin(uncommitted_value);//dhq: 这个是走正常的prepare, accept流程了。这个就是相当于在上一个朝代完成了标准paxos的phase 1，那么就应该继续完成phase 2，必须用相同的value，只是换了个pn而已.
      } else {
	// active!
	dout(10) << "that's everyone.  active!" << dendl;
	extend_lease();

	need_refresh = false;
	if (do_refresh()) {
	  finish_round();
	}
      }
    }
  } else {
    // no, this is an old message, discard
    dout(10) << "old pn, ignoring" << dendl;
  }

  if (need_refresh)
    (void)do_refresh();
}

void Paxos::collect_timeout()
{
  dout(1) << "collect timeout, calling fresh election" << dendl;
  collect_timeout_event = 0;
  logger->inc(l_paxos_collect_timeout);
  assert(mon->is_leader());
  mon->bootstrap(); //直接重新选举了
}


// leader
void Paxos::begin(bufferlist& v)
{
  dout(10) << "begin for " << last_committed+1 << " " 
	   << v.length() << " bytes"
	   << dendl;

  assert(mon->is_leader());
  assert(is_updating() || is_updating_previous());

  // we must already have a majority for this to work.
  assert(mon->get_quorum().size() == 1 ||
	 num_last > (unsigned)mon->monmap->size()/2);//leader当选后准备工作完成
  
  // and no value, yet.
  assert(new_value.length() == 0);

  // accept it ourselves
  accepted.clear(); //清空已accept的peon列表
  accepted.insert(mon->rank);//自己先accept
  new_value = v;

  if (last_committed == 0) {
    MonitorDBStore::TransactionRef t(new MonitorDBStore::Transaction);//这个t，很快被后面的覆盖，有意义吗？ 可能只是借用来进行decode，将其并入new_value? 
    // initial base case; set first_committed too
    t->put(get_name(), "first_committed", 1);//为什么是1,不是0?
    decode_append_transaction(t, new_value);

    bufferlist tx_bl;
    t->encode(tx_bl);//t什么时候释放？

    new_value = tx_bl;
  }

  // store the proposed value in the store. IF it is accepted, we will then
  // have to decode it into a transaction and apply it.
  MonitorDBStore::TransactionRef t(new MonitorDBStore::Transaction);
  t->put(get_name(), last_committed+1, new_value);

  // note which pn this pending value is for.
  t->put(get_name(), "pending_v", last_committed + 1);
  t->put(get_name(), "pending_pn", accepted_pn);

  dout(30) << __func__ << " transaction dump:\n";
  JSONFormatter f(true);
  t->dump(&f);
  f.flush(*_dout);
  MonitorDBStore::TransactionRef debug_tx(new MonitorDBStore::Transaction);
  bufferlist::iterator new_value_it = new_value.begin();
  debug_tx->decode(new_value_it);
  debug_tx->dump(&f);
  *_dout << "\nbl dump:\n";
  f.flush(*_dout);
  *_dout << dendl;

  logger->inc(l_paxos_begin);
  logger->inc(l_paxos_begin_keys, t->get_keys());
  logger->inc(l_paxos_begin_bytes, t->get_bytes());
  utime_t start = ceph_clock_now(NULL);

  get_store()->apply_transaction(t);//保存前面put的三个 k/v? 

  utime_t end = ceph_clock_now(NULL);
  logger->tinc(l_paxos_begin_latency, end - start);

  assert(g_conf->paxos_kill_at != 3);

  if (mon->get_quorum().size() == 1) {
    // we're alone, take it easy
    commit_start();
    return;
  }

  // ask others to accept it too!
  for (set<int>::const_iterator p = mon->get_quorum().begin();
       p != mon->get_quorum().end();
       ++p) {
    if (*p == mon->rank) continue;
    
    dout(10) << " sending begin to mon." << *p << dendl;
    MMonPaxos *begin = new MMonPaxos(mon->get_epoch(), MMonPaxos::OP_BEGIN,
				     ceph_clock_now(g_ceph_context));
    begin->values[last_committed+1] = new_value;
    begin->last_committed = last_committed;//这个直接发送过去了
    begin->pn = accepted_pn;
    
    mon->messenger->send_message(begin, mon->monmap->get_inst(*p));
  }

  // set timeout event
  accept_timeout_event = new C_AcceptTimeout(this);
  mon->timer.add_event_after(g_conf->mon_accept_timeout, accept_timeout_event);
}

// peon
void Paxos::handle_begin(MonOpRequestRef op)
{
  op->mark_paxos_event("handle_begin");
  MMonPaxos *begin = static_cast<MMonPaxos*>(op->get_req());
  dout(10) << "handle_begin " << *begin << dendl;

  // can we accept this?
  if (begin->pn < accepted_pn) {//这个应该是历史消息，按照paxos模型，消息可以有很大延迟才到。
    dout(10) << " we accepted a higher pn " << accepted_pn << ", ignoring" << dendl;
    op->mark_paxos_event("have higher pn, ignore");//在有leader的情况下不该发生？ ==> 可能是旧leader的消息延迟了很久才到
    return;
  }//如果走到下面，说明当前leader还是leader，那么不会有人在同时发请求。
  assert(begin->pn == accepted_pn);//leader完成collect过程后，pn是一直不变的。除非下台了。如果下台了，重新选举某个peer还在quorum里面，那么再完成collect后，pn还是一致的。
  assert(begin->last_committed == last_committed);//因为leader没有变化，请求一个一个处理，所以总是维持一致的last_committed，双方知道下一个版本是啥
  //之所以说begin()对应了phase1和phase2，其实从这里也可以看出来，实际上peer根本不会比较自己是否accept了别的pn，因为只有leader在发请求。
  assert(g_conf->paxos_kill_at != 4);

  logger->inc(l_paxos_begin);

  // set state.
  state = STATE_UPDATING;
  lease_expire = utime_t();  // cancel lease

  // yes.
  version_t v = last_committed+1; //设置version，由于Ceph的实现中，串行处理。在正常分支中，不会有其他 uncommitted版本存在。
  dout(10) << "accepting value for " << v << " pn " << accepted_pn << dendl;
  // store the accepted value onto our store. We will have to decode it and
  // apply its transaction once we receive permission to commit.
  MonitorDBStore::TransactionRef t(new MonitorDBStore::Transaction);
  t->put(get_name(), v, begin->values[v]);

  // note which pn this pending value is for.
  t->put(get_name(), "pending_v", v);//下面会apply，但是这些都是pending的
  t->put(get_name(), "pending_pn", accepted_pn);//不用写pending_version，因为有last_committed已经持久化了，这个含义明确。

  dout(30) << __func__ << " transaction dump:\n";
  JSONFormatter f(true);
  t->dump(&f);
  f.flush(*_dout);
  *_dout << dendl;

  logger->inc(l_paxos_begin_bytes, t->get_bytes());
  utime_t start = ceph_clock_now(NULL);

  get_store()->apply_transaction(t);

  utime_t end = ceph_clock_now(NULL);
  logger->tinc(l_paxos_begin_latency, end - start);

  assert(g_conf->paxos_kill_at != 5);
  
  // reply
  MMonPaxos *accept = new MMonPaxos(mon->get_epoch(), MMonPaxos::OP_ACCEPT,
				    ceph_clock_now(g_ceph_context));
  accept->pn = accepted_pn;
  accept->last_committed = last_committed;
  begin->get_connection()->send_message(accept);
}

// leader
void Paxos::handle_accept(MonOpRequestRef op)
{
  op->mark_paxos_event("handle_accept");
  MMonPaxos *accept = static_cast<MMonPaxos*>(op->get_req());
  dout(10) << "handle_accept " << *accept << dendl;
  int from = accept->get_source().num();

  if (accept->pn != accepted_pn) {//按照这个解释，我认为pn类似于raft的term。这个意思是别人已经当选了，错发给自己的accept。
    // we accepted a higher pn, from some other leader //问题是，这个在什么情况下发生？新当选了leader? epoch什么值？
    dout(10) << " we accepted a higher pn " << accepted_pn << ", ignoring" << dendl;
    op->mark_paxos_event("have higher pn, ignore");
    return;
  }
  if (last_committed > 0 &&
      accept->last_committed < last_committed-1) {
    dout(10) << " this is from an old round, ignoring" << dendl;
    op->mark_paxos_event("old round, ignore");
    return;
  }
  assert(accept->last_committed == last_committed ||   // not committed
	 accept->last_committed == last_committed-1);  // 参见is_updating_previous()的注释，应该是跟handle_last()里面那个begin()对应的，这种情况下，大家还没完全一致，但是差别不会太大。
	 																								//所以晚点到达的accept，位于quorum个之外的还可以处理？
  assert(is_updating() || is_updating_previous());//上面一句也有个||，前后分别对应is_updating和is_updating_previous两个状态。只不过这个代码写得隐晦而已。
  assert(accepted.count(from) == 0);
  accepted.insert(from);//TODO: bug? 接收到对应last_committed和last_committed-1的accept，都放到 accepted里面？ 这么计算把别人对上次的回答当作这次的，不是bug吗？
  dout(10) << " now " << accepted << " have accepted" << dendl;//上一行理解有误，实际上对方accept的，记录在 pending 里面，这个last_committed，是已经commit的版本号。

  assert(g_conf->paxos_kill_at != 6);

  // only commit (and expose committed state) when we get *all* quorum
  // members to accept.  otherwise, they may still be sharing the now
  // stale state.
  // FIXME: we can improve this with an additional lease revocation message
  // that doesn't block for the persist.  啥意思？下面的做法跟lease有关？
  if (accepted == mon->get_quorum()) {//所有在quorum里面的都响应才行。如果原来认为活跃的任意一个不干活，会引起timeout，参见accept_timeout()
    // yay, commit!
    dout(10) << " got majority, committing, done with update" << dendl;
    op->mark_paxos_event("commit_start");
    commit_start();//一旦收完到quorum内所有peer的 accept，就启动 commit完成后，commit_finish触发进行下一个instance。
  }
}

void Paxos::accept_timeout()
{
  dout(1) << "accept timeout, calling fresh election" << dendl;
  accept_timeout_event = 0;
  assert(mon->is_leader());
  assert(is_updating() || is_updating_previous() || is_writing() ||
	 is_writing_previous());
  logger->inc(l_paxos_accept_timeout);
  mon->bootstrap();
}

struct C_Committed : public Context {
  Paxos *paxos;
  C_Committed(Paxos *p) : paxos(p) {}
  void finish(int r) {
    assert(r >= 0);
    Mutex::Locker l(paxos->mon->lock);//这个类，在构造函数中获得锁，析构函数中释放锁，所以不需要显示调用加锁解锁函数。
    paxos->commit_finish();//相当于这一段执行，是受mon->lock保护的，为什么保护这一段？原理？ 这段时间应该通过lock阻挡了选举？
  }
};

void Paxos::commit_start()
{
  dout(10) << __func__ << " " << (last_committed+1) << dendl;

  assert(g_conf->paxos_kill_at != 7);

  MonitorDBStore::TransactionRef t(new MonitorDBStore::Transaction);

  // commit locally
  t->put(get_name(), "last_committed", last_committed + 1);//这个修改，下面是queue_trans，不是apply

  // decode the value and apply its transaction to the store.
  // this value can now be read from last_committed.
  decode_append_transaction(t, new_value);

  dout(30) << __func__ << " transaction dump:\n";
  JSONFormatter f(true);
  t->dump(&f);
  f.flush(*_dout);
  *_dout << dendl;

  logger->inc(l_paxos_commit);
  logger->inc(l_paxos_commit_keys, t->get_keys());
  logger->inc(l_paxos_commit_bytes, t->get_bytes());
  commit_start_stamp = ceph_clock_now(NULL);
			//C_Committed在finish执行时，会获取锁(在finish函数)，但是当前上下文应该持有了锁，什么时候放锁的?  否则C_Committed:finish() 没法拿到锁。
  get_store()->queue_transaction(t, new C_Committed(this));//dhq:transaction结束时callback C_Committed的finish? 即异步trans
	//在MonitorDBStore.h的finish()中有注释，实际上是在做了txn apply后，调用callback
  if (is_updating_previous())
    state = STATE_WRITING_PREVIOUS;
  else if (is_updating())
    state = STATE_WRITING;//dhq: 设置这两个状态后，依赖于异步的commit完成回调，去做清除，本上下文实际上很快结束，不会修改此状态。参见paxos::commit_finish()
  else
    assert(0);

  if (mon->get_quorum().size() > 1) {
    // cancel timeout event
    mon->timer.cancel_event(accept_timeout_event);
    accept_timeout_event = 0;
  }
}

void Paxos::commit_finish()
{
  dout(20) << __func__ << " " << (last_committed+1) << dendl;
  utime_t end = ceph_clock_now(NULL);
  logger->tinc(l_paxos_commit_latency, end - commit_start_stamp);

  assert(g_conf->paxos_kill_at != 8);
  //为什么在commit_finish()才修改让lease失效？ 而不是begin()? 在handle_begin()里面，直接就让失效了。
  // cancel lease - it was for the old value.
  //  (this would only happen if message layer lost the 'begin', but
  //   leader still got a majority and committed with out us.)
  lease_expire = utime_t();  // cancel lease
  //上面注释什么意思？我理解commit_finish是仅仅在leader执行的，上面的注释写得应该是peon? 再说，leader自己需要lease吗？
  last_committed++;
  last_commit_time = ceph_clock_now(NULL);

  // refresh first_committed; this txn may have trimmed.
  first_committed = get_store()->get(get_name(), "first_committed");

  _sanity_check_store();

  // tell everyone
  for (set<int>::const_iterator p = mon->get_quorum().begin();
       p != mon->get_quorum().end();
       ++p) {
    if (*p == mon->rank) continue;

    dout(10) << " sending commit to mon." << *p << dendl;
    MMonPaxos *commit = new MMonPaxos(mon->get_epoch(), MMonPaxos::OP_COMMIT,
				      ceph_clock_now(g_ceph_context));
    commit->values[last_committed] = new_value;
    commit->pn = accepted_pn;
    commit->last_committed = last_committed;

    mon->messenger->send_message(commit, mon->monmap->get_inst(*p));//本地的last_committed等信息已经修改了，通知其他的
  }//TODO：这里并没有等待消息返回，怎么保证我再begin()，对方在handle_begin()时，与我的last_committed相等? 上面的OP_COMMIT消息可能延时很久才到。

  assert(g_conf->paxos_kill_at != 9);

  // get ready for a new round.
  new_value.clear();

  remove_legacy_versions();

  // WRITING -> REFRESH
  // among other things, this lets do_refresh() -> mon->bootstrap() know
  // it doesn't need to flush the store queue
  assert(is_writing() || is_writing_previous());
  state = STATE_REFRESH;

  if (do_refresh()) {//do_refresh的注释中(.h)，在它返回false时，abort，应该是异常情况吧。
    commit_proposal();
    if (mon->get_quorum().size() > 1) {
      extend_lease();
    }

    finish_contexts(g_ceph_context, waiting_for_commit);

    assert(g_conf->paxos_kill_at != 10);
    //do_refresh()返回false到底代表什么含义？为什么不能finish_round()?
    finish_round();//我认为这是修改状态，代表本轮结束。让pending的proposal可以继续执行
  }
}

//Peon的函数，store_state内部也是走transaction.
void Paxos::handle_commit(MonOpRequestRef op)
{
  op->mark_paxos_event("handle_commit");
  MMonPaxos *commit = static_cast<MMonPaxos*>(op->get_req());
  dout(10) << "handle_commit on " << commit->last_committed << dendl;

  logger->inc(l_paxos_commit);

  if (!mon->is_peon()) {
    dout(10) << "not a peon, dropping" << dendl;
    assert(0);
    return;
  }

  op->mark_paxos_event("store_state");
  store_state(commit);

  if (do_refresh()) {
    finish_contexts(g_ceph_context, waiting_for_commit);//Peon端，没有等待被propose的。
  }
}

void Paxos::extend_lease()
{
  assert(mon->is_leader());
  //assert(is_active());

  lease_expire = ceph_clock_now(g_ceph_context);
  lease_expire += g_conf->mon_lease;
  acked_lease.clear();
  acked_lease.insert(mon->rank);

  dout(7) << "extend_lease now+" << g_conf->mon_lease 
	  << " (" << lease_expire << ")" << dendl;

  // bcast
  for (set<int>::const_iterator p = mon->get_quorum().begin();
      p != mon->get_quorum().end(); ++p) {

    if (*p == mon->rank) continue;
    MMonPaxos *lease = new MMonPaxos(mon->get_epoch(), MMonPaxos::OP_LEASE,
				     ceph_clock_now(g_ceph_context));
    lease->last_committed = last_committed;//授权的lease，是跟last_committed相关的，相当于到这个版本，都是授权了的。
    lease->lease_timestamp = lease_expire; //告诉对方，用这个超时时间
    lease->first_committed = first_committed;
    mon->messenger->send_message(lease, mon->monmap->get_inst(*p));
  }

  // set timeout event.
  //  if old timeout is still in place, leave it.
  if (!lease_ack_timeout_event) {
    lease_ack_timeout_event = new C_LeaseAckTimeout(this);
    mon->timer.add_event_after(g_conf->mon_lease_ack_timeout, 
			       lease_ack_timeout_event);
  }

  // set renew event
  lease_renew_event = new C_LeaseRenew(this);
  utime_t at = lease_expire;
  at -= g_conf->mon_lease;
  at += g_conf->mon_lease_renew_interval;
  mon->timer.add_event_at(at, lease_renew_event);
}
//有这些检查，实际上是因为没有搞定时间同步问题。按照raft论文所说，这个本身是复杂的。
void Paxos::warn_on_future_time(utime_t t, entity_name_t from)
{
  utime_t now = ceph_clock_now(g_ceph_context);
  if (t > now) {
    utime_t diff = t - now;
    if (diff > g_conf->mon_clock_drift_allowed) {
      utime_t warn_diff = now - last_clock_drift_warn;
      if (warn_diff >
	  pow(g_conf->mon_clock_drift_warn_backoff, clock_drift_warned)) {
	mon->clog->warn() << "message from " << from << " was stamped " << diff
			 << "s in the future, clocks not synchronized";
	last_clock_drift_warn = ceph_clock_now(g_ceph_context);
	++clock_drift_warned;
      }
    }
  }

}
//需要bootstrap时。我理解，是因为commit完成表述一个状态修改完成，自然要让上面的paxosserivice更新状态。比如，mds状态修改完了，mdsmonitor自然需要知道结果，因此做个update.
bool Paxos::do_refresh()
{
  bool need_bootstrap = false;

  utime_t start = ceph_clock_now(NULL);

  // make sure we have the latest state loaded up
  mon->refresh_from_paxos(&need_bootstrap);

  utime_t end = ceph_clock_now(NULL);
  logger->inc(l_paxos_refresh);
  logger->tinc(l_paxos_refresh_latency, end - start);

  if (need_bootstrap) {//需要bootstrap才返回false，正常都是成功
    dout(10) << " doing requested bootstrap" << dendl;
    mon->bootstrap();
    return false;
  }

  return true;
}

void Paxos::commit_proposal()
{
  dout(10) << __func__ << dendl;
  assert(mon->is_leader());
  assert(is_refresh());

  list<Context*> ls;
  ls.swap(committing_finishers);//从pending_finishers ==swap==>  committing_finishers  ==swap==>  ls
  finish_contexts(g_ceph_context, ls);//做callback，paxosservice调用 queue_pending_finisher()注册的钩子
}

void Paxos::finish_round()
{
  dout(10) << __func__ << dendl;
  assert(mon->is_leader());

  // ok, now go active!
  state = STATE_ACTIVE;

  dout(20) << __func__ << " waiting_for_acting" << dendl;
  finish_contexts(g_ceph_context, waiting_for_active);
  dout(20) << __func__ << " waiting_for_readable" << dendl;
  finish_contexts(g_ceph_context, waiting_for_readable);
  dout(20) << __func__ << " waiting_for_writeable" << dendl;
  finish_contexts(g_ceph_context, waiting_for_writeable);
  
  dout(10) << __func__ << " done w/ waiters, state " << state << dendl;

  if (should_trim()) {
    trim();
  }

  if (is_active() && pending_proposal) {
    propose_pending();
  }
}


// peon
void Paxos::handle_lease(MonOpRequestRef op)
{
  op->mark_paxos_event("handle_lease");
  MMonPaxos *lease = static_cast<MMonPaxos*>(op->get_req());
  // sanity
  if (!mon->is_peon() ||
      last_committed != lease->last_committed) {
    dout(10) << "handle_lease i'm not a peon, or they're not the leader,"
	     << " or the last_committed doesn't match, dropping" << dendl;
    op->mark_paxos_event("invalid lease, ignore");
    return;
  }

  warn_on_future_time(lease->sent_timestamp, lease->get_source());

  // extend lease
  if (lease_expire < lease->lease_timestamp) {
    lease_expire = lease->lease_timestamp;

    utime_t now = ceph_clock_now(g_ceph_context);
    if (lease_expire < now) {
      utime_t diff = now - lease_expire;
      derr << "lease_expire from " << lease->get_source_inst() << " is " << diff << " seconds in the past; mons are probably laggy (or possibly clocks are too skewed)" << dendl;
    }
  }

  state = STATE_ACTIVE;

  dout(10) << "handle_lease on " << lease->last_committed
	   << " now " << lease_expire << dendl;

  // ack
  MMonPaxos *ack = new MMonPaxos(mon->get_epoch(), MMonPaxos::OP_LEASE_ACK,
				 ceph_clock_now(g_ceph_context));
  ack->last_committed = last_committed;
  ack->first_committed = first_committed;
  ack->lease_timestamp = ceph_clock_now(g_ceph_context);
  lease->get_connection()->send_message(ack);

  // (re)set timeout event.
  reset_lease_timeout();

  // kick waiters
  finish_contexts(g_ceph_context, waiting_for_active);
  if (is_readable())
    finish_contexts(g_ceph_context, waiting_for_readable);
}

void Paxos::handle_lease_ack(MonOpRequestRef op)
{
  op->mark_paxos_event("handle_lease_ack");
  MMonPaxos *ack = static_cast<MMonPaxos*>(op->get_req());
  int from = ack->get_source().num();

  if (!lease_ack_timeout_event) {
    dout(10) << "handle_lease_ack from " << ack->get_source() 
	     << " -- stray (probably since revoked)" << dendl;
  }
  else if (acked_lease.count(from) == 0) {
    acked_lease.insert(from);
    
    if (acked_lease == mon->get_quorum()) {
      // yay!
      dout(10) << "handle_lease_ack from " << ack->get_source() 
	       << " -- got everyone" << dendl;
      mon->timer.cancel_event(lease_ack_timeout_event);
      lease_ack_timeout_event = 0;


    } else {
      dout(10) << "handle_lease_ack from " << ack->get_source() 
	       << " -- still need "
	       << mon->get_quorum().size() - acked_lease.size()
	       << " more" << dendl;
    }
  } else {
    dout(10) << "handle_lease_ack from " << ack->get_source() 
	     << " dup (lagging!), ignoring" << dendl;
  }

  warn_on_future_time(ack->sent_timestamp, ack->get_source());
}

void Paxos::lease_ack_timeout()
{
  dout(1) << "lease_ack_timeout -- calling new election" << dendl;
  assert(mon->is_leader());
  assert(is_active());
  logger->inc(l_paxos_lease_ack_timeout);
  lease_ack_timeout_event = 0;
  mon->bootstrap();
}

void Paxos::reset_lease_timeout()
{
  dout(20) << "reset_lease_timeout - setting timeout event" << dendl;
  if (lease_timeout_event)
    mon->timer.cancel_event(lease_timeout_event);
  lease_timeout_event = new C_LeaseTimeout(this);
  mon->timer.add_event_after(g_conf->mon_lease_ack_timeout, lease_timeout_event);
}

void Paxos::lease_timeout()
{
  dout(1) << "lease_timeout -- calling new election" << dendl;
  assert(mon->is_peon());
  logger->inc(l_paxos_lease_timeout);
  lease_timeout_event = 0;
  mon->bootstrap();
}

void Paxos::lease_renew_timeout()
{
  lease_renew_event = 0;
  extend_lease();
}


/*
 * trim old states
 */
void Paxos::trim()
{
  assert(should_trim());
  version_t end = MIN(get_version() - g_conf->paxos_min,
		      get_first_committed() + g_conf->paxos_trim_max);

  if (first_committed >= end)
    return;

  dout(10) << "trim to " << end << " (was " << first_committed << ")" << dendl;

  MonitorDBStore::TransactionRef t = get_pending_transaction();

  for (version_t v = first_committed; v < end; ++v) {
    dout(10) << "trim " << v << dendl;
    t->erase(get_name(), v);//把过早(end之前)的erase掉
  }
  t->put(get_name(), "first_committed", end);//再移动first_committed到end的位置
  if (g_conf->mon_compact_on_trim) {
    dout(10) << " compacting trimmed range" << dendl;
    t->compact_range(get_name(), stringify(first_committed - 1), stringify(end));
  }

  trimming = true;
  queue_pending_finisher(new C_Trimmed(this));
}

/*这个函数是collect时用的，而不是propose时用的。也就是说，属于leader当选后使用，而不是在选举过程中使用。当选后，用新的PN，处理上一个朝代潜在的未完成的instance
 * return a globally unique, monotonically increasing proposal number
 */现在理解是: 对应paxos made code中的ballot numbers? 但是文档里面不是说leader当选调用一次的。
version_t Paxos::get_new_proposal_number(version_t gt)
{
  if (last_pn < gt) 
    last_pn = gt;
  
  // update. make it unique among all monitors.
  last_pn /= 100; //由于gt可能是别人发过来的，是不同的rank，如果直接把last_pn +=100，last_pn所带的rank就是别人的，不是自己应该产生的合法pn。
  last_pn++;
  last_pn *= 100;
  last_pn += (version_t)mon->rank;//用python做过简单实验，如果不断用上次的pn做迭代，实际上就是不断加100

  // write
  MonitorDBStore::TransactionRef t(new MonitorDBStore::Transaction);
  t->put(get_name(), "last_pn", last_pn);//这个也要持久化记录

  dout(30) << __func__ << " transaction dump:\n";
  JSONFormatter f(true);
  t->dump(&f);
  f.flush(*_dout);
  *_dout << dendl;

  logger->inc(l_paxos_new_pn);
  utime_t start = ceph_clock_now(NULL);

  get_store()->apply_transaction(t);

  utime_t end = ceph_clock_now(NULL);
  logger->tinc(l_paxos_new_pn_latency, end - start);

  dout(10) << "get_new_proposal_number = " << last_pn << dendl;
  return last_pn;
}


void Paxos::cancel_events()
{
  if (collect_timeout_event) {
    mon->timer.cancel_event(collect_timeout_event);
    collect_timeout_event = 0;
  }
  if (accept_timeout_event) {
    mon->timer.cancel_event(accept_timeout_event);
    accept_timeout_event = 0;
  }
  if (lease_renew_event) {
    mon->timer.cancel_event(lease_renew_event);
    lease_renew_event = 0;
  }
  if (lease_ack_timeout_event) {
    mon->timer.cancel_event(lease_ack_timeout_event);
    lease_ack_timeout_event = 0;
  }  
  if (lease_timeout_event) {
    mon->timer.cancel_event(lease_timeout_event);
    lease_timeout_event = 0;
  }
}

void Paxos::shutdown()
{
  dout(10) << __func__ << " cancel all contexts" << dendl;

  // discard pending transaction
  pending_proposal.reset();

  finish_contexts(g_ceph_context, waiting_for_writeable, -ECANCELED);
  finish_contexts(g_ceph_context, waiting_for_commit, -ECANCELED);
  finish_contexts(g_ceph_context, waiting_for_readable, -ECANCELED);
  finish_contexts(g_ceph_context, waiting_for_active, -ECANCELED);
  finish_contexts(g_ceph_context, pending_finishers, -ECANCELED);
  finish_contexts(g_ceph_context, committing_finishers, -ECANCELED);
  if (logger)
    g_ceph_context->get_perfcounters_collection()->remove(logger);
  delete logger;
}

void Paxos::leader_init()
{
  cancel_events();
  new_value.clear();

  // discard pending transaction
  pending_proposal.reset();

  finish_contexts(g_ceph_context, pending_finishers, -EAGAIN);
  finish_contexts(g_ceph_context, committing_finishers, -EAGAIN);

  logger->inc(l_paxos_start_leader);

  if (mon->get_quorum().size() == 1) {
    state = STATE_ACTIVE;
    return;
  }

  state = STATE_RECOVERING;
  lease_expire = utime_t();
  dout(10) << "leader_init -- starting paxos recovery" << dendl;
  collect(0);
}

void Paxos::peon_init()
{
  cancel_events();
  new_value.clear();

  state = STATE_RECOVERING;
  lease_expire = utime_t();
  dout(10) << "peon_init -- i am a peon" << dendl;

  // start a timer, in case the leader never manages to issue a lease
  reset_lease_timeout();

  // discard pending transaction
  pending_proposal.reset();

  // no chance to write now!
  finish_contexts(g_ceph_context, waiting_for_writeable, -EAGAIN);
  finish_contexts(g_ceph_context, waiting_for_commit, -EAGAIN);
  finish_contexts(g_ceph_context, pending_finishers, -EAGAIN);
  finish_contexts(g_ceph_context, committing_finishers, -EAGAIN);

  logger->inc(l_paxos_start_peon);
}

void Paxos::restart()
{
  dout(10) << "restart -- canceling timeouts" << dendl;
  cancel_events();
  new_value.clear();

  if (is_writing() || is_writing_previous()) {
    dout(10) << __func__ << " flushing" << dendl;
    mon->lock.Unlock();
    mon->store->flush();
    mon->lock.Lock();
    dout(10) << __func__ << " flushed" << dendl;
  }
  state = STATE_RECOVERING;

  // discard pending transaction
  pending_proposal.reset();

  finish_contexts(g_ceph_context, committing_finishers, -EAGAIN);
  finish_contexts(g_ceph_context, pending_finishers, -EAGAIN);
  finish_contexts(g_ceph_context, waiting_for_commit, -EAGAIN);
  finish_contexts(g_ceph_context, waiting_for_active, -EAGAIN);

  logger->inc(l_paxos_restart);
}


void Paxos::dispatch(MonOpRequestRef op)
{
  assert(op->is_type_paxos());
  op->mark_paxos_event("dispatch");
  PaxosServiceMessage *m = static_cast<PaxosServiceMessage*>(op->get_req());
  // election in progress?
  if (!mon->is_leader() && !mon->is_peon()) {
    dout(5) << "election in progress, dropping " << *m << dendl;
    return;    
  }

  // check sanity
  assert(mon->is_leader() || 
	 (mon->is_peon() && m->get_source().num() == mon->get_leader()));//应该是指peon只接受leader的消息，但是不能接受其他peon的消息
  
  switch (m->get_type()) {

  case MSG_MON_PAXOS:
    {
      MMonPaxos *pm = reinterpret_cast<MMonPaxos*>(m);

      // NOTE: these ops are defined in messages/MMonPaxos.h
      switch (pm->op) {
	// learner
      case MMonPaxos::OP_COLLECT:
	handle_collect(op);
	break;
      case MMonPaxos::OP_LAST:
	handle_last(op);
	break;
      case MMonPaxos::OP_BEGIN:
	handle_begin(op);
	break;
      case MMonPaxos::OP_ACCEPT:
	handle_accept(op);
	break;		
      case MMonPaxos::OP_COMMIT:
	handle_commit(op);
	break;
      case MMonPaxos::OP_LEASE:
	handle_lease(op);
	break;
      case MMonPaxos::OP_LEASE_ACK:
	handle_lease_ack(op);
	break;
      default:
	assert(0);
      }
    }
    break;
    
  default:
    assert(0);
  }
}


// -----------------
// service interface

// -- READ --

bool Paxos::is_readable(version_t v)//缺省值v=0，即为最小版本，基本都成立。
{
  bool ret;
  if (v > last_committed)
    ret = false;
  else//做了这么多非原子判断，外面需要持有锁？
    ret =
      (mon->is_peon() || mon->is_leader()) &&
      (is_active() || is_updating() || is_writing()) &&  //recovery, load等阶段，不允许读
      last_committed > 0 &&           // must have a value
      (mon->get_quorum().size() == 1 ||  // alone, or
       is_lease_valid()); // have lease
  dout(5) << __func__ << " = " << (int)ret
	  << " - now=" << ceph_clock_now(g_ceph_context)
	  << " lease_expire=" << lease_expire
	  << " has v" << v << " lc " << last_committed
	  << dendl;
  return ret;
}

bool Paxos::read(version_t v, bufferlist &bl)
{
  if (!get_store()->get(get_name(), v, bl))
    return false;
  return true;
}

version_t Paxos::read_current(bufferlist &bl)
{
  if (read(last_committed, bl))
    return last_committed;
  return 0;
}


bool Paxos::is_lease_valid()
{
  return ((mon->get_quorum().size() == 1)
      || (ceph_clock_now(g_ceph_context) < lease_expire));
}

// -- WRITE --

bool Paxos::is_writeable()
{
  return
    mon->is_leader() &&
    is_active() &&
    is_lease_valid();//为什么writeable也要判断lease?  is_readable函数是带版本参数的，跟这个不同。
}

void Paxos::propose_pending()
{
  assert(is_active());
  assert(pending_proposal);

  cancel_events();

  bufferlist bl;
  pending_proposal->encode(bl);

  dout(10) << __func__ << " " << (last_committed + 1)
	   << " " << bl.length() << " bytes" << dendl;
  dout(30) << __func__ << " transaction dump:\n";
  JSONFormatter f(true);
  pending_proposal->dump(&f);
  f.flush(*_dout);
  *_dout << dendl;

  pending_proposal.reset();//让pending_proposal不再ref里面的Transaction类型对象,参见http://en.cppreference.com/w/cpp/memory/shared_ptr/reset

  committing_finishers.swap(pending_finishers); //list::swap()的含义： Exchanges the contents of two lists， http://www.cplusplus.com/reference/list/list/swap-free/
  state = STATE_UPDATING;
  begin(bl);
}

void Paxos::queue_pending_finisher(Context *onfinished)
{
  dout(5) << __func__ << " " << onfinished << dendl;
  assert(onfinished);
  pending_finishers.push_back(onfinished);
}

MonitorDBStore::TransactionRef Paxos::get_pending_transaction() //pending transaction，不是proposal
{
  assert(mon->is_leader());
  if (!pending_proposal) {
    pending_proposal.reset(new MonitorDBStore::Transaction);
    assert(pending_finishers.empty());
  }
  return pending_proposal;
}

bool Paxos::trigger_propose()
{
  if (is_active()) {
    dout(10) << __func__ << " active, proposing now" << dendl;
    propose_pending();
    return true;
  } else {
    dout(10) << __func__ << " not active, will propose later" << dendl;
    return false;
  }
}

bool Paxos::is_consistent()
{
  return (first_committed <= last_committed);
}

