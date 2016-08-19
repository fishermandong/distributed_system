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

#include "Elector.h"
#include "Monitor.h"

#include "common/Timer.h"
#include "MonitorDBStore.h"
#include "MonmapMonitor.h"
#include "messages/MMonElection.h"

#include "common/config.h"
#include "include/assert.h"

#define dout_subsys ceph_subsys_mon
#undef dout_prefix
#define dout_prefix _prefix(_dout, mon, epoch)
static ostream& _prefix(std::ostream *_dout, Monitor *mon, epoch_t epoch) {
  return *_dout << "mon." << mon->name << "@" << mon->rank
		<< "(" << mon->get_state_name()
		<< ").elector(" << epoch << ") ";
}


void Elector::init()
{
  epoch = mon->store->get(Monitor::MONITOR_NAME, "election_epoch");
  if (!epoch)
    epoch = 1;
  dout(1) << "init, last seen epoch " << epoch << dendl;
}

void Elector::shutdown()
{
  if (expire_event)
    mon->timer.cancel_event(expire_event);
}

void Elector::bump_epoch(epoch_t e) //epoch要持久化的，应该是一直增长的
{
  dout(10) << "bump_epoch " << epoch << " to " << e << dendl;
  assert(epoch <= e);
  epoch = e;
  MonitorDBStore::TransactionRef t(new MonitorDBStore::Transaction);//这几句应该是monitor的函数做的，不应是elector做
  t->put(Monitor::MONITOR_NAME, "election_epoch", epoch);
  mon->store->apply_transaction(t);

  mon->join_election();//join election会使mon进入 STATE_ELECTING 状态，

  // clear up some state
  electing_me = false;    //隐含清除这些，觉得比较怪
  acked_me.clear(); //下面这两个跟start()里面是重复的
  classic_mons.clear();
}


void Elector::start()//推选自己的开始一步
{
  if (!participating) {
    dout(0) << "not starting new election -- not participating" << dendl;
    return;
  }
  dout(5) << "start -- can i be leader?" << dendl;

  acked_me.clear();
  classic_mons.clear();
  init();//重新从store获取epoch，这个是个保险步骤，因为不知道是什么情况下调用的start().
  
  // start by trying to elect me
  if (epoch % 2 == 0) //epoch的偶数表明是稳定态，参见.h。但是没有对epoch做更多判断，或者跟peer的epoch做比较。
    bump_epoch(epoch+1);  // odd == election cycle
  start_stamp = ceph_clock_now(g_ceph_context);
  electing_me = true;
  acked_me[mon->rank] = CEPH_FEATURES_ALL;
  leader_acked = -1;

  // bcast to everyone else
  for (unsigned i=0; i<mon->monmap->size(); ++i) {//根据 monmap来发送。注意，monmap可以理解为根据配置来的，而quorum则是根据选举得到的。
    if ((int)i == mon->rank) continue;
    Message *m = new MMonElection(MMonElection::OP_PROPOSE, epoch, mon->monmap);
    mon->messenger->send_message(m, mon->monmap->get_inst(i));
  }
  
  reset_timer();//选举timer，参见expire()函数
}

void Elector::defer(int who) //响应别人的选举，defer(暂时不)推选自己
{
  dout(5) << "defer to " << who << dendl;

  if (electing_me) {
    // drop out
    acked_me.clear();
    classic_mons.clear();
    electing_me = false;
  }

  // ack them
  leader_acked = who;//表明我支持了who当leader. 疑问： 这个东西不需要持久化？
  ack_stamp = ceph_clock_now(g_ceph_context);
  MMonElection *m = new MMonElection(MMonElection::OP_ACK, epoch, mon->monmap);
  m->sharing_bl = mon->get_supported_commands_bl();
  mon->messenger->send_message(m, mon->monmap->get_inst(who));
  //疑问: 如果send完了ack后宕机？重启后什么情景？ 对leader的影响是什么？
  // set a timer  注意，不选举自己，也需要timer的
  reset_timer(1.0);  // give the leader some extra time to declare victory
}


void Elector::reset_timer(double plus)
{
  // set the timer
  cancel_timer();
  expire_event = new C_ElectionExpire(this);
  mon->timer.add_event_after(g_conf->mon_lease + plus,
			     expire_event);
}


void Elector::cancel_timer()
{
  if (expire_event) {
    mon->timer.cancel_event(expire_event);
    expire_event = 0;
  }
}
//正常需要等待在monmap所有的回答才成功，只有发生expire时，才按照 > 50%的原则执行。如果rank=3的先自荐，那么在它expire前，
//应该就会收到rank=2的自荐消息，rank=3的废掉自己，响应rank=2的自荐。所以实际上只要各个monitor都或者，选leader很容易成功，谁先自荐问题不大。
void Elector::expire()
{
  dout(5) << "election timer expired" << dendl;
  
  // did i win?
  if (electing_me &&
      acked_me.size() > (unsigned)(mon->monmap->size() / 2)) {//注意，expire判断的是 > monmap->size()/2，而handle_ack里面不是除以2。条件是不同的。
    // i win
    victory(); //如何彻底避免，我在宣布victory的时候，实际上还有比我rank低的，刚刚开始选举自己？而且结果应该是它赢？
  } else {//上面问题，我觉得真的发生，会让各个已经投票的在执行jion_election后改变状态，不是STATE_PEON，从而在paxos层不能形成quorum？ 但是会不会是两个leader? 先当选的不知道后面被替代了？
    // whoever i deferred to didn't declare victory quickly enough.
    if (mon->has_ever_joined)//两个分支，分别用start()和bootstrap()能看出点区别
      start();
    else
      mon->bootstrap();
  }
}


void Elector::victory()
{
  leader_acked = -1;
  electing_me = false;

  uint64_t features = CEPH_FEATURES_ALL;
  set<int> quorum;
  for (map<int, uint64_t>::iterator p = acked_me.begin(); p != acked_me.end();
       ++p) {
    quorum.insert(p->first);//ack过我的，全部进入quorum。注意，这个跟monmap就不是一码事情了，只包含选举leader过程中投票的。
    features &= p->second;//取最小集合？
  }

  // decide what command set we're supporting
  bool use_classic_commands = !classic_mons.empty();
  // keep a copy to share with the monitor; we clear classic_mons in bump_epoch
  set<int> copy_classic_mons = classic_mons;
  
  cancel_timer();
  
  assert(epoch % 2 == 1);  // election
  bump_epoch(epoch+1);     // is over! 变成偶数

  // decide my supported commands for peons to advertise
  const bufferlist *cmds_bl = NULL;
  const MonCommand *cmds;
  int cmdsize;
  if (use_classic_commands) {
    mon->get_classic_monitor_commands(&cmds, &cmdsize);
    cmds_bl = &mon->get_classic_commands_bl();
  } else {
    mon->get_locally_supported_monitor_commands(&cmds, &cmdsize);
    cmds_bl = &mon->get_supported_commands_bl();
  }
  
  // tell everyone!
  for (set<int>::iterator p = quorum.begin();
       p != quorum.end();
       ++p) {
    if (*p == mon->rank) continue;
    MMonElection *m = new MMonElection(MMonElection::OP_VICTORY, epoch, mon->monmap);
    m->quorum = quorum;
    m->quorum_features = features;
    m->sharing_bl = *cmds_bl;
    mon->messenger->send_message(m, mon->monmap->get_inst(*p));
  }
    
  // tell monitor 这边执行win_election跟peon执行lose_election不是同步的，有没有问题？
  mon->win_election(epoch, quorum, features, cmds, cmdsize, &copy_classic_mons);
}


void Elector::handle_propose(MonOpRequestRef op)//别人在推选它自己
{
  op->mark_event("elector:handle_propose");
  MMonElection *m = static_cast<MMonElection*>(op->get_req());
  dout(5) << "handle_propose from " << m->get_source() << dendl;
  int from = m->get_source().num();

  assert(m->epoch % 2 == 1); // election
  uint64_t required_features = mon->get_required_features();
  dout(10) << __func__ << " required features " << required_features
           << ", peer features " << m->get_connection()->get_features()
           << dendl;
  if ((required_features ^ m->get_connection()->get_features()) &
      required_features) {
    dout(5) << " ignoring propose from mon" << from
	    << " without required features" << dendl;
    nak_old_peer(op);
    return;
  } else if (m->epoch > epoch) {
    bump_epoch(m->epoch);
  } else if (m->epoch < epoch) {
    // got an "old" propose,
    if (epoch % 2 == 0 &&    // in a non-election cycle
	mon->quorum.count(from) == 0) {  // from someone outside the quorum，不在quorum里面的
      // a mon just started up, call a new election so they can rejoin!  为什么我要start_election? 因为其epoch太旧，不可能当选？
      dout(5) << " got propose from old epoch, quorum is " << mon->quorum 
	      << ", " << m->get_source() << " must have just started" << dendl;
      // we may be active; make sure we reset things in the monitor appropriately.
      mon->start_election();//我应该进入election状态的，这个函数实际上会走到推选自己的代码。疑问(bug?)： 这里不该return吗？还走后面啥含义？尤其是走到defer，还跟人说同意？
    } else {//已经在选举过程中，或者对方在quorum里面，注意这里直接return了。
      dout(5) << " ignoring old propose" << dendl;
      return;//对于那个epoch小的，如果总是到这个分支? =>对方会在handle_ack()里面会收到大的epoch，然后bump_epoch的
    }
  }
  //走到下面，说明对方的epoch不比我的epoch小了。
  if (mon->rank < from) {//from(对方)的rank比我大
    // i would win over them.
    if (leader_acked >= 0) {        // we already acked someone,  leader_acked在handle_victory中复位
      assert(leader_acked < from);  // and they still win, of course //比我rank大的根本不会ack
      dout(5) << "no, we already acked " << leader_acked << dendl;
    } else {//如果没有acked过，比我小的在推选它自己，那么我应该选自己才对。
      // wait, i should win!
      if (!electing_me) {
	mon->start_election();
      }
    }
  } else {//约定让rank小的candidate赢取?
    // they would win over me
    if (leader_acked < 0 ||      // haven't acked anyone yet, or
	leader_acked > from ||   // they would win over who you did ack, or
	leader_acked == from) {  // this is the guy we're already deferring to
      defer(from);//这个函数内部会发送 OP_ACK。如果刚才leader_acked有效，实际上该leader_acked对应的以前我所ack的propose，就算被废掉了。
    } else {//按照上面分析，假定rank为3先推选自己，即使收到了足够的ack，也会因为rank=2的自荐者废掉，重新来了。选举leader可能不是一次投票成功的。
      // ignore them!
      dout(5) << "no, we already acked " << leader_acked << dendl;
    }
  }
}
//要重点关注与handle_propose的对应关系
void Elector::handle_ack(MonOpRequestRef op)
{
  op->mark_event("elector:handle_ack");
  MMonElection *m = static_cast<MMonElection*>(op->get_req());
  dout(5) << "handle_ack from " << m->get_source() << dendl;
  int from = m->get_source().num();

  assert(m->epoch % 2 == 1); // election
  if (m->epoch > epoch) {//疑问： 按照.h里面的注释，表示lost track of what was going on,可能我重启了之类的
    dout(5) << "woah, that's a newer epoch, i must have rebooted.  bumping and re-starting!" << dendl;
    bump_epoch(m->epoch);
    start();
    return;
  }
  assert(m->epoch == epoch);
  uint64_t required_features = mon->get_required_features();
  if ((required_features ^ m->get_connection()->get_features()) &
      required_features) {
    dout(5) << " ignoring ack from mon" << from
	    << " without required features" << dendl;
    return;
  }
  
  if (electing_me) {
    // thanks
    acked_me[from] = m->get_connection()->get_features();
    if (!m->sharing_bl.length())
      classic_mons.insert(from);
    dout(5) << " so far i have " << acked_me << dendl;
    
    // is that _everyone_?
    if (acked_me.size() == mon->monmap->size()) {//为什么需要everyone? 注意在expire时，判断的就是> 1/2，即可认为victory()
      // if yes, shortcut to election finish
      victory();
    }
  } else {//估计可能是旧的ack消息，以前我曾经推选过自己，但是现在我已经不在推选自己了
    // ignore, i'm deferring already.
    assert(leader_acked >= 0);
  }
}


void Elector::handle_victory(MonOpRequestRef op)
{
  op->mark_event("elector:handle_victory");
  MMonElection *m = static_cast<MMonElection*>(op->get_req());
  dout(5) << "handle_victory from " << m->get_source() << " quorum_features " << m->quorum_features << dendl;
  int from = m->get_source().num();

  assert(from < mon->rank);
  assert(m->epoch % 2 == 0);  

  leader_acked = -1;

  // i should have seen this election if i'm getting the victory.
  if (m->epoch != epoch + 1) { //在victory()中，已经加1，所以是偶数，且比peon看到的大1
    dout(5) << "woah, that's a funny epoch, i must have rebooted.  bumping and re-starting!" << dendl;
    bump_epoch(m->epoch);
    start();
    return;
  }

  bump_epoch(m->epoch);
  
  // they win
  mon->lose_election(epoch, m->quorum, from, m->quorum_features);
  
  // cancel my timer
  cancel_timer();

  // stash leader's commands
  if (m->sharing_bl.length()) {
    MonCommand *new_cmds;
    int cmdsize;
    bufferlist::iterator bi = m->sharing_bl.begin();
    MonCommand::decode_array(&new_cmds, &cmdsize, bi);
    mon->set_leader_supported_commands(new_cmds, cmdsize);
  } else { // they are a legacy monitor; use known legacy command set
    const MonCommand *new_cmds;
    int cmdsize;
    mon->get_classic_monitor_commands(&new_cmds, &cmdsize);
    mon->set_leader_supported_commands(new_cmds, cmdsize);
  }
}

void Elector::nak_old_peer(MonOpRequestRef op)
{
  op->mark_event("elector:nak_old_peer");
  MMonElection *m = static_cast<MMonElection*>(op->get_req());
  uint64_t supported_features = m->get_connection()->get_features();

  if (supported_features & CEPH_FEATURE_OSDMAP_ENC) {
    uint64_t required_features = mon->get_required_features();
    dout(10) << "sending nak to peer " << m->get_source()
	     << " that only supports " << supported_features
	     << " of the required " << required_features << dendl;
    
    MMonElection *reply = new MMonElection(MMonElection::OP_NAK, m->epoch,
					   mon->monmap);
    reply->quorum_features = required_features;
    mon->features.encode(reply->sharing_bl);
    m->get_connection()->send_message(reply);
  }
}
//注意这个函数直接exit了，因为自己支持的feature不足，没法协同工作
void Elector::handle_nak(MonOpRequestRef op)
{
  op->mark_event("elector:handle_nak");
  MMonElection *m = static_cast<MMonElection*>(op->get_req());
  dout(1) << "handle_nak from " << m->get_source()
	  << " quorum_features " << m->quorum_features << dendl;

  CompatSet other;
  bufferlist::iterator bi = m->sharing_bl.begin();
  other.decode(bi);
  CompatSet diff = Monitor::get_supported_features().unsupported(other);
  
  derr << "Shutting down because I do not support required monitor features: { "
       << diff << " }" << dendl;
  
  exit(0);
  // the end!
}

void Elector::dispatch(MonOpRequestRef op)
{
  op->mark_event("elector:dispatch");
  assert(op->is_type_election());

  switch (op->get_req()->get_type()) {
    
  case MSG_MON_ELECTION:
    {
      if (!participating) {
        return;
      }
      if (op->get_req()->get_source().num() >= mon->monmap->size()) {
	dout(5) << " ignoring bogus election message with bad mon rank " 
		<< op->get_req()->get_source() << dendl;
	return;
      }

      MMonElection *em = static_cast<MMonElection*>(op->get_req());

      // assume an old message encoding would have matched
      if (em->fsid != mon->monmap->fsid) {
	dout(0) << " ignoring election msg fsid " 
		<< em->fsid << " != " << mon->monmap->fsid << dendl;
	return;
      }

      if (!mon->monmap->contains(em->get_source_addr())) {//TODO: 既然下面判断可能mon->monmap是旧的epoch的，那么这里会不会因为它mon->monmap旧了而误判？
	dout(1) << "discarding election message: " << em->get_source_addr()
		<< " not in my monmap " << *mon->monmap << dendl;
	return;
      }

      MonMap *peermap = new MonMap;
      peermap->decode(em->monmap_bl);
      if (peermap->epoch > mon->monmap->epoch) {//这个epoch，跟下面比较的em->epoch，以及epoch，应该不是相同的。应为map内部的epoch
	dout(0) << em->get_source_inst() << " has newer monmap epoch " << peermap->epoch
		<< " > my epoch " << mon->monmap->epoch 
		<< ", taking it"
		<< dendl;
	mon->monmap->decode(em->monmap_bl);
        MonitorDBStore::TransactionRef t(new MonitorDBStore::Transaction);
        t->put("monmap", mon->monmap->epoch, em->monmap_bl);//写盘了，为什么不是peermap->epoch，而是用旧的？ ==>我认为decode里面，也decode了epoch，所以相同了
        t->put("monmap", "last_committed", mon->monmap->epoch);
        mon->store->apply_transaction(t);
	//mon->monmon()->paxos->stash_latest(mon->monmap->epoch, em->monmap_bl);
	cancel_timer();
	mon->bootstrap();
	delete peermap;
	return;
      }
      if (peermap->epoch < mon->monmap->epoch) {//TODO: 这种情况下，为什么不报错 ?
	dout(0) << em->get_source_inst() << " has older monmap epoch " << peermap->epoch
		<< " < my epoch " << mon->monmap->epoch 
		<< dendl;
      } 
      delete peermap;

      switch (em->op) {// handle_propose()里面，有em->epoch < epoch的处理分支，允许小于它,并且会 start_election
      case MMonElection::OP_PROPOSE:
	handle_propose(op);
	return;
      }

      if (em->epoch < epoch) {//为什么又比较这个epoch?
	dout(5) << "old epoch, dropping" << dendl;
	break;
      }

      switch (em->op) {
      case MMonElection::OP_ACK:
	handle_ack(op);
	return;
      case MMonElection::OP_VICTORY:
	handle_victory(op);
	return;
      case MMonElection::OP_NAK:
	handle_nak(op);
	return;
      default:
	assert(0);
      }
    }
    break;
    
  default: 
    assert(0);
  }
}

void Elector::start_participating()
{
  if (!participating) {
    participating = true;
    call_election();
  }
}
