package algorithm

import (
  "github.com/golang/protobuf/proto"

  "github.com/lichuang/gpaxos/common"
  "github.com/lichuang/gpaxos/config"
  "github.com/lichuang/gpaxos/util"

  log "github.com/lichuang/log4go"
)

type Acceptor struct {
  Base

  config  *config.Config
  state   *AcceptorState
}

func NewAcceptor(instance *Instance) *Acceptor{
  return &Acceptor{
    Base:          newBase(instance),
    state: newAcceptorState(instance.config, instance.logStorage),
    config:        instance.config,
  }
}

func (self *Acceptor) Init() error {
  instanceId, err := self.state.Load()
  if err != nil {
    log.Error("load state fail:%v", err)
    return err
  }

  if instanceId == 0 {
    log.Info("empty database")
  }

  self.SetInstanceId(instanceId)

  log.Info("OK")

  return nil
}

func (self *Acceptor) GetInstanceId() uint64 {
  return self.Base.GetInstanceId()
}

func (self *Acceptor) SetInstanceId(instanceId uint64) {
  self.Base.setInstanceId(instanceId)
}

func (self *Acceptor) InitForNewPaxosInstance() {
  self.state.init()
}

func (self *Acceptor) NewInstance() {
  self.Base.newInstance()
  self.state.init()
}

func (self *Acceptor) GetAcceptorState() *AcceptorState {
  return self.state
}

// handle paxos prepare msg
func (self *Acceptor) onPrepare(msg *common.PaxosMsg) error {
  log.Info("start prepare msg instanceid %d, from %d, proposalid %d",
    msg.GetInstanceID(), msg.GetNodeID(), msg.GetProposalID())

  reply := &common.PaxosMsg{
    InstanceID: proto.Uint64(self.GetInstanceId()),
    NodeID:     proto.Uint64(self.config.GetMyNodeId()),
    ProposalID: proto.Uint64(msg.GetProposalID()),
    MsgType:    proto.Int32(common.MsgType_PaxosPrepareReply),
  }

  ballot := NewBallotNumber(msg.GetProposalID(), msg.GetNodeID())
  state := self.state

  if ballot.BE(state.GetPromiseNum()) {
    log.Debug("[promise]promiseid %d, promisenodeid %d, preacceptedid %d, preacceptednodeid %d",
      state.GetPromiseNum().proposalId, state.GetPromiseNum().nodeId,
      state.GetAcceptedNum().proposalId, state.GetAcceptedNum().nodeId)

    reply.PreAcceptID = proto.Uint64(state.GetAcceptedNum().proposalId)
    reply.PreAcceptNodeID = proto.Uint64(state.GetAcceptedNum().nodeId)

    if state.GetAcceptedNum().proposalId > 0 {
      reply.Value = util.CopyBytes(state.GetAcceptedValue())
    }

    state.SetPromiseNum(ballot)

    err := state.Persist(self.GetInstanceId(), self.Base.GetLastChecksum())
    if err != nil {
      log.Error("persist fail, now instanceid %d ret %v", self.GetInstanceId(), err)
      return err
    }
  } else {
    log.Debug("[reject]promiseid %d, promisenodeid %d",
      state.GetPromiseNum().proposalId, state.GetPromiseNum().nodeId)

    reply.RejectByPromiseID = proto.Uint64(state.GetPromiseNum().proposalId)
  }

  replyNodeId := msg.GetNodeID()
  log.Info("end prepare instanceid %d replynodeid %d", self.GetInstanceId(), replyNodeId)

  self.Base.sendPaxosMessage(replyNodeId, reply)

  return nil
}

// handle paxos accept msg
func (self *Acceptor) onAccept(msg *common.PaxosMsg) error {
  log.Info("start accept msg instanceid %d, from %d, proposalid %d, valuelen %d",
    msg.GetInstanceID(), msg.GetNodeID(), msg.GetProposalID(), len(msg.Value))

  reply := &common.PaxosMsg{
    InstanceID: proto.Uint64(self.GetInstanceId()),
    NodeID:     proto.Uint64(self.config.GetMyNodeId()),
    ProposalID: proto.Uint64(msg.GetProposalID()),
    MsgType:    proto.Int32(common.MsgType_PaxosAcceptReply),
  }

  ballot := NewBallotNumber(msg.GetProposalID(), msg.GetNodeID())
  state := self.state

  if ballot.BE(state.GetPromiseNum()) {
    log.Debug("[promise]promiseid %d, promisenodeid %d, preacceptedid %d, preacceptednodeid %d",
      state.GetPromiseNum().proposalId, state.GetPromiseNum().nodeId,
      state.GetAcceptedNum().proposalId, state.GetAcceptedNum().nodeId)

    state.SetPromiseNum(ballot)
    state.SetAcceptedNum(ballot)
    state.SetAcceptedValue(msg.Value)

    err := state.Persist(self.GetInstanceId(), self.Base.GetLastChecksum())
    if err != nil {
      log.Error("persist fail, now instanceid %d ret %v", self.GetInstanceId(), err)
      return err
    }
  } else {
    log.Debug("[reject]promiseid %d, promisenodeid %d",
      state.GetPromiseNum().proposalId, state.GetPromiseNum().nodeId)

    reply.RejectByPromiseID = proto.Uint64(state.GetPromiseNum().proposalId)
  }

  replyNodeId := msg.GetNodeID()
  log.Info("end accept instanceid %d replynodeid %d", self.GetInstanceId(), replyNodeId)

  self.Base.sendPaxosMessage(replyNodeId, reply)

  return nil
}
