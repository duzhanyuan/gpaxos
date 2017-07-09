package algorithm


import (
  "github.com/golang/protobuf/proto"

  "github.com/lichuang/gpaxos/config"
  "github.com/lichuang/gpaxos/common"
  "github.com/lichuang/gpaxos/log"
  "github.com/lichuang/gpaxos/logstorage"
  "github.com/lichuang/gpaxos/util"
)

type Acceptor struct {
  base          *Base
  config        *config.Config
  acceptorState *AcceptorState
}

func NewAcceptor(config *config.Config, transport common.MsgTransport, instance *Instance, storage logstorage.LogStorage) *Acceptor {
  acceptor := new(Acceptor)
  acceptor.base = newBase(config, transport, instance)
  acceptor.acceptorState = newAcceptorState(config, storage)
  acceptor.config = config

  return acceptor
}

func (self *Acceptor) Init() error {
  var instanceId uint64
  err := self.acceptorState.Load(&instanceId)
  if err != nil {
    log.Error("load state fail:%v", err)
    return err
  }

  if instanceId == 0 {
    log.Info("empty database")
  }

  self.base.SetInstanceId(instanceId)

  return nil
}

func (self *Acceptor) NewInstance() {
  self.base.NewInstance()
  self.acceptorState.init()
}

func (self *Acceptor) GetAcceptorState() *AcceptorState {
  return self.acceptorState
}

func (self *Acceptor) OnPrepare(msg common.PaxosMsg) error {
  log.Info("start prepare msg instanceid %d, from %d, proposalid %d",
    msg.GetInstanceID(), msg.GetNodeID(), msg.GetProposalID())

  reply := common.PaxosMsg{
    InstanceID: proto.Uint64(self.base.GetInstanceId()),
    NodeID:     proto.Uint64(self.config.GetMyNodeId()),
    ProposalID: proto.Uint64(msg.GetProposalID()),
    MsgType:    proto.Int32(common.MsgType_PaxosPrepareReply),
  }

  ballot := newBallotNumber(msg.GetProposalID(), msg.GetNodeID())

  if ballot.BE(self.acceptorState.GetPromiseNum()) {
    log.Debug("[promise]promiseid %d, promisenodeid %d, preacceptedid %d, preacceptednodeid %d",
      self.acceptorState.GetPromiseNum().proposalId, self.acceptorState.GetPromiseNum().nodeId,
      self.acceptorState.GetAcceptedNum().proposalId, self.acceptorState.GetAcceptedNum().nodeId)

    reply.PreAcceptID = proto.Uint64(self.acceptorState.GetAcceptedNum().proposalId)
    reply.PreAcceptNodeID = proto.Uint64(self.acceptorState.GetAcceptedNum().nodeId)

    if self.acceptorState.GetAcceptedNum().proposalId > 0 {
      reply.Value = util.CopyBytes(self.acceptorState.AcceptValue)
    }

    self.acceptorState.PromiseNum = *ballot
    err := self.acceptorState.Persist(self.base.GetInstanceId(), self.base.GetLastChecksum())
    if err != nil {
      log.Error("persist fail, now instanceid %d ret %v", self.base.GetInstanceId(), err)
      return err
    }
  } else {
    log.Debug("[reject]promiseid %d, promisenodeid %d",
      self.acceptorState.GetPromiseNum().proposalId, self.acceptorState.GetPromiseNum().nodeId)

    reply.RejectByPromiseID = proto.Uint64(self.acceptorState.GetPromiseNum().proposalId)
  }

  replyNodeId := msg.GetNodeID()
  log.Info("end prepare instanceid %d replynodeid %d", self.base.GetInstanceId(), replyNodeId)

  self.base.SendPaxosMessage(replyNodeId, reply, common.Message_SendType_UDP)

  return nil
}

func (self *Acceptor) OnAccept(msg common.PaxosMsg) error {
  log.Info("start accept msg instanceid %d, from %d, proposalid %d, valuelen %d",
    msg.GetInstanceID(), msg.GetNodeID(), msg.GetProposalID(), len(msg.Value))

  reply := common.PaxosMsg{
    InstanceID: proto.Uint64(self.base.GetInstanceId()),
    NodeID:     proto.Uint64(self.config.GetMyNodeId()),
    ProposalID: proto.Uint64(msg.GetProposalID()),
    MsgType:    proto.Int32(common.MsgType_PaxosAcceptReply),
  }

  ballot := newBallotNumber(msg.GetProposalID(), msg.GetNodeID())

  if ballot.BE(self.acceptorState.GetPromiseNum()) {
    log.Debug("[promise]promiseid %d, promisenodeid %d, preacceptedid %d, preacceptednodeid %d",
      self.acceptorState.GetPromiseNum().proposalId, self.acceptorState.GetPromiseNum().nodeId,
      self.acceptorState.GetAcceptedNum().proposalId, self.acceptorState.GetAcceptedNum().nodeId)

    self.acceptorState.PromiseNum = *ballot
    self.acceptorState.AcceptedNum = *ballot
    self.acceptorState.AcceptValue = util.CopyBytes(msg.Value)

    err := self.acceptorState.Persist(self.base.GetInstanceId(), self.base.GetLastChecksum())
    if err != nil {
      log.Error("persist fail, now instanceid %d ret %v", self.base.GetInstanceId(), err)
      return err
    }
  } else {
    log.Debug("[reject]promiseid %d, promisenodeid %d",
      self.acceptorState.GetPromiseNum().proposalId, self.acceptorState.GetPromiseNum().nodeId)

    reply.RejectByPromiseID = proto.Uint64(self.acceptorState.GetPromiseNum().proposalId)
  }

  replyNodeId := msg.GetNodeID()
  log.Info("end accept instanceid %d replynodeid %d", self.base.GetInstanceId(), replyNodeId)

  self.base.SendPaxosMessage(replyNodeId, reply, common.Message_SendType_UDP)

  return nil
}
