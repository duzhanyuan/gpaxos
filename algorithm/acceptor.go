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

  config        *config.Config
  state *AcceptorState
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
  return self.Base.getInstanceId()
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

  reply := common.PaxosMsg{
    InstanceID: proto.Uint64(self.GetInstanceId()),
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
      reply.Value = util.CopyBytes(self.acceptorState.acceptValues)
    }

    self.acceptorState.SetPromiseNum(ballot)

    err := self.acceptorState.Persist(self.GetInstanceId(), self.GetLastChecksum())
    if err != nil {
      log.Error("persist fail, now instanceid %d ret %v", self.GetInstanceId(), err)
      return err
    }
  } else {
    log.Debug("[reject]promiseid %d, promisenodeid %d",
      self.acceptorState.GetPromiseNum().proposalId, self.acceptorState.GetPromiseNum().nodeId)

    reply.RejectByPromiseID = proto.Uint64(self.acceptorState.GetPromiseNum().proposalId)
  }

  replyNodeId := msg.GetNodeID()
  log.Info("end prepare instanceid %d replynodeid %d", self.GetInstanceId(), replyNodeId)

  self.SendPaxosMessage(replyNodeId, reply, common.Message_SendType_UDP)

  return nil
}

// handle paxos accept msg
func (self *Acceptor) onAccept(msg *common.PaxosMsg) error {
  return nil
}
