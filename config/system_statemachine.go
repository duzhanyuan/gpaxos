package config

/*
import (
  "github.com/lichuang/gpaxos"
  "github.com/lichuang/gpaxos/storage"
)

import (
  "github.com/lichuang/gpaxos/common"
  "github.com/lichuang/gpaxos/storage"
  "github.com/golang/protobuf/proto"
  "github.com/lichuang/gpaxos"
  "errors"
  log "github.com/lichuang/log4go"
)

type MembershipChangedCallback func(idx int32, list gpaxos.NodeList)

type SystemStateMachine struct {
  InsideStateMachine

  myGroupIdx      int32
  myNodeId        uint64
  systemVariables *common.SystemVariables
  systemStore     *storage.SystemVariablesStore

  nodeIdMap                 map[uint64]bool
  membershipChangedCallback MembershipChangedCallback
}

func NewSystemStateMachine(groupIdx int32, myNodeId uint64,
  storage *storage.LogStorage,
  callback MembershipChangedCallback) *SystemStateMachine {
  return &SystemStateMachine{
    myGroupIdx:                groupIdx,
    myNodeId:                  myNodeId,
    systemStore:               storage.NewSystemVariablesStore(storage),
    nodeIdMap:                 make(map[uint64]bool),
    membershipChangedCallback: callback,
  }
}

func (self *SystemStateMachine) Init() error {
  err := self.systemStore.Read(self.myGroupIdx, self.systemVariables)
  if err != nil && err != common.ErrKeyNotFound {
    return err
  }

  if err == common.ErrKeyNotFound {
    self.systemVariables.Gid = proto.Uint64(0)
    self.systemVariables.Version = proto.Uint64(-1)
    log.Info("variables not exist")
  } else {
    self.RefleshNodeID()
    log.Info("OK, gourpidx %d gid %d version %d", self.myGroupIdx, self.systemVariables.GetGid(), self.systemVariables.GetVersion())
  }

  return nil
}

func (self *SystemStateMachine) UpdateSystemVariables(variables *common.SystemVariables) error {
  options := storage.WriteOptions{
    Sync: true,
  }

  err := self.systemStore.Write(options, self.myGroupIdx, variables)
  if err != nil {
    return err
  }

  self.systemVariables = proto.Clone(variables).(*common.SystemVariables)
  self.RefleshNodeID()
  return nil
}

func (self *SystemStateMachine) Execute(groupIdx int32, instanceId uint64, value []byte,
  ctx *gpaxos.StateMachineContext) error {
  var variables common.SystemVariables
  err := proto.Unmarshal(value, &variables)
  if err != nil {
    log.Error("Variables.ParseFromArray fail:%v", err)
    return err
  }

  var smret error
  if ctx != nil && ctx.Context != nil {
    smret = (ctx.Context).(error)
  }

  if variables.GetGid() != 0 && variables.GetGid() != self.systemVariables.GetGid() {
    log.Error("modify.gid %d not equal to now.gid %d", variables.GetGid(), self.systemVariables.GetGid())
    return errors.New("bad gid")
  }

  if variables.GetVersion() != self.systemVariables.GetVersion() {
    log.Error("modify.version %d not equal to now.version %d", variables.GetVersion(), self.systemVariables.GetVersion())
    if smret != nil {
      smret = gpaxos.Paxos_MembershipOp_GidNotSame
    }
    return nil
  }

  variables.Version = proto.Uint64(instanceId)
  err = self.UpdateSystemVariables(&variables)
  if err != nil {
    return err
  }

  log.Info("OK, new version %d gid %d", self.systemVariables.GetVersion(), self.systemVariables.GetGid())
  smret = nil
  return nil
}

func (self *SystemStateMachine) GetGid() uint64 {
  return self.systemVariables.GetGid()
}

func (self *SystemStateMachine) GetMembership(nodes gpaxos.NodeList, version *uint64) {
  *version = self.systemVariables.GetVersion()

  for i := 0; i < len(self.systemVariables.MemberShip); i++ {
    node := self.systemVariables.MemberShip[i]
    tmp := gpaxos.Node{
      NodeId: node.GetNodeid(),
    }
    nodes = append(nodes, tmp)
  }
}

func (self *SystemStateMachine) Membership_OPValue(nodes gpaxos.NodeList, version uint64, value []byte) error {
  variables := common.SystemVariables{
    Version: proto.Uint64(version),
    Gid:     proto.Uint64(self.systemVariables.GetGid()),
  }

  for _, node := range nodes {
    tmp := &common.PaxosNodeInfo{
      Rid:    proto.Uint64(0),
      Nodeid: proto.Uint64(node.NodeId),
    }

    self.systemVariables.MemberShip = append(self.systemVariables.MemberShip, tmp)
  }

  value, err := proto.Marshal(&variables)
  if err != nil {
    log.Error("Variables.Serialize fail: %v", err)
    return err
  }

  return nil
}

func (self *SystemStateMachine) CreateGid_OPValue(gid uint64, value []byte) error {
  variables := proto.Clone(self.systemVariables).(*common.SystemVariables)
  variables.Gid = proto.Uint64(gid)

  value, err := proto.Marshal(variables)
  if err != nil {
    log.Error("Variables.Serialize fail: %v", err)
    return err
  }

  return nil
}

func (self *SystemStateMachine) AddNodeIDList(nodes gpaxos.NodeList) {
  if self.systemVariables.GetGid() != 0 {
    return
  }

  self.nodeIdMap = make(map[uint64]bool)
  self.systemVariables.MemberShip = make([]*common.PaxosNodeInfo, 0)

  for _, node := range nodes {
    tmp := &common.PaxosNodeInfo{
      Rid:    proto.Uint64(0),
      Nodeid: proto.Uint64(node.NodeId),
    }

    self.systemVariables.MemberShip = append(self.systemVariables.MemberShip, tmp)
  }

  self.RefleshNodeID()
}

func (self *SystemStateMachine) RefleshNodeID() {
  self.nodeIdMap = make(map[uint64]bool)
  nodes := make([]gpaxos.Node, 0)

  for _, node := range self.systemVariables.GetMemberShip() {
    nodeInfo := gpaxos.Node{
      NodeId: node.GetNodeid(),
    }

    self.nodeIdMap[node.GetNodeid()] = true

    nodes = append(nodes, nodeInfo)
  }

  if self.membershipChangedCallback != nil {
    self.membershipChangedCallback(self.myGroupIdx, nodes)
  }
}

func (self *SystemStateMachine) GetNodeVount() int {
  return len(self.nodeIdMap)
}

func (self *SystemStateMachine) GetMajorityCount() int {
  return int(self.GetNodeVount()/2) + 1
}

func (self *SystemStateMachine) IsValidNodeID(nodeId uint64) bool {
  if self.systemVariables.GetGid() == 0 {
    return true
  }

  _, exist := self.nodeIdMap[nodeId]
  return exist
}

func (self *SystemStateMachine) IsIMInMembership() bool {
  _, exist := self.nodeIdMap[self.myNodeId]
  return exist
}

func (self *SystemStateMachine) GetCheckpointBuffer() ([]byte, error) {
  if self.systemVariables.GetVersion() == uint64(-1) ||
    self.systemVariables.GetGid() == 0 {
    return nil, nil
  }

  return proto.Marshal(self.systemVariables)
}

func (self *SystemStateMachine) GetSystemVariables(variables *common.SystemVariables) {
  variables = proto.Clone(self.systemVariables).(*common.SystemVariables)
}

func (self *SystemStateMachine) GetMembershipMap() map[uint64]bool {
  return self.nodeIdMap
}
*/