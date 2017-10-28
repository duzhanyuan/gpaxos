package config

import (
  "github.com/lichuang/gpaxos/common"
  "github.com/lichuang/gpaxos/util"
)

type Config struct {
  isFollower         bool
  followToNodeId     uint64
  systemStateMachine *SystemStateMachine
  masterStateMachine *MasterStateMachine
  myFollowerMaps     map[uint64]uint64
}

func (self *Config) LogSync() bool {
  return true
}

func (self *Config) SyncInterval() int32 {
  return 5
}

func (self *Config) GetGid() uint64 {
  return 0
}

func (self *Config) GetMyNodeId() uint64 {
  return 0
}

func (self *Config) GetMajorityCount() int {
  return 0
}

func (self *Config) GetNodeCount() int {
  return 0
}

func (self *Config) IsIMFollower() bool {
  return self.isFollower
}

func (self *Config) GetFollowToNodeID() uint64 {
  return self.followToNodeId
}

func (self *Config) GetMyFollowerCount() int32 {
  return int32(len(self.myFollowerMaps))
}

func (self *Config) AddFollowerNode(followerNodeId uint64) {
  self.myFollowerMaps[followerNodeId] = util.NowTimeMs() + uint64(common.GetAskforLearnInterval() * 3)
}

func (self *Config) AddTmpNodeOnlyForLearn(nodeId uint64) {

}

func (self *Config) GetSystemVSM() *SystemStateMachine {
  return self.systemStateMachine
}

func (self *Config) GetMasterSM() *MasterStateMachine {
  return self.masterStateMachine
}

func (self *Config) CheckConfig() bool {
  return true
}

func (self *Config) GetIsUseMembership() bool {
  return false
}

func (self *Config) IsValidNodeID(nodeId uint64) bool {
  return true
}