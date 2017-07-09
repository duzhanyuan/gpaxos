package config

import (
  "github.com/lichuang/gpaxos/common"
  "github.com/lichuang/gpaxos/util"
)

type Config struct {
  IsFollower bool
  FollowToNodeId uint64
  SystemStateMachine *SystemStateMachine
  MyFollowerMap map[uint64]uint64
}

func (self *Config) LogSync() bool {
  return true
}

func (self *Config) SyncInterval() int32 {
  return 5
}

func (self *Config) GetMyGroupIdx() int32 {
  return 0
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
  return self.IsFollower
}

func (self *Config) GetFollowToNodeID() uint64 {
  return self.FollowToNodeId
}

func (self *Config) GetMyFollowerCount() int32 {
  return int32(len(self.MyFollowerMap))
}

func (self *Config) AddFollowerNode(followerNodeId uint64) {
  self.MyFollowerMap[followerNodeId] = util.NowTimeMs() + uint64(common.GetAskforLearnInterval() * 3)
}

func (self *Config) AddTmpNodeOnlyForLearn(nodeId uint64) {

}

func (self *Config) GetSystemVSM() *SystemStateMachine {
  return self.SystemStateMachine
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