package config

type Config struct {
  IsFollower bool
  FollowToNodeId uint64
  SystemStateMachine *SystemStateMachine
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

func (self *Config) AddFollowerNode(followerNodeId uint64) {

}

func (self *Config) GetSystemVSM() *SystemStateMachine {
  return self.SystemStateMachine
}