package config

type Config struct {
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
