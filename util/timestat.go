package util

import (
  "time"
  "github.com/lichuang/gpaxos/util"
)

type TimeStat struct {
  Time uint64
}

func NewTimeStat() *TimeStat{
  return &TimeStat{
    Time: util.NowTimeMs(),
  }
}

func (self *TimeStat) Point() uint64 {
  nowTime := util.NowTimeMs()
  var passTime uint64
  if nowTime > self.Time {
    passTime = nowTime - self.Time
  }

  self.Time = nowTime

  return passTime
}
