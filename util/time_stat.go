
package util

type TimeStat struct {
  Time uint64
}

func NewTimeStat() *TimeStat{
  return &TimeStat{
    Time: NowTimeMs(),
  }
}

func (self *TimeStat) Point() uint64 {
  nowTime := NowTimeMs()
  var passTime uint64
  if nowTime > self.Time {
    passTime = nowTime - self.Time
  }

  self.Time = nowTime

  return passTime
}
