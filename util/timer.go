package util

type Timer struct {
  TimerId uint32
  AbsTime uint64
  TimerType int
}

func NewTimer(timerId uint32, absTime uint64, timerType int) *Timer {
  return &Timer{
    TimerId:timerId,
    AbsTime:absTime,
    TimerType:timerType,
  }
}

func (self *Timer) LT(other *Timer) bool {
  if self.AbsTime == other.AbsTime {
    return self.TimerId < other.TimerId
  }

  return self.AbsTime < other.AbsTime
}

func byTimer(a, b interface{}) int {
  timer1 := a.(*Timer)
  timer2 := b.(*Timer)

  ret := timer1.LT(timer2)
  if ret {
    return -1
  }

  return 1
}
