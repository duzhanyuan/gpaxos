package util

import (
  "testing"
  "sync"
  "time"
)

type TimerTester struct {
  now uint64
  diff int
  i int
  wg *sync.WaitGroup
  fired bool
  t *testing.T
}

func newTimerTester(wg *sync.WaitGroup, now uint64, i int, diff int, t *testing.T) *TimerTester {
  return &TimerTester{
    wg:wg,
    now:now,
    diff:diff,
    i:i,
    fired:false,
    t:t,
  }
}

func (self *TimerTester) OnTimeout(timer *Timer) {
  now := NowTimeMs()

  self.fired = true
  diff := now - self.now
  // TODO: the error is too big!!
  TestAssert(self.t,
    diff >= uint64(self.diff - 5) && diff <= uint64(self.diff) + 5,
    "timer %d error, diff:%d, expected: %d, now:%d\n", self.i, diff, self.diff, self.now)
  if self.wg != nil {
    self.wg.Done()
  }
}

func TestTimeerThread(t *testing.T) {
  thread := NewTimerThread()
  cnt := 100
  start := 100

  var wg sync.WaitGroup
  wg.Add(cnt)

  for i:=0;i<cnt;i++ {
    now := NowTimeMs()
    timer_tester := newTimerTester(&wg, now, i,i+start,t)
    thread.AddTimer(uint32(start + i), 0, timer_tester)
  }

  wg.Wait()
}

func TestDelTimer(t *testing.T) {
  thread := NewTimerThread()
  now := NowTimeMs()
  timer_tester := newTimerTester(nil, now, 0,1000,t)
  id := thread.AddTimer(1000, 0, timer_tester)

  thread.DelTimer(id)

  // wait timeout
  time.Sleep(1500 * time.Millisecond)

  TestAssert(t,
    timer_tester.fired == false,
      "timer should not be fired")
}
