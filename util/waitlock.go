package util

const WAIT_LOCK_USERTIME_AVG_INTERVAL = 250

type WaitLock struct {
  serialock *TimeoutCond
  isLockUsing bool
  waitLockCount int
  maxWaitLockCount int
  lockUseTimeSum int
  avgLockUseTime int
  lockUseTimeCount int
  rejectRate int
  lockWaitTimeThresholdMs int
}

func NewWaitLock() *WaitLock {
  return &WaitLock{
    serialock:NewTimeoutCond(),
    isLockUsing:false,
    waitLockCount:0,
    maxWaitLockCount:-1,
    lockUseTimeSum:0,
    avgLockUseTime:0,
    lockUseTimeCount:0,
    rejectRate:0,
    lockWaitTimeThresholdMs:-1,
  }
}

func (self *WaitLock) CanLock() bool {
  if self.maxWaitLockCount != -1 && self.waitLockCount >= self.maxWaitLockCount {
    return false
  }

  if self.lockWaitTimeThresholdMs == -1 {
    return true
  }

  return Rand() % 100 >= uint32(self.rejectRate)
}

func (self *WaitLock) RefleshRejectRate(useTimeMs uint64) {
  if self.lockWaitTimeThresholdMs == -1 {
    return
  }

  self.lockUseTimeSum += int(useTimeMs)
  self.lockUseTimeCount++

  if self.lockUseTimeCount >= WAIT_LOCK_USERTIME_AVG_INTERVAL {
    self.avgLockUseTime = self.lockUseTimeSum / self.lockUseTimeCount
    self.lockUseTimeSum = 0
    self.lockUseTimeCount = 0

    if self.avgLockUseTime > self.lockWaitTimeThresholdMs {
      if self.rejectRate != 98 {
        self.rejectRate += 3
        if self.rejectRate > 98 {
          self.rejectRate = 98
        }
      }
    } else {
      if self.rejectRate != 0 {
        self.rejectRate -= 3
        if self.rejectRate < 0 {
          self.rejectRate = 0
        }
      }
    }
  }
}

func (self *WaitLock) SetMaxWaitLogCount(maxWaitLockCount int) {
  self.maxWaitLockCount = maxWaitLockCount
}

func (self *WaitLock) SetLockWaitTimeThreshold(lockWaitTimeThresholdMs int) {
  self.lockWaitTimeThresholdMs = lockWaitTimeThresholdMs
}

func (self *WaitLock) Lock(timeoutMs uint64, useTimeMs *uint64) bool {
  beginTime := NowTimeMs()

  self.serialock.Lock()
  if !self.CanLock() {
    *useTimeMs = 0
    self.serialock.Unlock()
    return false
  }

  self.waitLockCount++
  getLock := false

  for self.isLockUsing {
    if timeoutMs == -1 {
      self.serialock.WaitFor(1000)
      continue
    } else {
      if self.serialock.WaitFor(int(timeoutMs)) {
        getLock = false
        break
      }
    }
  }

  self.waitLockCount--
  endTime := NowTimeMs()
  *useTimeMs = endTime - beginTime

  self.RefleshRejectRate(*useTimeMs)

  if getLock {
    self.isLockUsing = true
  }
  self.serialock.Unlock()

  return getLock
}

func (self *WaitLock) Unlock() {
  self.serialock.Lock()
  self.isLockUsing = false
  self.serialock.Signal()
  self.serialock.Unlock()
}

func (self *WaitLock) GetNowHoldThreadCount() int {
  return self.waitLockCount
}

func (self *WaitLock) GetNowAvgThreadWaitTime() int {
  return self.avgLockUseTime
}

func (self *WaitLock) GetNowRejectRate() int {
  return self.rejectRate
}