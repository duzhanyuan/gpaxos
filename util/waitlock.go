package util

import (
	"time"
	"sync"
	"errors"
	log "github.com/lichuang/log4go"
)

var Waitlock_Timeout = errors.New("waitlock timeout")

type Waitlock struct {
	mutex sync.Mutex
	inUse bool
	waitChan chan bool
	waitCount int32
}

func NewWaitlock() *Waitlock {
	return &Waitlock{
		inUse:false,
		waitChan: make(chan bool, 0),
		waitCount:0,
	}
}

func (self *Waitlock) Lock(waitMs int) (int, error) {
	log.Debug("in lock")
	timeOut := false

	now := NowTimeMs()
	getLock := false

	self.mutex.Lock()
	if !self.inUse {
		self.inUse = true
		getLock = true
	} else {
		//atomic.AddInt32(&self.waitCount, 1)
		self.waitCount += 1
	}
	self.mutex.Unlock()

	if getLock {
		// assume there is no time cost
		return 0, nil
	}

	timer := time.NewTimer(time.Duration(waitMs) * time.Millisecond)
	select {
	case <- timer.C:
		timeOut = true
		break
	case <- self.waitChan:
		break
	}

	self.mutex.Lock()
	//atomic.AddInt32(&self.waitCount, -1)
	self.waitCount += 1
	self.mutex.Unlock()
	if timeOut {
		log.Debug("lock timeout")
		return -1, Waitlock_Timeout
	}
	timer.Stop()

	return int(NowTimeMs() - now), nil
}

func (self *Waitlock) Unlock() {
	log.Debug("in unlock")
	self.mutex.Lock()
	self.inUse = false
	if self.waitCount == 1 {
		self.mutex.Unlock()
		return
	}
	self.mutex.Unlock()

	timeOut := false
	timer := time.NewTimer(time.Duration(1) * time.Millisecond)
	select {
	case <- timer.C:
		timeOut = true
		break
	case self.waitChan <- true:
		break
	}

	log.Debug("unlock")
	if !timeOut {
		timer.Stop()
	}
}