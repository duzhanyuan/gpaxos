package network

import (
  "net"
  log "github.com/lichuang/log4go"
  "time"
	"sync"
)

const (
  MaxWaitIOTime = time.Millisecond * 100
)

type Listener struct {
  *net.TCPListener

  addr string
  factory SessionFactory
  stopChan chan bool
  end bool
  sessionMap map[string]Session
  mutex sync.Mutex
}

func NewListener(addr string, factory SessionFactory) *Listener {
  return &Listener{
    addr:addr,
    factory:factory,
    stopChan:make(chan bool),
    end:false,
    sessionMap:make(map[string]Session),
  }
}

func (self *Listener) Run() {
  listener, err := net.Listen("tcp", self.addr)
  if err != nil {
    log.Error("listen error: %v", err)
    return
  }
  tcplistener, ok := listener.(*net.TCPListener)
  if !ok {
    log.Error("wrap tcp listener error")
    return
  }
  self.TCPListener = tcplistener
  go self.main()
}

func (self *Listener) main() {
  for {
    self.SetDeadline(time.Now().Add(MaxWaitIOTime))

    conn, err := self.TCPListener.Accept()

    select {
    case <- self.stopChan:
      if err == nil {
        conn.Close()
      }
      return
    default:
      break
    }

    if err != nil {
      netErr, ok := err.(net.Error)

      if ok && netErr.Timeout() && netErr.Temporary() {
        continue
      }
    }

    if self.end {
      return
    }
    session := self.factory.Create(conn)
    addr := conn.RemoteAddr().String()

    self.mutex.Lock()
    self.sessionMap[addr] = session
		self.mutex.Unlock()

    go func(addr string) {
			session.Handle()

			log.Info("close connection from %s", addr)
			self.mutex.Lock()
			delete(self.sessionMap, addr)
			self.mutex.Unlock()
		} (addr)
  }
}

func (self *Listener) Stop() {
  self.end = true
  close(self.stopChan)
  self.TCPListener.Close()

  self.mutex.Lock()
  for _, session := range self.sessionMap {
    session.Stop()
  }
	self.mutex.Unlock()
}