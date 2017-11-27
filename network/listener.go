package network

import (
  "net"
  log "github.com/lichuang/log4go"
)

type Listener struct {
  addr string
  factory SessionFactory
  listener net.Listener
}

func NewListener(addr string, factory SessionFactory) *Listener {
  return &Listener{
    addr:addr,
    factory:factory,
  }
}

func (self *Listener) Run() {
  listener, err := net.Listen("tcp", self.addr)
  if err != nil {
    log.Error("listen error: %v", err)
    return
  }
  self.listener = listener
  go self.main()
}

func (self *Listener) main() {
  for {
    conn, err := self.listener.Accept()
    if err != nil {
      continue
    }

    session := self.factory.Create(conn)
    go session.Handle()
  }
}

