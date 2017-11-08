package network

import (
  "net"
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
  listener, _ := net.Listen("tcp", self.addr)
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

