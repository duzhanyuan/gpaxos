package network

import (
  "net"
)

type Listener struct {
  addr string
  handler ConnectionHandler
  listener net.Listener
}

func NewListener(addr string, handler ConnectionHandler) *Listener {
  return &Listener{
    addr:addr,
    handler:handler,
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

    go self.handler(conn)
  }
}

