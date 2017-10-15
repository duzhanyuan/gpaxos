package network

import (
  "github.com/lichuang/gpaxos"
  "fmt"
  "net"
  "strconv"
)

type ConnectionHandler func(conn net.Conn)

type Network struct {
  options *gpaxos.Options

  listener *Listener
  connections map[string]*Connection
}

func NewNetwork(options *gpaxos.Options, handler ConnectionHandler) *Network {
  connections := make(map[string]*Connection)

  var listener *Listener

  for _, node := range options.NodeList {
    addr := node.Ip + ":" + strconv.Itoa(node.Port)
    if node.Ip == options.MyNode.Ip && node.Port == options.MyNode.Port {
      listener = NewListener(addr, handler)
      listener.Run()
      continue
    }

    connections[addr] = NewConnection(addr)
  }

  return &Network{
    options:options,
    connections:connections,
    listener:listener,
  }
}

func (self *Network) Send(msg, addr string) {
  conn, exist := self.connections[addr]
  if !exist {
    fmt.Printf("%s not exist", addr)
    return
  }

  conn.Send(msg)
}

func (self *Network) Broadcast(msg string) {
  for _, conn := range self.connections {
    conn.Send(msg)
  }
}
