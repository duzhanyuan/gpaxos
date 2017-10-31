package network

import (
  "github.com/lichuang/gpaxos"
  "net"
  "strconv"

  log "github.com/lichuang/log4go"
  "github.com/lichuang/gpaxos/common"
)

type ConnectionHandler func(conn net.Conn)

type Network struct {
  options *gpaxos.Options

  listener *Listener
  connections map[uint64]*Connection
}

func NewNetwork(options *gpaxos.Options, handler ConnectionHandler) *Network {
  connections := make(map[uint64]*Connection)

  var listener *Listener

  for _, node := range options.NodeList {
    addr := node.Ip + ":" + strconv.Itoa(node.Port)
    if node.Ip == options.MyNode.Ip && node.Port == options.MyNode.Port {
      listener = NewListener(addr, handler)
      listener.Run()
      continue
    }

    connections[node.Id] = NewConnection(addr)
  }

  return &Network{
    options:options,
    connections:connections,
    listener:listener,
  }
}

func (self *Network) Send(nodeid uint64, msg []byte) error {
  conn, exist := self.connections[nodeid]
  if !exist {
    log.Error("%d not exist", nodeid)
    return common.ErrNodeNotFound
  }

  conn.Send(msg)
  return nil
}

func (self *Network) Broadcast(msg []byte) error {
  for _, conn := range self.connections {
    conn.Send(msg)
  }

  return nil
}
