package network

import (
  "github.com/lichuang/gpaxos"
  "strconv"

  log "github.com/lichuang/log4go"
  "github.com/lichuang/gpaxos/common"
)

type Network struct {
  options *gpaxos.Options

  listener *Listener
  connections map[uint64]*Connection
}

func NewNetwork(options *gpaxos.Options, factory SessionFactory) *Network {
  connections := make(map[uint64]*Connection)

  var listener *Listener

  for _, node := range options.NodeList {
    addr := node.Ip + ":" + strconv.Itoa(node.Port)
    if node.Ip == options.MyNode.Ip && node.Port == options.MyNode.Port {
      listener = NewListener(addr, factory)
      listener.Run()
      continue
    }

    connections[node.Id] = NewConnection(addr, node.Id)
  }

  return &Network{
    options:options,
    connections:connections,
    listener:listener,
  }
}

func (self *Network) SendMessage(nodeid uint64, msg []byte) error {
  conn, exist := self.connections[nodeid]
  if !exist {
    log.Error("%d not exist", nodeid)
    return common.ErrNodeNotFound
  }

  go conn.Send(msg)
  return nil
}

func (self *Network) BroadcastMessage(msg []byte) error {
  for _, conn := range self.connections {
    go conn.Send(msg)
  }

  return nil
}

func (self *Network) Close() {
  self.listener.Stop()
}