package network

import (
  "testing"
  "github.com/lichuang/gpaxos"
  "github.com/lichuang/gpaxos/log"
  "fmt"
  "net"
  "strconv"
  "bufio"
  "time"
)

func handleConnection1(conn net.Conn) {
  remoteAddr := conn.RemoteAddr().String()
  log.Debug("accept connection from %s", remoteAddr)

  for {
    line, err := bufio.NewReader(conn).ReadBytes('\n')
    if err != nil {
      break
    }

    log.Debug("msg1: %s", string(line))
  }

  log.Debug("connection from %s disconnected", remoteAddr)
}

func handleConnection2(conn net.Conn) {
  remoteAddr := conn.RemoteAddr().String()
  log.Debug("accept connection from %s", remoteAddr)

  for {
    line, err := bufio.NewReader(conn).ReadBytes('\n')
    if err != nil {
      break
    }

    log.Debug("msg2: %s", string(line))
  }

  log.Debug("connection from %s disconnected", remoteAddr)
}

func Test_basic(t *testing.T) {
  node1 := gpaxos.Node{
    Ip:"127.0.0.1",
    Port:11111,
  }
  addr1 := node1.Ip + ":" + strconv.Itoa(node1.Port)
  fmt.Printf("%s\n", addr1)

  node2 := gpaxos.Node{
    Ip:"127.0.0.1",
    Port:22222,
  }
  addr2 := node2.Ip + ":" + strconv.Itoa(node2.Port)

  nodeList := make([]gpaxos.Node, 0)
  nodeList = append(nodeList, node1)
  nodeList = append(nodeList, node2)

  options1 := &gpaxos.Options{
    MyNode: node1,
    NodeList: nodeList,
  }
  options2 := &gpaxos.Options{
      MyNode: node2,
      NodeList: nodeList,
  }

  net1 := NewNetwork(options1, handleConnection1)
  net2 := NewNetwork(options2, handleConnection2)

  net1.Send("from net1\n", addr2)
  net2.Send("from net2\n:", addr1)

  time.Sleep(1 * time.Second)
  fmt.Printf("OK")
}
