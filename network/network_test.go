package network

import (
  "testing"
  "github.com/lichuang/gpaxos"
  "fmt"
  "net"
  "bufio"
  "time"
)

type TestSessionFactory struct {

}

func (self *TestSessionFactory)Create(conn net.Conn) Session {
  return NewTestSession(conn)
}

type TestSession struct {
  conn net.Conn
}

func NewTestSession(conn net.Conn) *TestSession {
  return &TestSession{
    conn:conn,
  }
}

func (self *TestSession)Handle() {
  conn := self.conn
  remoteAddr := conn.RemoteAddr().String()
  fmt.Printf("accept connection from %s", remoteAddr)

  for {
    line, err := bufio.NewReader(conn).ReadBytes('\n')
    if err != nil {
      break
    }

    fmt.Printf("msg: %s", string(line))
  }

  fmt.Printf("connection from %s disconnected", remoteAddr)
}

func Test_basic(t *testing.T) {
  node1 := gpaxos.NewNode("127.0.0.1", 11111)
  node2 := gpaxos.NewNode("127.0.0.1", 22222)

  nodeList := make([]*gpaxos.Node, 0)
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

  net1 := NewNetwork(options1, &TestSessionFactory{})
  net2 := NewNetwork(options2, &TestSessionFactory{})

  net1.SendMessage(node2.Id,[]byte("from net1\n"))
  net2.SendMessage(node1.Id,[]byte("from net2\n"))

  time.Sleep(1 * time.Second)
  fmt.Printf("OK")
}
