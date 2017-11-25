package storage

import "strconv"

type PaxosComparator struct {
}

func (self *PaxosComparator) Compare(a, b []byte) int {
  ua, _ := strconv.ParseUint(string(a), 10, 64)
  ub, _ := strconv.ParseUint(string(b), 10, 64)

  if ua == ub {
    return 0
  }

  if ua < ub {
    return -1
  }
  return 1
}

func (self *PaxosComparator) Name() string {
  return "paxos_comparator"
}

func (self *PaxosComparator) Separator(dst, a, b []byte) []byte {
  return nil
}

func (self *PaxosComparator) Successor(dst, b []byte) []byte {
  return nil
}
