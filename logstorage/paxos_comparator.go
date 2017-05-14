package logstorage

type PaxosComparator struct {
}

func (self *PaxosComparator) Compare(a, b []byte) int {
	return 0
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
