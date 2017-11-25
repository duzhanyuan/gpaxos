package algorithm

const (
  PrepareTimer = iota
  AcceptTimer
  LearnerTimer
)

// instance id status
type Status int
const (
  Decided   = iota + 1
  Pending   // not yet decided.
)
