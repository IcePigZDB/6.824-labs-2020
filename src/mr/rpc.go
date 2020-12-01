package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"strconv"
)

// RegWorkerArgs regworker need not args
type RegWorkerArgs struct {
}

// RegWorkerReply  reply workerID
type RegWorkerReply struct {
	WorkerID uint
}

// ReqTaskArgs rpc args for woker to request task
type ReqTaskArgs struct {
	WorkerID uint
}

// ReqTaskReply rpc args for master to return task
type ReqTaskReply struct {
	Task *Task
}

// ReportTaskArgs rpc args for worker to report
type ReportTaskArgs struct {
	Done     bool
	Seq      uint
	Phase    TaskPhase
	WorkerID uint
}

// ReportTaskReply need no return
type ReportTaskReply struct {
}

// Add your RPC definitions here.

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the master.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func masterSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
