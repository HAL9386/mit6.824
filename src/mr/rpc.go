package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"strconv"
	"time"
)

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

// Add your RPC definitions here.
type RequestTaskArgs struct {
}

type RequestTaskReply struct {
	TaskType  int        // 0: map, 1: reduce, 2: wait, 3: done
	Filenames []string   // filenames for task either map or reduce
	TaskID    int        // taskid 
	NReduce   int        // nreduce 
	AsignedAt time.Time  // 
}

type TaskReportArgs struct {
	TaskID     int       // taskid
	TaskType   int       // tasktype
	OFilenames []string  // map task report intermediate filenames, reduce task report result
}

type TaskReportReply struct {
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
