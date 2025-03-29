package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
)

type Coordinator struct {
	// Your definitions here.
	nReduce   int       // number of reduce tasks
	nMap      int       // number of map tasks
	nDone     int       // number of finished tasks
	filenames []string  // filenames of map tasks
	issued    []bool    // whether a map task has been issued
	finished  int       // number of finished reduce tasks
}

// Your code here -- RPC handlers for the worker to call.
func (c *Coordinator) RequestTask(args *RequestTaskArgs, reply *RequestTaskReply) error {
	if c.nDone == c.nMap {
		reply.TaskType = 1
		return nil
	}
	for i, isIssued := range c.issued {
		if isIssued {
			continue
		}
		c.issued[i] = true
		reply.TaskType = 0
		reply.Filename = c.filenames[i]
		return nil
	}
	return nil
}

func (c *Coordinator) MapRequest(args *MapRequestArgs, reply *MapRequestReply) error {
	// find a file that has not been issued
	for i, isIssued := range c.issued {
		if isIssued {
			continue
		}
		c.issued[i] = true
		reply.Filename = c.filenames[i]
		return nil
	}
	return nil
}

func (c *Coordinator) ReduceRequest(args *ReduceRequestArgs, reply *ExampleReply) error {
	return nil
}

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	ret := false

	// Your code here.
	if c.finished == c.nReduce {
		ret = true
	}

	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Your code here.
	c.nReduce   = nReduce
	c.nMap      = len(files)
	c.nDone     = 0
	c.filenames = files
	c.issued    = make([]bool, c.nMap)
	c.finished  = 0
	c.server()
	return &c
}
