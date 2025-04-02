package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

type Task struct {
	Status    int        // 0: pending, 1: in progress, 2: completed
	ID        int        // task ID
	Type      int        // 0: map, 1: reduce, 2: wait, 3: done
	Filenames []string   // text files or intermediate files
	AsignedAt time.Time  // time when the task was assigned
}

type Coordinator struct {
	// Your definitions here.
	nReduce     int         // number of reduce tasks
	isMapDone   bool        // true if all map tasks are done
	mapTasks    []Task      // list of map tasks length is equal to number of files
	reduceTasks []Task      // list of reduce tasks length is equal to nReduce
	mu          sync.Mutex  // mutex to protect shared data
}

// Your code here -- RPC handlers for the worker to call.
func (c *Coordinator) RequestTask(arg *RequestTaskArgs, reply *RequestTaskReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	// try to assign a map task
	if c.asignMapTask(arg, reply) {
		return nil
	}
	// all map tasks are issued
	// check if all map tasks are done
	if !c.isAllMapTaskDone() {
		c.assignWaitTask(arg, reply)
		return nil
	}
	// try to assign a reduce task
	if c.asignReduceTask(arg, reply) {
		return nil
	}
	// all reduce tasks are issued
	// check if all reduce tasks are done(e.g. all tasks are completed)
	if !c.Done() {
		c.assignWaitTask(arg, reply)
		return nil
	}
	// all tasks are done
	c.assignExitTask(arg, reply)
	return nil
}

func (c *Coordinator) asignMapTask(arg *RequestTaskArgs, reply *RequestTaskReply) bool {
	for i := 0; i < len(c.mapTasks); i++ {
		if c.mapTasks[i].Status == 0 {
			reply.TaskType = 0
			reply.Filename = c.mapTasks[i].Filenames[0]
			reply.TaskID = c.mapTasks[i].ID
			c.mapTasks[i].Status = 1
			c.mapTasks[i].AsignedAt = time.Now()
			return true
		}
	}
	return false
}

func (c *Coordinator) asignReduceTask(arg *RequestTaskArgs, reply *RequestTaskReply) bool {
	for i := 0; i < len(c.reduceTasks); i++ {
		if c.reduceTasks[i].Status == 0 {
			reply.TaskType = 1
			reply.Filenames = nil
			reply.TaskID = c.reduceTasks[i].ID
			c.reduceTasks[i].Status = 1
			c.reduceTasks[i].AsignedAt = time.Now()
			return true
		}
	}
	return false
}

func (c *Coordinator) assignWaitTask(arg *RequestTaskArgs, reply *RequestTaskReply) {
	reply.TaskType  = 2    // wait task
	reply.Filenames = nil
	reply.TaskID    = -1
}

func (c *Coordinator) assignExitTask(arg *RequestTaskArgs, reply *RequestTaskReply) {
	reply.TaskType  = 3    // exit task
	reply.Filenames = nil
	reply.TaskID    = -1
}

func (c *Coordinator) checkMapTaskTimeout() {
	c.mu.Lock()
	defer c.mu.Unlock()
	for i := 0; i < len(c.mapTasks); i++ {
		if c.mapTasks[i].Status == 1 && time.Since(c.mapTasks[i].AsignedAt) > 10*time.Second {
			c.mapTasks[i].Status = 0
		}
	}
}

func (c *Coordinator) isAllMapTaskDone() bool {
	// check if any map task is not done
	for _, task := range c.mapTasks {
		if task.Status != 2 {
			return false
		}
	}
	return true
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
	// Your code here.
	c.mu.Lock()
	defer c.mu.Unlock()
	// check if all map tasks are done
	for _, task := range c.reduceTasks {
		if task.Status != 2 {
			return false
		}
	}
	return true 
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Your code here.
	c.nReduce   = nReduce
	c.isMapDone = false
	c.mapTasks  = make([]Task, len(files))
	c.reduceTasks = make([]Task, nReduce)
	for i := 0; i < len(files); i++ {
		c.mapTasks[i].ID = i
		c.mapTasks[i].Type = 0
		c.mapTasks[i].Filenames = files[i : i+1]
		c.mapTasks[i].Status = 0
	}
	for i := 0; i < nReduce; i++ {
		c.reduceTasks[i].ID = i
		c.reduceTasks[i].Type = 1
		c.reduceTasks[i].Filenames = make([]string, 0)
		c.reduceTasks[i].Status = 0
	}
	c.server()
	return &c
}
