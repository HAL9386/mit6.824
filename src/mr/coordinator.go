package mr

import (
	"fmt"
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
	nReduce        int         // number of reduce tasks
	isMapDone      bool        // true if all map tasks are done
	mapTasks       []Task      // list of map tasks length is equal to number of files
	reduceTasks    []Task      // list of reduce tasks length is equal to nReduce
	interFilenames []string    // map task report outfile so that coor could give it to reduce task without searching mr-*-Y
	mu             sync.Mutex  // mutex to protect shared data
}

// Your code here -- RPC handlers for the worker to call.
func (c *Coordinator) RequestTask(arg *RequestTaskArgs, reply *RequestTaskReply) error {
	// try to assign a map task
	if ok, taskID := c.asignMapTask(arg, reply); ok {
		fmt.Printf("Map task [%d] assigned to worker\n", taskID)
		go c.scheduleTaskTimeout(0, taskID)
		return nil
	}
	// all map tasks are issued
	// check if all map tasks are done
	if !c.isAllMapTaskDone() {
		c.assignWaitTask(arg, reply)
		fmt.Printf("Map task not done, waiting...\n")
		return nil
	}
	// try to assign a reduce task
	if ok, taskID := c.asignReduceTask(arg, reply); ok {
		fmt.Printf("Reduce task [%d] assigned to worker\n", taskID)
		go c.scheduleTaskTimeout(1, taskID)
		return nil
	}
	// all reduce tasks are issued
	// check if all reduce tasks are done(e.g. all tasks are completed)
	if !c.Done() {
		c.assignWaitTask(arg, reply)
		fmt.Printf("Reduce task not done, waiting...\n")
		return nil
	}
	// all tasks are done
	c.assignExitTask(arg, reply)
	fmt.Printf("All tasks are done, exiting...\n")
	return nil
}

func (c *Coordinator) TaskReport(arg *TaskReportArgs, reply *TaskReportReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	// mark the task as done
	if arg.TaskType == 0 {  // is a map task 
		c.mapTasks[arg.TaskID].Status = 2
		c.interFilenames = append(c.interFilenames, arg.OFilenames...)
	} else if arg.TaskType == 1 {  // is a reduce task
		c.reduceTasks[arg.TaskID].Status = 2
	}
	return nil
}

func (c *Coordinator) asignMapTask(arg *RequestTaskArgs, reply *RequestTaskReply) (bool, int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for i := 0; i < len(c.mapTasks); i++ {
		if c.mapTasks[i].Status == 0 {
			reply.TaskType  = 0
			reply.Filenames = c.mapTasks[i].Filenames
			reply.TaskID    = c.mapTasks[i].ID
			reply.NReduce   = c.nReduce
			c.mapTasks[i].Status    = 1
			c.mapTasks[i].AsignedAt = time.Now()
			return true, i
		}
	}
	return false, -1
}

func (c *Coordinator) asignReduceTask(arg *RequestTaskArgs, reply *RequestTaskReply) (bool, int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for i := 0; i < len(c.reduceTasks); i++ {
		if c.reduceTasks[i].Status == 0 {
			reply.TaskType  = 1
			reply.Filenames = c.interFilenames
			reply.TaskID    = c.reduceTasks[i].ID
			c.reduceTasks[i].Status    = 1
			c.reduceTasks[i].AsignedAt = time.Now()
			return true, i
		}
	}
	return false, -1
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

func (c *Coordinator) scheduleTaskTimeout(taskType int, taskID int) {
	<-time.After(10 * time.Second)
	c.mu.Lock()
	defer c.mu.Unlock()
	// If the task is still in progress after 10 seconds, mark it as pending
	if taskType == 0 {
		if c.mapTasks[taskID].Status == 1 {
			log.Printf("Map task %d timed out, reassigning.", taskID)
			c.mapTasks[taskID].Status = 0
		}
	} else if taskType == 1 {
		if c.reduceTasks[taskID].Status == 1 {
			log.Printf("Reduce task %d timed out, reassigning.", taskID)
			c.reduceTasks[taskID].Status = 0
		}
	}
}

func (c *Coordinator) isAllMapTaskDone() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
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
	c.nReduce     = nReduce
	c.isMapDone   = false
	c.mapTasks    = make([]Task, len(files))
	c.reduceTasks = make([]Task, nReduce)
	for i := 0; i < len(files); i++ {
		c.mapTasks[i].ID        = i
		c.mapTasks[i].Type      = 0
		c.mapTasks[i].Status    = 0
		c.mapTasks[i].Filenames = files[i : i+1]
	}
	for i := 0; i < nReduce; i++ {
		c.reduceTasks[i].ID        = i
		c.reduceTasks[i].Type      = 1
		c.reduceTasks[i].Status    = 0
		c.reduceTasks[i].Filenames = nil  // the input files of the reduce task follow the naming convention 
	}
	c.server()
	return &c
}
