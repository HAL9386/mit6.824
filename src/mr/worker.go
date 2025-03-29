package mr

import (
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"net/rpc"
	"os"
	"sort"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

type ByKey []KeyValue

func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()

	reply := CallRequestTask()
	intermediate := []KeyValue{}
	if reply.TaskType == 0 {
		file, err := os.Open(reply.Filename)
		if err != nil {
			log.Fatalf("cannot open %v", reply.Filename)
		}
		content, err := io.ReadAll(file)
		if err != nil {
			log.Fatalf("cannot read %v", reply.Filename)
		}
		file.Close()
		kva := mapf(reply.Filename, string(content))
		intermediate := append(intermediate, kva...)
		sort.Sort(ByKey(intermediate))
	}

}

// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
	}
}

func CallRequestTask() *RequestTaskReply {
	args := RequestTaskArgs{}
	reply := RequestTaskReply{}
	ok := call("Coordinator.RequestTask", &args, &reply)
	if ok {
		fmt.Printf("RequestTask success\n")
	} else {
		fmt.Printf("RequestTask failed\n")
	}
	return &reply
}

func CallMapRequest() {
	args := MapRequestArgs{}
	reply := MapRequestReply{}
	ok := call("Coordinator.MapRequest", &args, &reply)
	if ok {
		fmt.Printf("MapRequest success\n")
	} else {
		fmt.Printf("MapRequest failed\n")
	}
}

func CallReduceRequest() {
	args := ReduceRequestArgs{}
	reply := ReduceRequestReply{}
	ok := call("Coordinator.ReduceRequest", &args, &reply)
	if ok {
		fmt.Printf("ReduceRequest success\n")
	} else {
		fmt.Printf("ReduceRequest failed\n")
	}
}

func CallMapFinished() {

}

func CallRequestFinished() {

}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
