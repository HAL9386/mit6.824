package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"net/rpc"
	"os"
	"sort"
	"time"
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

	for {
		reply := CallRequestTask()
		switch reply.TaskType {
		case 0:  // map task
			doMapTask(reply, mapf)
		case 1:  // reduce task
			doReduceTask(reply, reducef)
		case 2:  // wait
			time.Sleep(1 * time.Second)
		case 3:  // exit
			fmt.Printf("All tasks are done, exiting...\n")
		default:
			fmt.Printf("Unknown task type: %d\n", reply.TaskType)
		}
	}		
}

func doMapTask(reply *RequestTaskReply, mapf func(string, string) []KeyValue) {
	// open input file
	file, err := os.Open(reply.Filenames[0])
	if err != nil {
		log.Fatalf("cannot open %v", reply.Filenames[0])
	}
	defer file.Close()
	// create intermediate files
	intermediate := []KeyValue{}
	dec := json.NewDecoder(file)
	for {
		var kv KeyValue
		if err := dec.Decode(&kv); err == io.EOF {
			break
		} else if err != nil {
			log.Fatalf("cannot decode %v", reply.Filenames[0])
		}
		intermediate = append(intermediate, mapf(kv.Key, kv.Value)...)
	}
	// create intermediate files
	intermediateFiles := make([]*os.File, reply.NReduce)
	for i := 0; i < reply.NReduce; i++ {
		filename := fmt.Sprintf("mr-%d-%d", reply.TaskID, i)
		file, err := os.Create(filename)
		if err != nil {
			log.Fatalf("cannot create %v", filename)
		}
		defer file.Close()
		intermediateFiles[i] = file
	}
	// write intermediate files
	for _, kv := range intermediate {
		reduceTaskID := ihash(kv.Key) % reply.NReduce
		enc := json.NewEncoder(intermediateFiles[reduceTaskID])
		if err := enc.Encode(&kv); err != nil {
			log.Fatalf("cannot encode %v", kv)
		}
	}
	// close intermediate files
	for _, file := range intermediateFiles {
		if err := file.Close(); err != nil {
			log.Fatalf("cannot close %v", file.Name())
		}
	}
	// report task done
	reply.TaskType = 1
	reply.Filenames = nil
	reply.TaskID = reply.TaskID
	reply.AsignedAt = time.Now()
	reply.Status = 2
	ok := CallTaskReport(reply)
	if ok {
		fmt.Printf("Call TaskReport success\n")
	} else {
		fmt.Printf("Call TaskReport failed\n")
	}
}

func doReduceTask(reply *RequestTaskReply, reducef func(string, []string) string) {
	// open intermediate files
	intermediate := make([][]KeyValue, reply.NReduce)
	for i := 0; i < reply.NReduce; i++ {
		intermediate[i] = []KeyValue{}
	}

	for _, filename := range reply.Filenames {
		file, err := os.Open(filename)
		if err != nil {
			log.Fatalf("cannot open %v", filename)
		}
		defer file.Close()

		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err == io.EOF {
				break
			} else if err != nil {
				log.Fatalf("cannot decode %v", filename)
			}
			reduceTaskID := ihash(kv.Key) % reply.NReduce
			intermediate[reduceTaskID] = append(intermediate[reduceTaskID], kv)
		}
	}

	for i := 0; i < reply.NReduce; i++ {
		sort.Sort(ByKey(intermediate[i]))
	}

	reply.TaskType = 3
	reply.Filenames = intermediate
	reply.TaskID = reply.TaskID
	reply.AsignedAt = time.Now()
	reply.Status = 2

	ok := CallTaskReport(reply)
	if ok {
		fmt.Printf("Call TaskReport success\n")
	} else {
		fmt.Printf("Call TaskReport failed\n")
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
		fmt.Printf("Call RequestTask success\n")
	} else {
		fmt.Printf("Call RequestTask failed\n")
	}
	return &reply
}

func CallTaskReport() bool {
	args := TaskReportArgs{}
	reply := TaskReportReply{}
	return call("Coordinator.TaskReport", &args, &reply)
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
