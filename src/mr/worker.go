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
	"strconv"
	"strings"
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
		reply, ok := CallRequestTask()
		if !ok {
			return
		}
		switch reply.TaskType {
		case 0:  // map task
			taskID, taskType, interFilenames := doMapTask(reply, mapf)
			CallTaskReport(taskID, taskType, interFilenames)
		case 1:  // reduce task
			taskID, taskType, _ := doReduceTask(reply, reducef)
			CallTaskReport(taskID, taskType, nil)
		case 2:  // wait
			time.Sleep(1 * time.Second)
		case 3:  // exit
			fmt.Printf("All tasks are done, exiting...\n")
			return
		default:
			fmt.Printf("Unknown task type: %d\n", reply.TaskType)
		}
	}		
}

func doMapTask(reply *RequestTaskReply, mapf func(string, string) []KeyValue) (taskID int, taskType int, interFilenames []string) {
	intermediate := []KeyValue{}
	file, err := os.Open(reply.Filenames[0])
	if err != nil {
		log.Fatalf("cannot open %v", reply.Filenames[0])
	}
	defer file.Close()
	content, err := io.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", reply.Filenames[0])
	}
	kva := mapf(reply.Filenames[0], string(content))
	intermediate = append(intermediate, kva...)
	sort.Sort(ByKey(intermediate))
	// intermediate[i:j) has the same key
	buckets := make([][]KeyValue, reply.NReduce)
	i := 0
	for i < len(intermediate) {
		j := i + 1;
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		// now intermediate[i:j) has the same key
		// write them to the intermediate file
		reduceID := ihash(intermediate[i].Key) % reply.NReduce
		buckets[reduceID] = append(buckets[reduceID], intermediate[i:j]...)
		i = j
	}
	interFilenames = make([]string, 0)
	for reduceID, kva := range buckets {
		oname := fmt.Sprintf("mr-%d-%d", reply.TaskID, reduceID)
		ofile, _ := os.CreateTemp(".", oname)	
		enc := json.NewEncoder(ofile)
		for _, kv := range kva {
			err := enc.Encode(&kv)	
			if err != nil {
				os.Remove(ofile.Name())
				log.Fatalf("cannot encode %v", kv)
			}
		}
		ofile.Close()
		os.Rename(ofile.Name(), oname)
		interFilenames = append(interFilenames, oname)
	}
	return reply.TaskID, 0, interFilenames
}

func doReduceTask(reply *RequestTaskReply, reducef func(string, []string) string) (taskID int, taskType int, oFilenames []string) {
	// read all the intermediate files
	intermediate := []KeyValue{}
	for _, filename := range reply.Filenames {
		// search for the correct intermediate files
		parts := strings.Split(filename, "-")
		if parts[2] != strconv.Itoa(reply.TaskID) {
			continue
		}
		// read the intermediate file
		// and append to the intermediate slice
		file, err := os.Open(filename)
		if err != nil {
			log.Fatalf("Reduce task [%d] cannot open %v", reply.TaskID, filename)
		}
		defer file.Close()
		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if dec.Decode(&kv) != nil {
				break
			}
			intermediate = append(intermediate, kv)
		}
	}
	sort.Sort(ByKey(intermediate))
	oname := fmt.Sprintf("mr-out-%d", reply.TaskID)
	ofile, _ := os.CreateTemp(".", oname)
	i := 0
	for i < len(intermediate) {
		j := i + 1;
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}
		reduceResult := reducef(intermediate[i].Key, values)
		fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, reduceResult)
		i = j
	}
	ofile.Close()
	os.Rename(ofile.Name(), oname)
	return reply.TaskID, 1, nil
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

func CallRequestTask() (*RequestTaskReply, bool) {
	args := RequestTaskArgs{}
	reply := RequestTaskReply{}
	ok := call("Coordinator.RequestTask", &args, &reply)
	if ok {
		fmt.Printf("Call RequestTask success, taskID: [%d] taskType: [%d]\n", reply.TaskID, reply.TaskType)
	} else {
		log.Printf("Call RequestTask failed, assume coordinator exited\n")
	}
	return &reply, ok
}

func CallTaskReport(taskID int, taskType int, oFilenames []string) {
	args := TaskReportArgs{TaskID: taskID, TaskType: taskType, OFilenames: oFilenames}
	reply := TaskReportReply{}
	ok := call("Coordinator.TaskReport", &args, &reply)
	if ok {
		fmt.Printf("Call TaskReport success, taskID: [%d] taskType: [%d]\n", taskID, taskType)
		fmt.Printf("  with output filenames: ")
		for _, fname := range oFilenames {
			fmt.Printf("%v ", fname)
		}
		fmt.Printf("\n")
	} 
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
