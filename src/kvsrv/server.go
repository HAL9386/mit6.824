package kvsrv

import (
	"log"
	"sync"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}


type KVServer struct {
	mu sync.Mutex
	// Your definitions here.
	data map[string]string
	lastResult map[int64]string
}


func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()
	reply.Value = kv.data[args.Key]
}

func (kv *KVServer) Put(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if args.RequestType == 1 {
		delete(kv.lastResult, args.RequestID)
		return
	}
	if _, exists := kv.lastResult[args.RequestID]; exists {
		return
	}
	kv.data[args.Key] = args.Value
	kv.lastResult[args.RequestID] = args.Value
	reply.Value = args.Value
}

func (kv *KVServer) Append(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if args.RequestType == 1 {
		delete(kv.lastResult, args.RequestID)
		return
	}
	if lastRes, exists := kv.lastResult[args.RequestID]; exists {
		reply.Value = lastRes
		return
	}
	reply.Value = kv.data[args.Key]
	kv.lastResult[args.RequestID] = reply.Value
	kv.data[args.Key] += args.Value
}

func StartKVServer() *KVServer {
	kv := new(KVServer)
	// You may need initialization code here.
	kv.data = make(map[string]string)	
	kv.lastResult = make(map[int64]string)
	return kv
}
