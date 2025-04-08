package main

import (
	"fmt"
	"log"
	"net"
	"net/rpc"
)

//
// Common RPC request/reply definitions
//

type PutArgs struct {
	Key   string
	Value string
}

type PutReply struct {
}

type GetArgs struct {
	Key string
}

type GetReply struct {
	Value string
}

//
// Client
//

func connect() *rpc.Client {
	client, err := rpc.Dial("tcp", ":1234")
	if err != nil {
		log.Fatal("dialing:", err)
	}
	return client
}

// channel-based kv store
type KVch struct {
	reqCh chan request
}

type request struct {
	op    string       // operation type, "get" or "put"
	key   string       // key
	value string       // only for put
	rspCh chan string  // response channel
}

// channel-based server
func serverCh() {
	kv := &KVch{reqCh: make(chan request)}
	rpcs := rpc.NewServer()
	rpcs.Register(kv)
	l, e := net.Listen("tcp", ":1234")
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go func() {
		for {
			conn, err := l.Accept()
			if err == nil {
				go rpcs.ServeConn(conn)
			} else {
				break
			}
		}
		l.Close()
	}()
	go func() {
		data := make(map[string]string)
		for req := range kv.reqCh {
			switch req.op {
			case "get":
				req.rspCh <- data[req.key]
			case "put":
				data[req.key] = req.value
				req.rspCh <- ""
			}
		}
	}()
}

// client get method
func getCh(key string) string {
	client := connect()
	defer client.Close()
	args := GetArgs{Key: key}
	var reply GetReply
	err := client.Call("KVch.GetCh", &args, &reply)
	if err != nil {
		log.Fatal("error:", err)
	}
	return reply.Value
}

func putCh(key string, val string) {
	client := connect()
	args := PutArgs{key, val}
	reply := PutReply{}
	err := client.Call("KVch.PutCh", &args, &reply)
	if err != nil {
		log.Fatal("error:", err)
	}
	client.Close()
}

// channel-based get method
func (kv *KVch) GetCh(args *GetArgs, reply *GetReply) error {
	req := request{
		op:    "get",
		key:   args.Key,
		rspCh: make(chan string),
	}
	kv.reqCh <- req
	reply.Value = <-req.rspCh
	return nil
}

// channed-based put method
func (kv *KVch) PutCh(args *PutArgs, reply *PutReply) error {
	req := request{
		op:    "put",
		key:   args.Key,
		value: args.Value,
		rspCh: make(chan string),
	}
	kv.reqCh <- req
	<-req.rspCh  // Wait for response before returning
	return nil
}

func main() {
	serverCh()

	putCh("subject", "6.5840")
	fmt.Printf("Put(subject, 6.5840) done\n")
	fmt.Printf("get(subject) -> %s\n", getCh("subject"))
}
