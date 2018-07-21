package raftkv

import "labrpc"
import "crypto/rand"
import (
	"math/big"
	"time"
)

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	leader	int
	id		int64
	seqNo	int
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// You'll have to add code here.
	ck.id = nrand()
	ck.seqNo = 0
	return ck
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) Get(key string) string {

	// You will have to modify this function.
	cnt := len(ck.servers)
	args := GetArgs{
		Key:   key,
		SeqNo: ck.seqNo,
		Id:    ck.id,
	}
	DPrintf("%v client send Get %v", ck.id, args)

	for {
		done := make(chan bool, 1)
		reply := GetReply{}
		go func(leader int) {
			ok := ck.servers[leader].Call("KVServer.Get", &args, &reply)
			done <- ok
		}(ck.leader)
		select {
		case ok := <- done:
			if ok && !reply.WrongLeader {
				if reply.Err == OK {
					// not increment seqNo for Get
					DPrintf("%v client Get result %v", ck.id, reply)
					return reply.Value
				} else {
					return ""
				}
			} else {
				ck.leader = (ck.leader + 1) % cnt
				time.Sleep(15 * time.Millisecond)
			}
		case <- time.After(300 * time.Millisecond):
			ck.leader = (ck.leader + 1) % cnt
		}
	}
	return ""
}

//
// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
	cnt := len(ck.servers)
	args := PutAppendArgs{
		Key:   key,
		Value: value,
		Op:    op,
		SeqNo: ck.seqNo,
		Id:    ck.id,
	}
	DPrintf("%v client send PutAppend %v", ck.id, args)
	for {
		done := make(chan bool, 1)
		reply := PutAppendReply{}
		go func(leader int) {
			ok := ck.servers[leader].Call("KVServer.PutAppend", &args, &reply)
			done <- ok
		}(ck.leader)
		select {
		case ok := <- done:
			if ok && !reply.WrongLeader && reply.Err == OK {
				ck.seqNo++
				return
			} else {
				ck.leader = (ck.leader + 1) % cnt
				time.Sleep(15 * time.Millisecond)
			}
		case <- time.After(300 * time.Millisecond):
			ck.leader = (ck.leader + 1) % cnt
		}
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, PUT)
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, APPEND)
}
