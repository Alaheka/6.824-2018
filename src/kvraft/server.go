package raftkv

import (
	"labgob"
	"labrpc"
	"log"
	"raft"
	"sync"
	"time"
	"bytes"
)

const Debug = 0

const (
	GET		= "GET"
	PUT		= "PUT"
	APPEND	= "APPEND"
)

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Method		string
	Key			string
	Value		string
	Id			int64
	SeqNo		int
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	SnapshotIndex    int
	Duplicate        map[int64]int
	waitChs          map[int]chan Op
	KvStore          map[string]string
}

func (kv *KVServer) AppendOp(op Op) bool {
	kv.mu.Lock()
	index, _, isLeader := kv.rf.Start(op)
	if !isLeader {
		kv.mu.Unlock()
		return false
	}

	DPrintf("server is leader")
	waitCh := make(chan Op, 1)
	kv.waitChs[index] = waitCh
	kv.mu.Unlock()
	select {
	case returnedOp := <- waitCh:
		if returnedOp == op {
			DPrintf("server is sure leader")
			return true
		} else {
			return false
		}
	case <- time.After(1000 * time.Millisecond):
		kv.mu.Lock()
		delete(kv.waitChs, index)
		kv.mu.Unlock()
		return false
	}
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	DPrintf("%v server get Get %v", kv.me, args)
	op := Op{
		Method:GET,
		Key:args.Key,
		Value:"",
		Id:args.Id,
		SeqNo:args.SeqNo,
	}
	ok := kv.AppendOp(op)
	kv.mu.Lock()

	defer kv.mu.Unlock()
	if ok {
		if v, ok2 := kv.KvStore[op.Key]; ok2 {
			reply.Value = v
			reply.Err = OK
			reply.WrongLeader = false
		} else {
			reply.WrongLeader = false
			reply.Err = ErrNoKey
		}
	} else {
		reply.WrongLeader = true
	}
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	DPrintf("%v server get PutAppend %v", kv.me, args)
	reply.Err = OK
	kv.mu.Lock()
	if hasDup := kv.checkDup(args.Id, args.SeqNo); hasDup {
		kv.mu.Unlock()
		reply.WrongLeader = false
		return
	}
	kv.mu.Unlock()
	op := Op{
		Method:args.Op,
		Key:args.Key,
		Value:args.Value,
		Id:args.Id,
		SeqNo:args.SeqNo,
	}
	ok := kv.AppendOp(op)
	if ok {
		reply.WrongLeader = false
	} else {
		reply.WrongLeader = true
	}
}

func (kv *KVServer) checkDup(id int64, seqNo int) bool {
	if v, ok := kv.Duplicate[id]; ok && v >= seqNo {
		return true
	}
	return false
}
//
// the tester calls Kill() when a KVServer instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (kv *KVServer) Kill(){
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) checkSnapshot(lastCommandIndex int)  {
	if kv.maxraftstate > -1 && kv.rf.GetRaftStateSize() > int(0.95 * float64(kv.maxraftstate)) {
		kv.SnapshotIndex = lastCommandIndex
		w := new(bytes.Buffer)
		e := labgob.NewEncoder(w)
		e.Encode(kv.SnapshotIndex)
		e.Encode(kv.Duplicate)
		e.Encode(kv.KvStore)
		//log.Printf("call snapshot")
		kv.rf.Snapshot(lastCommandIndex, w.Bytes())
	}
}

func (kv *KVServer) applyDaemon()  {
	for {
		msg := <- kv.applyCh
		kv.mu.Lock()
		if !msg.CommandValid {
			//TODO: snapshot
			kv.readSnapshot(msg.SnapshotData)
		} else {
			op := msg.Command.(Op)
			//log.Printf("%v enter lock 4", kv.me)
			switch op.Method {
			case GET:
			case PUT:
				if hasDup := kv.checkDup(op.Id, op.SeqNo); !hasDup {
					kv.KvStore[op.Key] = op.Value
					kv.Duplicate[op.Id] = op.SeqNo
				}
			case APPEND:
				if hasDup := kv.checkDup(op.Id, op.SeqNo); !hasDup {
					kv.KvStore[op.Key] += op.Value
					kv.Duplicate[op.Id] = op.SeqNo
				}
			}
			if ch, ok := kv.waitChs[msg.CommandIndex]; ok{
				select {
				case <-ch:
				default:
				}
				ch <- op
				delete(kv.waitChs, msg.CommandIndex)
			}
			kv.checkSnapshot(msg.CommandIndex)
		}
		kv.mu.Unlock()
	}
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots with persister.SaveSnapshot(),
// and Raft should save its state (including log) with persister.SaveRaftState().
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.
	kv.SnapshotIndex = -1
	kv.Duplicate = make(map[int64]int)
	kv.KvStore = make(map[string]string)
	kv.waitChs = make(map[int]chan Op)
	kv.readSnapshot(persister.ReadSnapshot())
	go kv.applyDaemon()
	return kv
}

func (kv *KVServer) readSnapshot(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var snapshotIndex = -1
	var duplicate = make(map[int64]int)
	var kvStore = make(map[string]string)
	if d.Decode(&snapshotIndex) != nil ||
		d.Decode(&duplicate) != nil ||
		d.Decode(&kvStore) != nil {
		DPrintf("error decoding persistence")
	} else {
		kv.SnapshotIndex = snapshotIndex
		kv.Duplicate = duplicate
		kv.KvStore = kvStore
	}
}
