package shardmaster

import "raft"
import "labrpc"
import "sync"
import (
	"labgob"
	"time"
	"log"
)

const Debug = 0

const (
	JOIN	= "JOIN"
	LEAVE	= "LEAVE"
	MOVE	= "MOVE"
	QUERY	= "QUERY"
)

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

type Op struct {
	// Your data here.
	Method		string
	Id			int64
	SeqNo		int

	Servers 	map[int][]string	// for join
	GIDs 		[]int 				// for leave
	Shard 		int					// for move
	GID   		int					// for move
	Num 		int					// for query
}

type ShardMaster struct {
	mu			sync.Mutex
	me			int
	rf      	*raft.Raft
	applyCh 	chan raft.ApplyMsg

	// Your data here.
	Duplicate	map[int64]int
	configs 	[]Config // indexed by config num
	waitChs		map[int]chan Op
}

func (sm *ShardMaster) AppendOp(op Op) bool {
	sm.mu.Lock()
	index, _, isLeader := sm.rf.Start(op)
	if !isLeader {
		sm.mu.Unlock()
		return false
	}
	waitCh := make(chan Op, 1)
	sm.waitChs[index] = waitCh
	sm.mu.Unlock()
	select {
	case returnedOp := <- waitCh:
		if returnedOp.Id == op.Id && returnedOp.SeqNo == op.SeqNo {
			DPrintf("server is sure leader")
			return true
		} else {
			return false
		}
	case <- time.After(1000 * time.Millisecond):
		sm.mu.Lock()
		delete(sm.waitChs, index)
		sm.mu.Unlock()
		return false
	}
}

func (sm *ShardMaster) Join(args *JoinArgs, reply *JoinReply) {
	// Your code here.
	reply.Err = OK
	sm.mu.Lock()
	if hasDup := sm.checkDup(args.Id, args.SeqNo); hasDup {
		sm.mu.Unlock()
		reply.WrongLeader = false
		return
	}
	sm.mu.Unlock()
	op := Op{
		Method:JOIN,
		Servers:args.Servers,
		Id:args.Id,
		SeqNo:args.SeqNo,
	}
	ok := sm.AppendOp(op)
	if ok {
		reply.WrongLeader = false
	} else {
		reply.WrongLeader = true
	}
}

func (sm *ShardMaster) Leave(args *LeaveArgs, reply *LeaveReply) {
	// Your code here.
	reply.Err = OK
	sm.mu.Lock()
	if hasDup := sm.checkDup(args.Id, args.SeqNo); hasDup {
		sm.mu.Unlock()
		reply.WrongLeader = false
		return
	}
	sm.mu.Unlock()
	op := Op{
		Method:LEAVE,
		GIDs:args.GIDs,
		Id:args.Id,
		SeqNo:args.SeqNo,
	}
	ok := sm.AppendOp(op)
	if ok {
		reply.WrongLeader = false
	} else {
		reply.WrongLeader = true
	}
}

func (sm *ShardMaster) Move(args *MoveArgs, reply *MoveReply) {
	// Your code here.
	reply.Err = OK
	sm.mu.Lock()
	if hasDup := sm.checkDup(args.Id, args.SeqNo); hasDup {
		sm.mu.Unlock()
		reply.WrongLeader = false
		return
	}
	sm.mu.Unlock()
	op := Op{
		Method:MOVE,
		Shard:args.Shard,
		GID:args.GID,
		Id:args.Id,
		SeqNo:args.SeqNo,
	}
	ok := sm.AppendOp(op)
	if ok {
		reply.WrongLeader = false
	} else {
		reply.WrongLeader = true
	}
}

func (sm *ShardMaster) Query(args *QueryArgs, reply *QueryReply) {
	// Your code here.
	op := Op{
		Method:QUERY,
		Num:args.Num,
		Id:args.Id,
		SeqNo:args.SeqNo,
	}
	ok := sm.AppendOp(op)
	sm.mu.Lock()
	defer sm.mu.Unlock()
	if ok {
		num := op.Num
		if op.Num < 0 || op.Num >= len(sm.configs) {
			num = len(sm.configs) - 1
		}
		reply.Config = sm.configs[num]
		reply.Err = OK
		reply.WrongLeader = false
	} else {
		reply.WrongLeader = true
	}
}

func (sm *ShardMaster) copyConfig(config *Config) Config {
	res := Config{}
	res.Num = config.Num
	res.Shards = config.Shards
	res.Groups = make(map[int][]string)
	for k, v := range config.Groups {
		res.Groups[k] = make([]string, len(v))
		copy(res.Groups[k], v)
	}
	return res
}

func (sm *ShardMaster) checkDup(id int64, seqNo int) bool {
	if v, ok := sm.Duplicate[id]; ok && v >= seqNo {
		return true
	}
	return false
}

func (sm *ShardMaster) rebalance(config *Config)  {
	if len(config.Groups) == 0 {
		return
	}
	avgLen := NShards / len(config.Groups)
	left := NShards - avgLen * len(config.Groups)
	shardsPerGroup := make(map[int][]int)
	// initialize a shards list for every valid gids
	for k := range config.Groups {
		shardsPerGroup[k] = make([]int,0)
	}
	for i, gid := range config.Shards {
		if list, ok := shardsPerGroup[gid]; ok {
			shardsPerGroup[gid] = append(list, i)
		}
	}
	extraShards := make([]int, 0)
	// cut extra shards of valid group
	copyLeft := left
	for gid, list := range shardsPerGroup {
		if len(list) > avgLen + 1 && copyLeft > 0 {
			extraShards = append(extraShards, list[len(list) - (avgLen + 1):]...)
			shardsPerGroup[gid] = list[:len(list) - (avgLen + 1)]
			copyLeft--
		} else if len(list) > avgLen {
			extraShards = append(extraShards, list[len(list) - avgLen:]...)
			shardsPerGroup[gid] = list[:len(list) - avgLen]
		}
	}
	// collect shards of invalid group
	for s, gid := range config.Shards {
		if _, ok := config.Groups[gid]; !ok {
			extraShards = append(extraShards, s)
		}
	}
	//log.Printf("me %v, ave %v, left %v, nOfGroup %v, shardsPerGroup %v, extras %v", sm.me, avgLen, left, len(config.Groups), shardsPerGroup, extraShards)
	// fill to shardsPerGroup average
	for gid, list := range shardsPerGroup {
		//log.Printf("me %v, gid %v, length %v", sm.me, gid, len(list))
		if len(list) < avgLen {
			shardsPerGroup[gid] = append(list, extraShards[len(extraShards) - (avgLen - len(list)):]...)
			extraShards = extraShards[:len(extraShards) - (avgLen - len(list))]
		}
		if len(shardsPerGroup[gid]) >= avgLen + 1 {
			left--
		}
		//log.Printf("me %v, gid %v, shardsPerGroup %v", sm.me, gid, shardsPerGroup[gid])
	}
	// fill shardsPerGroup left
	for gid, list := range shardsPerGroup {
		if left < 1 {
			break
		}
		if len(list) < avgLen + 1 {
			shardsPerGroup[gid] = append(list, extraShards[len(extraShards) - (avgLen + 1 - len(list)):]...)
			extraShards = extraShards[:len(extraShards) - (avgLen + 1 - len(list))]
			left--
		}
	}
	// convert to Shard
	for gid, list := range shardsPerGroup {
		for _, s := range list {
			config.Shards[s] = gid
		}
	}
}

func (sm *ShardMaster) applyJoin(Servers map[int][]string)  {
	newConfig := sm.copyConfig(&sm.configs[len(sm.configs) - 1])
	newConfig.Num++
	for k, v := range Servers {
		newConfig.Groups[k] = make([]string, len(v))
		copy(newConfig.Groups[k], v)
	}
	sm.rebalance(&newConfig)
	sm.configs = append(sm.configs, newConfig)
}

func (sm *ShardMaster) applyLeave(GIDs []int)  {
	newConfig := sm.copyConfig(&sm.configs[len(sm.configs) - 1])
	newConfig.Num++
	for _, gid := range GIDs {
		delete(newConfig.Groups, gid)
	}
	sm.rebalance(&newConfig)
	sm.configs = append(sm.configs, newConfig)
}

func (sm *ShardMaster) applyMove(Shard int, GID int)  {
	newConfig := sm.copyConfig(&sm.configs[len(sm.configs) - 1])
	newConfig.Num++
	newConfig.Shards[Shard] = GID
	sm.configs = append(sm.configs, newConfig)
}

func (sm *ShardMaster) applyDaemon()  {
	for {
		msg := <- sm.applyCh
		sm.mu.Lock()
		op := msg.Command.(Op)
		switch op.Method {
		case QUERY:
		case JOIN:
			if hasDup := sm.checkDup(op.Id, op.SeqNo); !hasDup {
				sm.applyJoin(op.Servers)
			}
		case MOVE:
			if hasDup := sm.checkDup(op.Id, op.SeqNo); !hasDup {
				sm.applyMove(op.Shard, op.GID)
			}
		case LEAVE:
			if hasDup := sm.checkDup(op.Id, op.SeqNo); !hasDup {
				sm.applyLeave(op.GIDs)
			}
		}
		if ch, ok := sm.waitChs[msg.CommandIndex]; ok{
			select {
			case <-ch:
			default:
			}
			ch <- op
			delete(sm.waitChs, msg.CommandIndex)
		}
		sm.mu.Unlock()
	}
}

//
// the tester calls Kill() when a ShardMaster instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (sm *ShardMaster) Kill() {
	sm.rf.Kill()
	// Your code here, if desired.
}

// needed by shardkv tester
func (sm *ShardMaster) Raft() *raft.Raft {
	return sm.rf
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant shardmaster service.
// me is the index of the current server in servers[].
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardMaster {
	sm := new(ShardMaster)
	sm.me = me

	sm.configs = make([]Config, 1)
	sm.configs[0].Groups = map[int][]string{}

	labgob.Register(Op{})
	sm.applyCh = make(chan raft.ApplyMsg)
	sm.rf = raft.Make(servers, me, persister, sm.applyCh)

	// You may need initialization code here.
	sm.Duplicate = make(map[int64]int)
	sm.waitChs = make(map[int]chan Op)
	go sm.applyDaemon()
	return sm
}
