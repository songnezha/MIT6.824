package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"../labrpc"
)

// import "bytes"
// import "../labgob"

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in Lab 3 you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh; at that point you can add fields to
// ApplyMsg, but set CommandValid to false for these other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
}

// 当前角色
const ROLE_LEADER = "Leader"
const ROLE_FOLLOWER = "Follower"
const ROLE_CANDIDATES = "Candidates"

type LogEntry struct {
	Command interface{}
	Term    int
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	// 所有服务器，持久化状态（lab-2A不要求持久化）
	currentTerm int         // 服务器最后⼀次知道的任期号
	votedFor    int         // 在当前获得选票的候选⼈的 Id
	log         []*LogEntry // 操作日志

	// 所有服务器，易失状态
	commitIndex int // 已知的最⼤的已经被提交的⽇志条⽬的索引值
	lastApplied int // 最后被应⽤到状态机的⽇志条⽬索引值

	// 仅Leader，易失状态（成为leader时重置）
	nextIndex  []int //	每个follower的log同步起点索引（初始为leader log的最后一项）
	matchIndex []int // 每个follower的log同步进度（初始为0），和nextIndex强关联

	// 所有服务器，选举相关状态
	role              string    // 身份
	leaderId          int       // leader的id
	lastActiveTime    time.Time // 上次活跃时间（刷新时机：收到leader心跳、给其他candidates投票、请求其他节点投票）
	lastBroadcastTime time.Time // 作为leader，上次的广播时间
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	var term int
	var isleader bool
	// Your code here (2A).
	term = rf.currentTerm
	isleader = rf.role == ROLE_LEADER
	return term, isleader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term         int
	VoteGraunted bool
}

// 由Leader负责调⽤来复制⽇志指令；也会⽤作heartbeat
type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []*LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.currentTerm
	reply.VoteGraunted = false

	DPrintf("Raft Node[%d] Handle RequestVote, CandidatesId[%d] Term[%d] CurrentTerm[%d] LastLogIndex[%d] LastLogTerm[%d] votedFor[%d]",
		rf.me, args.CandidateId, args.Term, rf.currentTerm, args.LastLogIndex, args.LastLogTerm, rf.votedFor)

	defer func() {
		DPrintf("Raft Node[%d] Return RequestVote, CandidatesId[%d] VoteGranted[%v] ", rf.me, args.CandidateId, reply.VoteGraunted)
	}()

	// 如果 term < currentTerm 返回 false
	if args.Term < rf.currentTerm {
		return
	}

	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.role = ROLE_FOLLOWER
		rf.votedFor = -1
		rf.leaderId = -1
	}

	// 如果 votedFor 为空或者为 candidateId，并且候选⼈的⽇志⾄少和⾃⼰⼀样新，那么就投票给他
	if rf.votedFor == -1 || rf.votedFor == args.CandidateId {
		lastLogTerm := 0
		if len(rf.log) != 0 {
			lastLogTerm = rf.log[len(rf.log)-1].Term
		}
		if args.LastLogTerm < lastLogTerm || args.LastLogIndex < len(rf.log) {
			return
		}
		rf.votedFor = args.CandidateId
		reply.VoteGraunted = true
		rf.lastActiveTime = time.Now()
	}

	rf.persist()
}

//
// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).

	return index, term, isLeader
}

//
// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
//
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) electionLoop() {
	for !rf.killed() {
		time.Sleep(1 * time.Millisecond)

		func() {
			rf.mu.Lock()
			defer rf.mu.Unlock()

			now := time.Now()
			timeout := time.Duration(200+rand.Int31n(150)) * time.Millisecond
			elapses := now.Sub(rf.lastActiveTime)
			if rf.role == ROLE_FOLLOWER {
				if elapses >= timeout {
					rf.role = ROLE_CANDIDATES
					DPrintf("Raft Node[%d] FOLLOWER => CANDIDATE", rf.me)
				}
			}
			// Ask for Vote
			if rf.role == ROLE_CANDIDATES && elapses >= timeout {
				rf.lastActiveTime = now
				rf.currentTerm++
				rf.votedFor = rf.me
				rf.persist()

				args := RequestVoteArgs{
					Term:         rf.currentTerm,
					CandidateId:  rf.me,
					LastLogIndex: len(rf.log),
				}
				if len(rf.log) != 0 {
					args.LastLogTerm = rf.log[len(rf.log)-1].Term
				}
				rf.mu.Unlock()

				DPrintf("Raft Node[%d] RequestVote starts, Term[%d] LastLogIndex[%d] LastLogTerm[%d]", rf.me, args.Term,
					args.LastLogIndex, args.LastLogTerm)

				//RPC Ask for Vote
				type VoteResult struct {
					peerId int
					resp   *RequestVoteReply
				}

				voteCount := 1   //收到投票数，自己一票
				finishCount := 1 //收到应答数
				voteRsultChan := make(chan *VoteResult, len(rf.peers))
				for peerId := 0; peerId < len(rf.peers); peerId++ {
					go func(id int) {
						if id == rf.me {
							return
						}
						resp := RequestVoteReply{}
						if ok := rf.sendRequestVote(id, &args, &resp); ok {
							voteRsultChan <- &VoteResult{
								peerId: id,
								resp:   &resp,
							}
						} else {
							voteRsultChan <- &VoteResult{
								peerId: id,
								resp:   nil,
							}
						}
					}(peerId)
				}

				maxTerm := 0
				for {
					select {
					case VoteResult := <-voteRsultChan:
						finishCount++
						if VoteResult.resp != nil {
							if VoteResult.resp.VoteGraunted {
								voteCount++
							}
							if VoteResult.resp.Term > maxTerm {
								maxTerm = VoteResult.resp.Term
							}
						}
						if finishCount == len(rf.peers) || voteCount > len(rf.peers)/2 {
							goto END
						}
					}
				}
			END:
				rf.mu.Lock()

				defer func() {
					DPrintf("Raft Node[%d] RequestVote ends, finishCount[%d] voteCount[%d] Role[%s] maxTerm[%d] currentTerm[%d]", rf.me, finishCount, voteCount,
						rf.role, maxTerm, rf.currentTerm)
				}()

				// 若此时不是候选人，则直接忽略投票结果
				if rf.role != ROLE_CANDIDATES {
					return
				}
				// 发现了更高的term，切换回FOLLWER
				if maxTerm > rf.currentTerm {
					rf.role = ROLE_FOLLOWER
					rf.leaderId = -1
					rf.votedFor = -1
					rf.currentTerm = maxTerm
					rf.persist()
					return
				}
				// 成为LEADER
				if voteCount > len(rf.peers)/2 {
					rf.role = ROLE_LEADER
					rf.leaderId = rf.me
					rf.lastBroadcastTime = time.Unix(0, 0)
					return
				}
			}
		}()
	}
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	DPrintf("Raft Node[%d] Handle AppendEntries, LeaderId[%d] Term[%d] CurrentTerm[%d] role=[%s]",
		rf.me, args.LeaderId, args.Term, rf.currentTerm, rf.role)
	defer func() {
		DPrintf("Raft Node[%d] Return AppendEntries, LeaderId[%d] Term[%d] CurrentTerm[%d] role=[%s]",
			rf.me, args.LeaderId, args.Term, rf.currentTerm, rf.role)
	}()

	reply.Term = rf.currentTerm
	reply.Success = false

	if args.Term < rf.currentTerm {
		return
	}

	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.role = ROLE_FOLLOWER
		rf.votedFor = -1
		rf.leaderId = -1
	}

	rf.leaderId = args.LeaderId
	rf.lastBroadcastTime = time.Now()
	rf.persist()
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

// lab-2A实现心跳
func (rf *Raft) appendEntriesLoop() {
	for !rf.killed() {
		time.Sleep(1 * time.Millisecond)

		func() {
			rf.mu.Lock()
			defer rf.mu.Unlock()

			// 只有LEADER广播心跳
			if rf.role != ROLE_LEADER {
				return
			}
			// 100ms广播一次
			now := time.Now()
			if now.Sub(rf.lastBroadcastTime) < 100*time.Millisecond {
				return
			}
			rf.lastBroadcastTime = time.Now()

			//RPC 心跳
			type AppendResult struct {
				peerId int
				resp   *AppendEntriesReply
			}

			for peerId := 0; peerId < len(rf.peers); peerId++ {
				if peerId == rf.me {
					continue
				}
				args := AppendEntriesArgs{
					Term:     rf.currentTerm,
					LeaderId: rf.me,
					//log字段在2A不处理
				}

				go func(id int) {
					DPrintf("RaftNode[%d] appendEntries starts, myTerm[%d] peerId[%d]", rf.me, args.Term, id)
					reply := AppendEntriesReply{}
					if ok := rf.sendAppendEntries(id, &args, &reply); ok {
						rf.mu.Lock()
						defer rf.mu.Unlock()
						// 变成FOLLOWER
						if reply.Term > rf.currentTerm {
							rf.role = ROLE_FOLLOWER
							rf.leaderId = -1
							rf.votedFor = -1
							rf.currentTerm = reply.Term
							rf.persist()
						}
						DPrintf("RaftNode[%d] appendEntries ends, peerTerm[%d] myCurrentTerm[%d] myRole[%s]", rf.me, reply.Term, rf.currentTerm, rf.role)
					}
				}(peerId)
			}
		}()
	}
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	rf.role = ROLE_FOLLOWER
	rf.leaderId = -1
	rf.votedFor = -1
	rf.lastActiveTime = time.Now()

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// election
	go rf.electionLoop()

	// leader
	go rf.appendEntriesLoop()

	DPrintf("Raft Node[%d] START", me)

	return rf
}
