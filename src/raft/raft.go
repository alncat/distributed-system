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

import "sync"
import "labrpc"
import "time"
import "math/rand"
import "fmt"

import "bytes"
import "encoding/gob"

const MaxInt = int(^uint(0) >> 1)
const MinInt = -MaxInt - 1
//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make().
//
type ApplyMsg struct {
	Index       int
	Command     interface{}
	UseSnapshot bool   // ignore for lab2; only used in lab3
	Snapshot    []byte // ignore for lab2; only used in lab3
}

type AppendEntriesArgs struct {
	Term 		 int
	LeaderID 	 int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term		int
	Success		bool
}

//
// A Go object implementing a single Raft peer.
//
type LogEntry struct {
	Command	ApplyMsg
	//Command string
	Term	int
	Index	int
}

type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	currentTerm int
	currentLeader int
	votedFor  int
	log		  []LogEntry
	nextIndex []int
	matchIndex []int
	commitIndex int
	lastApplied int
	state		int
	electionTimer *time.Timer
	heartbeatTicker *time.Ticker
	Done chan struct{}
	indexCount map[int]int
	commitQueue []chan LogEntry
	clientRequest chan LogEntry
	condition	*sync.Cond
	applyCh		chan ApplyMsg
	wg			*sync.WaitGroup
	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	rf.mu.Lock()
	term = rf.currentTerm
	isleader = (rf.state == 2)
	//fmt.Println(rf.me, "is", rf.state)
	rf.mu.Unlock()
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
	 w := new(bytes.Buffer)
	 e := gob.NewEncoder(w)
	 e.Encode(rf.currentTerm)
	 e.Encode(rf.votedFor)
	 e.Encode(rf.commitIndex)
	 e.Encode(rf.log)
	 data := w.Bytes()
	 rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	// Your code here (2C).
	// Example:
	r := bytes.NewBuffer(data)
	d := gob.NewDecoder(r)
	d.Decode(&rf.currentTerm)
	d.Decode(&rf.votedFor)
	d.Decode(&rf.commitIndex)
	d.Decode(&rf.log)
	//fmt.Println("readPersist", rf.me, rf.currentTerm, rf.commitIndex, rf.log)
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
}




//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term		 int
	CandidateID  int
	LastLogIndex int
	LastLogTerm	 int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term		int
	VoteGranted bool
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
	} else{
		rf.persist()
		//fmt.Println(rf.me, "got request from", args.CandidateID, args.Term, "in", rf.currentTerm)
		if args.Term > rf.currentTerm {
			if(rf.state == 2){
				rf.state = 0
				rf.heartbeatTicker.Stop()
				rf.indexCount = nil
				rf.condition.Signal()
				close(rf.Done)
				//let sync single loop proceed and return
				rf.mu.Unlock()
				rf.wg.Wait()
				//regain lock
				rf.mu.Lock()
			}
			rf.state = 0
			rf.votedFor = -1
			reply.Term = rf.currentTerm
			rf.electionTimer.Stop()
			rf.electionTimer.Reset(300*time.Millisecond+time.Duration(rand.Intn(100))*time.Millisecond)
		}
		lastLog := rf.log[len(rf.log)-1]
		if (args.LastLogTerm > lastLog.Term || ((args.LastLogTerm == lastLog.Term && 
			args.LastLogIndex >= lastLog.Index) && (rf.votedFor == -1 || rf.votedFor == args.CandidateID))) {
			reply.VoteGranted = true
			rf.votedFor = args.CandidateID
			rf.state = 0
			fmt.Println(rf.me, "with", lastLog.Index, lastLog.Term, "got", args.LastLogTerm, "from", args.CandidateID)
			rf.electionTimer.Stop()
			rf.electionTimer.Reset(300*time.Millisecond+time.Duration(rand.Intn(100))*time.Millisecond)
		} else {
			reply.VoteGranted = false
		}
		rf.currentTerm = args.Term
		reply.Term = rf.currentTerm
	}
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.Success = false
	} else {
		oldIndex := rf.commitIndex
		lastIndex := 0
		if len(args.Entries) == 0 {
			//reset election timeout
			rf.electionTimer.Stop()
			//fmt.Println(rf.me, "got heartbeat from", args.LeaderID, "in", rf.currentTerm)
			if(rf.state == 2){
				//fmt.Println(rf.me, "got heartbeat from", args.LeaderID, "in", rf.currentTerm)
				rf.heartbeatTicker.Stop()
				rf.indexCount = nil
				rf.condition.Signal()
				rf.state = 0
				close(rf.Done)
				//let sync single loop proceed and return
				rf.mu.Unlock()
				rf.wg.Wait()
				//regain lock
				rf.mu.Lock()
			}
			rf.state = 0
			rf.currentTerm = args.Term
			rf.votedFor = -1
			rf.currentLeader = args.LeaderID
			reply.Term = rf.currentTerm
			//reply.Success = true
			rf.electionTimer.Reset(300*time.Millisecond+time.Duration(rand.Intn(100))*time.Millisecond)
			lastIndex = rf.log[len(rf.log) - 1].Index
		}
		if (len(rf.log) > args.PrevLogIndex && rf.log[args.PrevLogIndex].Term == args.PrevLogTerm) {
			for _, entry := range args.Entries {
				if len(rf.log) > entry.Index && rf.log[entry.Index].Term != entry.Term {
					rf.log = append(rf.log[:entry.Index])
				}
			}
			//set the last index to be the index of last log entry
			lastIndex = rf.log[len(rf.log) - 1].Index
			for _, entry := range args.Entries {
				if len(rf.log) <= entry.Index {
					rf.log = append(rf.log, entry)
					lastIndex = entry.Index
				}
			}
			reply.Success = true
			if args.LeaderCommit > rf.commitIndex {
				if args.LeaderCommit > lastIndex {
					rf.commitIndex = lastIndex
				} else {
					rf.commitIndex = args.LeaderCommit
				}
			}
			rf.persist()
			if oldIndex != rf.commitIndex {
				fmt.Println(rf.me, "send", oldIndex+1, rf.commitIndex)
				go func(from, to int){
					for i := from; i <= to; i++ {
						rf.mu.Lock()
						command := rf.log[i].Command
						rf.mu.Unlock()
						rf.applyCh <- command
					}
				}(oldIndex + 1, rf.commitIndex)
			}

		} else {
			reply.Term = rf.currentTerm
			reply.Success = false
		}
	}
}


func (rf *Raft) SyncSingle(id int){
	//fetch next log to be committed
	rf.wg.Add(1)
	defer rf.wg.Done()
	for{
		select {
		case <- rf.commitQueue[id]:
			rf.mu.Lock()
			toCommit := rf.nextIndex[id]
			if toCommit > len(rf.log) {
				continue
			}
			args := AppendEntriesArgs{
				LeaderID: rf.me,
			}
			reply := AppendEntriesReply{
				Term: rf.currentTerm,
				Success: false,
			}
			rf.mu.Unlock()
			loop:
			for {
				rf.mu.Lock()
				if rf.state != 2 {
					fmt.Println(rf.me, "sync", id, "exit")
					rf.mu.Unlock()
					return
				}
				if rf.nextIndex[id] >= len(rf.log) {
					if rf.matchIndex[id] == rf.nextIndex[id] - 1 {
						rf.mu.Unlock()
						break
					}
					rf.nextIndex[id] = rf.log[len(rf.log)-1].Index
				}
				args.Term = rf.currentTerm
				args.PrevLogIndex = rf.log[rf.nextIndex[id] - 1].Index
				args.PrevLogTerm = rf.log[rf.nextIndex[id] - 1].Term
				args.Entries	  = []LogEntry{rf.log[rf.nextIndex[id]]}
				args.LeaderCommit = rf.commitIndex
				fmt.Println(rf.me, "send", id, "new args", args, rf.nextIndex[id], len(rf.log), toCommit)
				rf.mu.Unlock()

				c := make(chan bool)
				go func() { c <- rf.sendAppendEntries(id, &args, &reply) } ()
				select {
				case success := <-c:
					// use err and reply
					if  success {
						rf.mu.Lock()
						if reply.Success {
							rf.matchIndex[id] = rf.nextIndex[id]
							rf.nextIndex[id]++
							if rf.indexCount == nil {
								fmt.Println(rf.me, "stop committing")
								rf.mu.Unlock()
								return
							}
							fmt.Println(rf.me, "got a reply from", id, "for", rf.matchIndex[id], rf.indexCount[rf.matchIndex[id]]+1)
							rf.indexCount[rf.matchIndex[id]]++
							if rf.indexCount[rf.matchIndex[id]] >= len(rf.peers)/2 + 1 {
								rf.condition.Signal()
							}
							if rf.matchIndex[id] >= len(rf.log) {
								//signal main routine
								rf.mu.Unlock()
								break loop
							}
						} else {
							if rf.nextIndex[id] > rf.matchIndex[id] + 1 {
								rf.nextIndex[id]--
							}
						}
						rf.mu.Unlock()
					}
				case <-rf.Done:
					// call timed out
					//fmt.Println(rf.me, "closed", id)
					return
				}

			}
		case <- rf.Done:
			//fmt.Println(rf.me, "done", id, "exit")
			return
		}
	}
}

func (rf *Raft) CommitLog(){
	me := rf.me
	//each routine synchronize the log of a follower
	for i := range rf.peers {
		if i == me {
			continue
		}
		go rf.SyncSingle(i)
	}

	for{
		select {
		case <-rf.clientRequest:
			rf.mu.Lock()
			if rf.state != 2 {
				rf.mu.Unlock()
				return
			}
			toCommit := rf.commitIndex + 1

			fmt.Println(me, "starts committing", toCommit, "in", rf.currentTerm, len(rf.log))
			if toCommit >= len(rf.log) {
				continue
			}
			payload := rf.log[toCommit]
			//rf.indexCount[toCommit] = 1
			payload.Term = rf.currentTerm
			//rf.log = append(rf.log, payload)
			rf.mu.Unlock()
			for i := range rf.peers {
				if i == me {
					continue
				}
				//ask put new log to the commit queue of followers
				go func(p LogEntry, id int){
					//fmt.Println(me, "is committing", p, "to", id)
					rf.commitQueue[id] <- p
				}(payload, i)
			}
			//wait for majority vote and update commitIndex
			rf.mu.Lock()
			committed := 0
			for rf.indexCount != nil {
				//committed index is the largest index with majority vote
				for i := toCommit; i < len(rf.log); i++ {
					if rf.indexCount[i] > len(rf.peers)/2 {
						committed = i
					}
				}
				if committed >= toCommit {
					break
				}
				rf.condition.Wait()
			}
			if rf.indexCount == nil {
				fmt.Println(rf.me, "stop committing")
				rf.mu.Unlock()
				return
			}
			//apply changes to state machine and update lastApplied
			startIndex := rf.commitIndex + 1
			rf.commitIndex = committed
			//go func(command ApplyMsg){
			rf.persist()
			//payload.Command = rf.log[committed].Command
			//rf.mu.Unlock()
			//rf.mu.Lock()
			for i := startIndex; i <= committed; i++ {
				fmt.Println(me, "committed", rf.log[i].Command, "in", payload.Term)
				rf.applyCh <- rf.log[i].Command
			}
			rf.mu.Unlock()
			//}(payload.Command)
		case <-rf.Done:
			return
		}
	}
}

func (rf *Raft) HeartBeatOnce(){
	rf.mu.Lock()
	args := AppendEntriesArgs{
		Term: rf.currentTerm,
		LeaderID: rf.me,
		PrevLogIndex: rf.log[len(rf.log)-1].Index,
		PrevLogTerm: rf.log[len(rf.log)-1].Term,
		LeaderCommit: rf.commitIndex,
	}
	reply := AppendEntriesReply{
		Term: rf.currentTerm,
		Success: false,
	}
	rf.mu.Unlock()
	me := rf.me
	for i := range rf.peers {
		if i == me {
			continue
		}
		go func(args AppendEntriesArgs, reply AppendEntriesReply, id int){
			rf.sendAppendEntries(id, &args, &reply)
		}(args, reply, i)
	}
}

func (rf *Raft) HeartBeat(){
	rf.mu.Lock()
	args := AppendEntriesArgs{
		Term: rf.currentTerm,
		LeaderID: rf.me,
		PrevLogIndex: rf.log[len(rf.log)-1].Index,
		PrevLogTerm: rf.log[len(rf.log)-1].Term,
		LeaderCommit: rf.commitIndex,
	}
	reply := AppendEntriesReply{
		Term: rf.currentTerm,
		Success: false,
	}
	rf.mu.Unlock()
	for {
		select {
		case <-rf.heartbeatTicker.C:
			rf.mu.Lock()
			args.PrevLogIndex = rf.log[len(rf.log)-1].Index
			args.PrevLogTerm = rf.log[len(rf.log)-1].Term
			args.LeaderCommit = rf.commitIndex
			rf.mu.Unlock()
			for i := range rf.peers {
				if i == rf.me {
					continue
				}
				go func(args AppendEntriesArgs, reply AppendEntriesReply, id int){
					//rf.mu.Lock()
					//if rf.nextIndex[id] > rf.matchIndex[id] + 1 {

					//}
					//rf.mu.Unlock()
					rf.sendAppendEntries(id, &args, &reply)
				}(args, reply, i)
			}
		case <-rf.Done:
			return
		}
	}
}

func (rf *Raft) ElectLeader() {
	//begin vote
	for {
		<- rf.electionTimer.C
		go func(){
			rf.mu.Lock()
			rf.currentTerm++
			rf.votedFor = rf.me
			args := RequestVoteArgs{
				Term: rf.currentTerm,
				CandidateID: rf.me,
				LastLogIndex: rf.log[len(rf.log)-1].Index,
				LastLogTerm: rf.log[len(rf.log)-1].Term,
			}
			reply := RequestVoteReply{
				Term: rf.currentTerm,
				VoteGranted: false,
			}
			rf.state = 1
			fmt.Println("election", rf.currentTerm, "starts by", rf.me, args)
			//reset election timeout
			rf.electionTimer.Stop()
			rf.electionTimer.Reset(300*time.Millisecond+time.Duration(rand.Intn(100))*time.Millisecond)
			rf.mu.Unlock()

			votes := make(chan bool, len(rf.peers))
			for i := range rf.peers {
				if i == rf.me {
					continue
				}
				go func(args RequestVoteArgs, reply RequestVoteReply, id int){
					if rf.sendRequestVote(id, &args, &reply) {
						votes <- reply.VoteGranted
					} else {
						votes <- false
					}
				}(args, reply, i)
			}
			//close(votes)
			count := 1
			tot := 1
			for v := range votes {
				if v {
					count++
					if count > len(rf.peers)/2 {
						break
					}
				}
				tot++
				if tot == len(rf.peers) {
					break
				}
			}
			//if elected as leader
			rf.mu.Lock()
			if count > len(rf.peers)/2 {
				//commit an empty entry
				//rf.commitIndex++
				//rf.log = append(rf.log, LogEntry{term: rf.currentTerm, index: rf.commitIndex})
				//reset nextIndex
				rf.electionTimer.Stop()
				for i := range rf.nextIndex {
					rf.nextIndex[i] = rf.log[len(rf.log)-1].Index + 1
					rf.matchIndex[i] = 0
				}
				//broadcast winning result
				fmt.Println(rf.me, "is leader among", len(rf.peers), rf.commitIndex)
				rf.state = 2
				rf.heartbeatTicker = time.NewTicker(150*time.Millisecond)
				//if rf.currentLeader != rf.me {
				rf.currentLeader = rf.me
				rf.indexCount = make(map[int]int)
				rf.Done = make(chan struct{})
				//for i := range rf.commitQueue {
				//	rf.commitQueue[i] = make(chan LogEntry, 1)
				//}
				go rf.HeartBeatOnce()
				go rf.HeartBeat()
				go rf.CommitLog()
				//}
			} else {
				//fmt.Println(rf.me, "back to follower")
				rf.votedFor = -1
				//rf.state = 0
			}
			rf.mu.Unlock()
		}()
	}
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

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	//fmt.Println("send", args.LeaderID)
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.mu.Lock()
	if rf.state != 2 {
		rf.mu.Unlock()
		return -1, -1, false
	}
	index := len(rf.log)
	term := rf.currentTerm
	isLeader := true
	payload := LogEntry{
		Index: index,
		Term: term,
		Command: ApplyMsg{
			Index: index,
			Command: command,
			},
		}
	rf.log = append(rf.log, payload)
	rf.indexCount[index] = 1
	rf.mu.Unlock()
	go func(p LogEntry){
		//fmt.Println(rf.me, "start", p)
		rf.clientRequest <- p
	}(payload)
	// Your code here (2B).

	return index, term, isLeader
}

//
// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (rf *Raft) Kill() {
	// Your code here, if desired.
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.state == 2 {
		rf.state = 0
		rf.heartbeatTicker.Stop()
		close(rf.Done)
		//close(rf.clientRequest)
		//rf.mu.Unlock()
		//rf.wg.Wait()
		fmt.Println(rf.me, "Killed")
	}
	rf.electionTimer.Stop()
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
	rf.condition = sync.NewCond(&rf.mu)
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.state = 0
	rf.currentTerm = 0
	rf.currentLeader = -1
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.votedFor = -1
	rf.log = []LogEntry{}
	rf.indexCount = make(map[int]int)
	rf.log = append(rf.log, LogEntry{Term:rf.currentTerm,Index:rf.commitIndex})
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))
	rf.electionTimer = time.NewTimer(300*time.Millisecond+time.Duration(rand.Intn(100))*time.Millisecond)
	rf.Done = make(chan struct{})
	rf.commitQueue = make([]chan LogEntry, len(rf.peers))
	for i := range rf.commitQueue {
		rf.commitQueue[i] = make(chan LogEntry, 1)
	}
	rf.clientRequest = make(chan LogEntry)
	rf.applyCh = applyCh
	rf.wg = &sync.WaitGroup{}
	// Your initialization code here (2A, 2B, 2C).

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	for i := rf.commitIndex + 1; i <= rf.log[len(rf.log)-1].Index; i++ {
		rf.indexCount[i] = 1
	}

	fmt.Println(rf.me, "is up", rf.commitIndex, len(rf.log))
	go rf.ElectLeader()
	return rf
}
