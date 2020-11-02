package raft

import "log"

// Debugging
const Debug = 3

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

// func (rf *Raft) RaftPrintf() {
// 	rf.mu.Lock()
// 	defer rf.mu.Unlock()
// 	Format1 := "We print Server %d  %s  term is %d  dead is %t  log length is %d  commit index is %d  last applied is %d"
// 	// Format2 := "We print Server %d  nextIndex is %v  matchIndex is %v  logs are %v"
// 	WARNING(Format1, rf.me, rf.currentState, rf.currentTerm, rf.killed(), len(rf.logs), rf.commitIndex, rf.lastApplied)
// 	// WARNING(Format2, rf.me, rf.nextIndex, rf.matchIndex, rf.logs)
// }

// func INFO(format string, a ...interface{}) (n int, err error) {
// 	if Debug > 2 {
// 		log.Printf(format, a...)
// 	}
// 	return
// }

// func WARNING(format string, a ...interface{}) (n int, err error) {
// 	if Debug > 1 {
// 		log.Printf(format, a...)
// 	}
// 	return
// }
