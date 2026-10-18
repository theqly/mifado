package node

import (
	"log"
)

func (n *Node) HandleRequestVote(args RequestVoteArgs) RequestVoteRes {
	n.mu.Lock()
	defer n.mu.Unlock()

	if args.Term > n.currentTerm {
		n.observeHigherTermLocked(args.Term)
	}

	res := RequestVoteRes{
		Term:        n.currentTerm,
		VoteGranted: false,
	}

	if args.Term < n.currentTerm {
		return res
	}

	if n.votedFor == noVote || n.votedFor == args.CandidateId {
		n.votedFor = args.CandidateId
		n.resetElectionTimerLocked()
		res.VoteGranted = true
		log.Printf("[node %d] voted for %d in term %d\n", n.id, args.CandidateId, n.currentTerm)
	}

	return res
}

func (n *Node) HandleAppendEntries(args AppendEntriesArgs) AppendEntriesRes {
	n.mu.Lock()
	defer n.mu.Unlock()

	res := AppendEntriesRes{
		Term:    n.currentTerm,
		Success: false,
	}

	if args.Term < n.currentTerm {
		return res
	}

	if args.Term > n.currentTerm {
		n.observeHigherTermLocked(args.Term)
	}

	n.role = Follower
	n.resetElectionTimerLocked()

	res.Term = n.currentTerm
	res.Success = true
	return res
}

func (n *Node) HandleStatus() map[string]interface{} {
	n.mu.Lock()
	defer n.mu.Unlock()

	var votedFor interface{}
	if n.votedFor != noVote {
		votedFor = n.votedFor
	}

	return map[string]interface{}{
		"id":           n.id,
		"current_term": n.currentTerm,
		"voted_for":    votedFor,
		"role":         string(n.role),
	}
}
