package node

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"math/rand"
	"net/http"
	"sync"
	"time"
)

type Role string

const (
	Follower  Role = "follower"
	Candidate Role = "candidate"
	Leader    Role = "leader"
)

const (
	noVote = -1

	minElectionTimeout = 450 * time.Millisecond
	maxElectionTimeout = 600 * time.Millisecond
	electionCheckEvery = 10 * time.Millisecond
	heartbeatInterval  = 50 * time.Millisecond
	rpcTimeout         = 500 * time.Millisecond
)

type Node struct {
	mu sync.Mutex

	id    int
	peers map[int]string

	currentTerm int
	votedFor    int
	role        Role

	electionTimer   time.Time
	electionTimeout time.Duration

	client *http.Client
	ctx    context.Context
	cancel context.CancelFunc

	stopCh chan struct{}
}

func NewNode(id int, peers map[int]string) *Node {
	ctx, cancel := context.WithCancel(context.Background())

	return &Node{
		id:          id,
		peers:       peers,
		currentTerm: 0,
		votedFor:    noVote,
		role:        Follower,
		client:      &http.Client{Timeout: rpcTimeout},
		ctx:         ctx,
		cancel:      cancel,
		stopCh:      make(chan struct{}),
	}
}

func (n *Node) Start() {
	n.mu.Lock()
	n.resetElectionTimerLocked()
	n.mu.Unlock()

	n.runElectionTimer()
}

func (n *Node) Stop() {
	close(n.stopCh)
	n.cancel()
}

func (n *Node) resetElectionTimerLocked() {
	span := maxElectionTimeout - minElectionTimeout
	steps := int(span / time.Millisecond)
	n.electionTimeout = minElectionTimeout + time.Duration(rand.Intn(steps))*time.Millisecond
	n.electionTimer = time.Now()
}

func (n *Node) runElectionTimer() {
	ticker := time.NewTicker(electionCheckEvery)
	defer ticker.Stop()

	for {
		select {
		case <-n.stopCh:
			return
		case <-ticker.C:
			n.mu.Lock()
			if n.role != Leader && n.electionTimeout > 0 && time.Since(n.electionTimer) >= n.electionTimeout {
				n.startElectionLocked()
			}
			n.mu.Unlock()
		}
	}
}

func (n *Node) startElectionLocked() {
	n.currentTerm++
	n.role = Candidate
	n.votedFor = n.id
	n.resetElectionTimerLocked()

	term := n.currentTerm
	candidateId := n.id
	votes := 1
	majority := len(n.peers)/2 + 1

	fmt.Printf("[node %d] starting election for term %d\n", n.id, term)

	if votes >= majority {
		n.becomeLeaderLocked(term)
		return
	}

	for peerId, peerURL := range n.peers {
		if peerId == n.id {
			continue
		}

		go func(id int, url string) {
			args := RequestVoteArgs{
				Term:        term,
				CandidateId: candidateId,
			}
			var res RequestVoteRes

			err := n.sendRPC(url+"/raft/request_vote", args, &res)
			if err != nil {
				return
			}

			n.mu.Lock()
			defer n.mu.Unlock()

			if res.Term > n.currentTerm {
				n.observeHigherTermLocked(res.Term)
				return
			}

			if n.role != Candidate || n.currentTerm != term || res.Term != term {
				return
			}

			if res.VoteGranted {
				votes++
				if votes >= majority {
					n.becomeLeaderLocked(term)
				}
			}
		}(peerId, peerURL)
	}
}

func (n *Node) becomeLeaderLocked(term int) {
	n.role = Leader
	fmt.Printf("[node %d] elected leader for term %d\n", n.id, term)

	n.startHeartbeats(term)
}

func (n *Node) startHeartbeats(term int) {
	for peerId, peerURL := range n.peers {
		if peerId == n.id {
			continue
		}
		go n.runHeartbeatPeer(peerURL, term)
	}
}

func (n *Node) runHeartbeatPeer(url string, term int) {
	ticker := time.NewTicker(heartbeatInterval)
	defer ticker.Stop()

	for {
		n.mu.Lock()
		leaderId := n.id
		n.mu.Unlock()

		args := AppendEntriesArgs{
			Term:     term,
			LeaderId: leaderId,
		}
		var res AppendEntriesRes
		err := n.sendRPC(url+"/raft/append_entries", args, &res)
		if err == nil {
			n.mu.Lock()
			if res.Term > n.currentTerm {
				n.observeHigherTermLocked(res.Term)
			}
			stillLeader := n.role == Leader && n.currentTerm == term
			n.mu.Unlock()

			if !stillLeader {
				return
			}
		}

		select {
		case <-n.stopCh:
			return
		case <-ticker.C:
		}
	}
}

func (n *Node) observeHigherTermLocked(term int) {
	if term <= n.currentTerm {
		return
	}
	n.currentTerm = term
	n.votedFor = noVote
	n.role = Follower
}

func (n *Node) sendRPC(url string, body interface{}, result interface{}) error {
	data, err := json.Marshal(body)
	if err != nil {
		return fmt.Errorf("marshal rpc request: %w", err)
	}

	req, err := http.NewRequestWithContext(
		n.ctx,
		http.MethodPost,
		url,
		bytes.NewReader(data),
	)
	if err != nil {
		return fmt.Errorf("create rpc request for %s: %w", url, err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := n.client.Do(req)
	if err != nil {
		return fmt.Errorf("send rpc to %s: %w", url, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode < http.StatusOK || resp.StatusCode >= http.StatusMultipleChoices {
		return fmt.Errorf("rpc %s returned http status %s", url, resp.Status)
	}

	if err := json.NewDecoder(resp.Body).Decode(result); err != nil {
		return fmt.Errorf("decode rpc response from %s: %w", url, err)
	}

	return nil
}
