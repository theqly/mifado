package node

type RequestVoteArgs struct {
	Term        int `json:"termIn"`
	CandidateId int `json:"candidateId"`
}

type RequestVoteRes struct {
	Term        int  `json:"termOut"`
	VoteGranted bool `json:"voteGranted"`
}

type AppendEntriesArgs struct {
	Term     int `json:"termIn"`
	LeaderId int `json:"leaderId"`
}

type AppendEntriesRes struct {
	Term    int  `json:"termOut"`
	Success bool `json:"success"`
}
