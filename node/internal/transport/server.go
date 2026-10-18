package transport

import (
	"mifado/internal/node"
	"net/http"

	"github.com/gin-gonic/gin"
)

type Server struct {
	node   *node.Node
	router *gin.Engine
}

func NewServer(node *node.Node) *Server {
	r := gin.New()
	r.Use(gin.Recovery())
	s := &Server{
		node:   node,
		router: r,
	}
	s.registerRoutes()
	return s
}

func (s *Server) registerRoutes() {
	raftGroup := s.router.Group("/raft")
	{
		raftGroup.POST("/request_vote", func(c *gin.Context) {
			var args node.RequestVoteArgs
			if err := c.ShouldBindJSON(&args); err != nil {
				c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
				return
			}
			c.JSON(http.StatusOK, s.node.HandleRequestVote(args))
		})

		raftGroup.POST("/append_entries", func(c *gin.Context) {
			var args node.AppendEntriesArgs
			if err := c.ShouldBindJSON(&args); err != nil {
				c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
				return
			}
			c.JSON(http.StatusOK, s.node.HandleAppendEntries(args))
		})
	}

	s.router.GET("/status", func(c *gin.Context) {
		c.JSON(http.StatusOK, s.node.HandleStatus())
	})
}

func (s *Server) Start(addr string) error {
	go s.router.Run(addr)
	return nil
}
