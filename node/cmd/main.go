package main

import (
	"log"
	"os"
	"strconv"

	"mifado/internal/config"
	"mifado/internal/node"
	"mifado/internal/transport"
)

func main() {
	nodeId := -1
	if nodeIdEnv := os.Getenv("NODE_ID"); nodeIdEnv != "" {
		if parsed, err := strconv.Atoi(nodeIdEnv); err == nil {
			nodeId = parsed
		}
	}

	if nodeId == -1 {
		log.Fatalf("node if isn't set")
	}

	iniPath := "cluster_ini.txt"

	cfg, err := config.LoadConfig(nodeId, iniPath)
	if err != nil {
		log.Fatalf("configuration error: %v", err)
	}

	log.Printf("starting Node %d with address %s", cfg.NodeId, cfg.BindAddr)

	n := node.NewNode(cfg.NodeId, cfg.Peers)

	server := transport.NewServer(n)
	if err := server.Start(cfg.BindAddr); err != nil {
		log.Fatalf("server error: %v", err)
	}

	n.Start()
}
