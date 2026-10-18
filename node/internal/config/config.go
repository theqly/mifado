package config

import (
	"bufio"
	"fmt"
	"net/url"
	"os"
	"strconv"
	"strings"
)

type Config struct {
	NodeId   int
	BindAddr string
	Peers    map[int]string
}

func LoadConfig(nodeId int, iniPath string) (*Config, error) {
	file, err := os.Open(iniPath)
	if err != nil {
		return nil, fmt.Errorf("failed to open cluster config: %w", err)
	}
	defer file.Close()

	peers := make(map[int]string)
	var bindAddr string

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}

		parts := strings.SplitN(line, "=", 2)
		if len(parts) != 2 {
			continue
		}

		id, err := strconv.Atoi(strings.TrimSpace(parts[0]))
		if err != nil {
			continue
		}

		url := strings.TrimSpace(parts[1])

		peers[id] = url
		if id == nodeId {
			bindAddr = url
		}
	}

	if bindAddr == "" {
		return nil, fmt.Errorf("node id %d not found in %s", nodeId, iniPath)
	}

	parsedURL, err := url.Parse(bindAddr)
	if err != nil {
		return nil, fmt.Errorf("invalid url for node %d: %w", nodeId, err)
	}

	port := parsedURL.Port()
	if port == "" {
		port = "8080"
	}
	bindAddr = ":" + port

	return &Config{
		NodeId:   nodeId,
		BindAddr: bindAddr,
		Peers:    peers,
	}, nil
}
