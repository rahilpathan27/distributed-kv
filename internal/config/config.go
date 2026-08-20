package config

import (
	"flag"
	"fmt"
	"strings"
)

// Config holds the configuration for a distributed KV store node.
type Config struct {
	Port       int      // TCP port for client connections
	ReplPort   int      // TCP port for replication connections
	DataDir    string   // Directory for WAL and data files
	Role       string   // "leader" or "follower"
	LeaderAddr string   // Leader address (for followers)
	NodeID     string   // Unique node identifier
	Peers      []string // List of peer addresses (for election)
}

// ParseFlags parses command-line flags into a Config.
func ParseFlags() *Config {
	c := &Config{}
	flag.IntVar(&c.Port, "port", 7070, "TCP port for client connections")
	flag.IntVar(&c.ReplPort, "repl-port", 7071, "TCP port for replication")
	flag.StringVar(&c.DataDir, "data-dir", "./data", "directory for WAL and data files")
	flag.StringVar(&c.Role, "role", "leader", "node role: leader or follower")
	flag.StringVar(&c.LeaderAddr, "leader-addr", "", "leader address for followers")
	flag.StringVar(&c.NodeID, "node-id", "", "unique node identifier")
	
	// Peers will be comma-separated
	var peersStr string
	flag.StringVar(&peersStr, "peers", "", "comma-separated list of peer addresses")
	flag.Parse()
	if peersStr != "" {
		c.Peers = strings.Split(peersStr, ",")
	}
	return c
}

// Validate checks the config for errors.
func (c *Config) Validate() error {
	if c.Role != "leader" && c.Role != "follower" {
		return fmt.Errorf("invalid role: %s (must be 'leader' or 'follower')", c.Role)
	}
	if c.Role == "follower" && c.LeaderAddr == "" {
		return fmt.Errorf("followers must specify --leader-addr")
	}
	if c.Port == c.ReplPort {
		return fmt.Errorf("client port and replication port must differ")
	}
	return nil
}
