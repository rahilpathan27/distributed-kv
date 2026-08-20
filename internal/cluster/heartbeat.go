package cluster

import (
	"log"
	"sync"
	"time"
)

// NodeState represents the health state of a node.
type NodeState int

const (
	StateAlive NodeState = iota
	StateSuspect
	StateDead
)

// String returns the string representation of a NodeState.
func (s NodeState) String() string {
	switch s {
	case StateAlive:
		return "ALIVE"
	case StateSuspect:
		return "SUSPECT"
	case StateDead:
		return "DEAD"
	default:
		return "UNKNOWN"
	}
}

// NodeInfo holds information about a cluster node.
type NodeInfo struct {
	Addr        string
	State       NodeState
	LastSeen    time.Time
	MissedPings int
}

// HeartbeatManager monitors the health of cluster nodes.
type HeartbeatManager struct {
	mu        sync.RWMutex
	nodes     map[string]*NodeInfo
	interval  time.Duration
	timeout   time.Duration
	maxMisses int
	logger    *log.Logger
	quit      chan struct{}
	wg        sync.WaitGroup

	OnNodeDown func(addr string)
	OnNodeUp   func(addr string)
}

// NewHeartbeatManager creates a new HeartbeatManager.
func NewHeartbeatManager(interval, timeout time.Duration, logger *log.Logger) *HeartbeatManager {
	if logger == nil {
		logger = log.New(log.Writer(), "", log.LstdFlags)
	}
	return &HeartbeatManager{
		nodes:     make(map[string]*NodeInfo),
		interval:  interval,
		timeout:   timeout,
		maxMisses: 3,
		logger:    logger,
		quit:      make(chan struct{}),
	}
}

// RegisterNode adds a node to be monitored.
func (hm *HeartbeatManager) RegisterNode(addr string) {
	hm.mu.Lock()
	defer hm.mu.Unlock()
	hm.nodes[addr] = &NodeInfo{
		Addr:        addr,
		State:       StateAlive,
		LastSeen:    time.Now(),
		MissedPings: 0,
	}
}

// UnregisterNode removes a node from monitoring.
func (hm *HeartbeatManager) UnregisterNode(addr string) {
	hm.mu.Lock()
	defer hm.mu.Unlock()
	delete(hm.nodes, addr)
}

// RecordHeartbeat records that a heartbeat was received from a node.
// This should be called when a PONG (or any message) is received from the node.
func (hm *HeartbeatManager) RecordHeartbeat(addr string) {
	hm.mu.Lock()
	defer hm.mu.Unlock()

	node, exists := hm.nodes[addr]
	if !exists {
		return
	}

	node.LastSeen = time.Now()
	node.MissedPings = 0

	if node.State != StateAlive {
		hm.logger.Printf("Node %s is back ALIVE", addr)
		node.State = StateAlive
		if hm.OnNodeUp != nil {
			hm.OnNodeUp(addr)
		}
	}
}

// Start begins the heartbeat monitoring loop.
func (hm *HeartbeatManager) Start() {
	hm.wg.Add(1)
	go hm.checkLoop()
}

// Stop stops the heartbeat monitoring.
func (hm *HeartbeatManager) Stop() {
	close(hm.quit)
	hm.wg.Wait()
}

// GetNodeStates returns a snapshot of all node states.
func (hm *HeartbeatManager) GetNodeStates() map[string]NodeState {
	hm.mu.RLock()
	defer hm.mu.RUnlock()

	states := make(map[string]NodeState, len(hm.nodes))
	for addr, node := range hm.nodes {
		states[addr] = node.State
	}
	return states
}

// checkLoop runs periodically and checks node health.
func (hm *HeartbeatManager) checkLoop() {
	defer hm.wg.Done()
	ticker := time.NewTicker(hm.interval)
	defer ticker.Stop()

	for {
		select {
		case <-hm.quit:
			return
		case <-ticker.C:
			hm.mu.Lock()
			now := time.Now()
			for addr, node := range hm.nodes {
				if now.Sub(node.LastSeen) > hm.timeout {
					node.MissedPings++
					if node.MissedPings == 1 && node.State != StateSuspect && node.State != StateDead {
						hm.logger.Printf("Node %s marked SUSPECT", addr)
						node.State = StateSuspect
					}
					if node.MissedPings >= hm.maxMisses && node.State != StateDead {
						hm.logger.Printf("Node %s marked DEAD", addr)
						node.State = StateDead
						if hm.OnNodeDown != nil {
							hm.OnNodeDown(addr)
						}
					}
				} else {
					node.MissedPings = 0
					if node.State != StateAlive {
						hm.logger.Printf("Node %s marked ALIVE", addr)
						node.State = StateAlive
						if hm.OnNodeUp != nil {
							hm.OnNodeUp(addr)
						}
					}
				}
			}
			hm.mu.Unlock()
		}
	}
}
