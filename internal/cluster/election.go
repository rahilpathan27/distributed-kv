package cluster

import (
	"bufio"
	"fmt"
	"io"
	"log"
	"net"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/rahilpathan27/distributed-kv/internal/wal"
)

// Role represents the current role of a node in the cluster.
type Role int

const (
	RoleFollower Role = iota
	RoleCandidate
	RoleLeader
)

// String returns the string representation of a Role.
func (r Role) String() string {
	switch r {
	case RoleFollower:
		return "FOLLOWER"
	case RoleCandidate:
		return "CANDIDATE"
	case RoleLeader:
		return "LEADER"
	default:
		return "UNKNOWN"
	}
}

// ElectionManager handles leader election among cluster nodes.
type ElectionManager struct {
	mu            sync.Mutex
	nodeID        string
	role          Role
	currentLeader string
	peers         []string // addresses of all other nodes
	walLog        *wal.WAL
	logger        *log.Logger
	listener      net.Listener
	quit          chan struct{}
	wg            sync.WaitGroup

	electionTimeout time.Duration // default 5s

	// Callbacks
	OnBecomeLeader   func()              // called when this node becomes leader
	OnBecomeFollower func(leader string) // called when a new leader is elected
}

// NewElectionManager creates a new ElectionManager.
func NewElectionManager(nodeID string, peers []string, w *wal.WAL, logger *log.Logger) *ElectionManager {
	if logger == nil {
		logger = log.New(io.Discard, "", 0)
	}
	return &ElectionManager{
		nodeID:          nodeID,
		role:            RoleFollower,
		peers:           peers,
		walLog:          w,
		logger:          logger,
		quit:            make(chan struct{}),
		electionTimeout: 5 * time.Second,
	}
}

// Start begins listening for election messages on the given address.
func (em *ElectionManager) Start(addr string) error {
	em.mu.Lock()
	defer em.mu.Unlock()

	l, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}
	em.listener = l
	em.wg.Add(1)
	go func() {
		defer em.wg.Done()
		for {
			conn, err := em.listener.Accept()
			if err != nil {
				select {
				case <-em.quit:
					return
				default:
					em.logger.Printf("accept error: %v", err)
				}
				return
			}
			em.wg.Add(1)
			go em.handlePeer(conn)
		}
	}()
	return nil
}

// Stop stops the election manager.
func (em *ElectionManager) Stop() error {
	em.mu.Lock()
	if em.listener != nil {
		em.listener.Close()
	}
	close(em.quit)
	em.mu.Unlock()

	em.wg.Wait()
	return nil
}

// GetRole returns the current role.
func (em *ElectionManager) GetRole() Role {
	em.mu.Lock()
	defer em.mu.Unlock()
	return em.role
}

// GetLeader returns the current leader's node ID.
func (em *ElectionManager) GetLeader() string {
	em.mu.Lock()
	defer em.mu.Unlock()
	return em.currentLeader
}

// StartElection initiates a leader election.
// This is called when the leader heartbeat times out.
func (em *ElectionManager) StartElection() {
	em.mu.Lock()
	em.role = RoleCandidate
	var lastLogIndex uint64
	if em.walLog != nil {
		lastLogIndex = em.walLog.LastIndex()
	}
	peers := make([]string, len(em.peers))
	copy(peers, em.peers)
	em.mu.Unlock()

	em.logger.Printf("Starting election, nodeID=%s, lastLogIndex=%d", em.nodeID, lastLogIndex)

	votes := 1 // self vote
	var mu sync.Mutex
	var wg sync.WaitGroup

	msg := fmt.Sprintf("VOTE_REQUEST %s %d\n", em.nodeID, lastLogIndex)

	for _, p := range peers {
		wg.Add(1)
		go func(peer string) {
			defer wg.Done()

			// Try with timeout
			ch := make(chan struct{})
			go func() {
				resp, err := em.sendElectionMessage(peer, msg)
				if err == nil && strings.TrimSpace(resp) == "VOTE_YES" {
					mu.Lock()
					votes++
					mu.Unlock()
				}
				close(ch)
			}()

			select {
			case <-ch:
			case <-time.After(2 * time.Second): // 2 seconds vote collection timeout
			}
		}(p)
	}

	wg.Wait()

	em.mu.Lock()
	defer em.mu.Unlock()

	// Ensure we are still candidate (didn't receive LEADER_ANNOUNCE during election)
	if em.role != RoleCandidate {
		return
	}

	majority := (len(em.peers)+1)/2 + 1

	if votes >= majority {
		em.role = RoleLeader
		em.currentLeader = em.nodeID
		em.logger.Printf("Elected as leader with %d votes", votes)

		if em.OnBecomeLeader != nil {
			em.OnBecomeLeader()
		}

		announceMsg := fmt.Sprintf("LEADER_ANNOUNCE %s\n", em.nodeID)
		for _, p := range em.peers {
			go em.sendElectionMessage(p, announceMsg)
		}
	} else {
		em.role = RoleFollower
		em.logger.Printf("Failed to win election, got %d votes, need %d", votes, majority)
	}
}

// handlePeer handles an incoming election protocol connection.
func (em *ElectionManager) handlePeer(conn net.Conn) {
	defer em.wg.Done()
	defer conn.Close()

	reader := bufio.NewReader(conn)
	msg, err := reader.ReadString('\n')
	if err != nil {
		return
	}
	msg = strings.TrimSpace(msg)

	parts := strings.Split(msg, " ")
	if len(parts) == 0 {
		return
	}

	cmd := parts[0]

	em.mu.Lock()
	defer em.mu.Unlock()

	switch cmd {
	case "VOTE_REQUEST":
		if len(parts) != 3 {
			fmt.Fprintf(conn, "VOTE_NO invalid_format\n")
			return
		}
		candidateID := parts[1]
		candidateLogIndex, err := strconv.ParseUint(parts[2], 10, 64)
		if err != nil {
			fmt.Fprintf(conn, "VOTE_NO invalid_index\n")
			return
		}

		var ourLogIndex uint64
		if em.walLog != nil {
			ourLogIndex = em.walLog.LastIndex()
		}

		// simplified: VOTE_YES if candidate's index >= our index
		if candidateLogIndex >= ourLogIndex {
			fmt.Fprintf(conn, "VOTE_YES\n")
		} else {
			fmt.Fprintf(conn, "VOTE_NO index_too_low\n")
		}
		_ = candidateID // used if logging

	case "LEADER_ANNOUNCE":
		if len(parts) != 2 {
			fmt.Fprintf(conn, "ACK invalid_format\n")
			return
		}
		leaderID := parts[1]

		em.role = RoleFollower
		em.currentLeader = leaderID
		if em.OnBecomeFollower != nil {
			// run in goroutine to prevent deadlock if callback calls manager methods
			go em.OnBecomeFollower(leaderID) 
		}
		fmt.Fprintf(conn, "ACK\n")
	}
}

func (em *ElectionManager) sendElectionMessage(peerAddr, msg string) (string, error) {
	conn, err := net.DialTimeout("tcp", peerAddr, 1*time.Second)
	if err != nil {
		return "", err
	}
	defer conn.Close()

	conn.SetDeadline(time.Now().Add(2 * time.Second))

	_, err = conn.Write([]byte(msg))
	if err != nil {
		return "", err
	}

	reader := bufio.NewReader(conn)
	resp, err := reader.ReadString('\n')
	if err != nil {
		return "", err
	}
	return strings.TrimSpace(resp), nil
}
