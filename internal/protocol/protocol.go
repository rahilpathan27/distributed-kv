package protocol

import (
	"errors"
	"fmt"
	"strings"
)

// CmdType represents the type of a command.
type CmdType int

const (
	CmdSet CmdType = iota // SET key value
	CmdGet                // GET key
	CmdDelete             // DELETE key
	CmdPing               // PING
)

// Command represents a parsed client command.
type Command struct {
	Type  CmdType
	Key   string
	Value string
}

// ParseCommand parses a single line of text into a Command.
// Format:
//   SET key value   - sets a key-value pair
//   GET key         - retrieves a value by key
//   DELETE key      - deletes a key
//   PING            - health check
// Returns an error for unrecognized commands or missing arguments.
func ParseCommand(line string) (Command, error) {
	line = strings.TrimSpace(line)
	if line == "" {
		return Command{}, errors.New("empty command")
	}

	fields := strings.Fields(line)
	cmdName := strings.ToUpper(fields[0])

	switch cmdName {
	case "SET":
		if len(fields) != 3 {
			return Command{}, errors.New("SET requires key and value")
		}
		return Command{Type: CmdSet, Key: fields[1], Value: fields[2]}, nil
	case "GET":
		if len(fields) != 2 {
			return Command{}, errors.New("GET requires key")
		}
		return Command{Type: CmdGet, Key: fields[1]}, nil
	case "DELETE":
		if len(fields) != 2 {
			return Command{}, errors.New("DELETE requires key")
		}
		return Command{Type: CmdDelete, Key: fields[1]}, nil
	case "PING":
		if len(fields) != 1 {
			return Command{}, errors.New("PING requires no arguments")
		}
		return Command{Type: CmdPing}, nil
	default:
		return Command{}, fmt.Errorf("unknown command: %s", fields[0])
	}
}

// FormatOK returns "+OK\n"
func FormatOK() string {
	return "+OK\n"
}

// FormatValue returns "+<value>\n"
func FormatValue(value string) string {
	return fmt.Sprintf("+%s\n", value)
}

// FormatError returns "-ERR <message>\n"
func FormatError(msg string) string {
	return fmt.Sprintf("-ERR %s\n", msg)
}

// FormatPong returns "+PONG\n"
func FormatPong() string {
	return "+PONG\n"
}

// String returns a human-readable representation of CmdType.
func (c CmdType) String() string {
	switch c {
	case CmdSet:
		return "SET"
	case CmdGet:
		return "GET"
	case CmdDelete:
		return "DELETE"
	case CmdPing:
		return "PING"
	default:
		return "UNKNOWN"
	}
}
