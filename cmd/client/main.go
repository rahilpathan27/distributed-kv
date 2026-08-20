package main

import (
	"bufio"
	"flag"
	"fmt"
	"net"
	"os"
	"strings"
)

func main() {
	addr := flag.String("addr", "localhost:7070", "server address")
	flag.Parse()

	conn, err := net.Dial("tcp", *addr)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to connect to %s: %v\n", *addr, err)
		os.Exit(1)
	}
	defer conn.Close()

	fmt.Printf("Connected to %s\n", *addr)
	fmt.Println("Type commands: SET key value | GET key | DELETE key | PING | QUIT")

	scanner := bufio.NewScanner(os.Stdin)
	reader := bufio.NewReader(conn)

	for {
		fmt.Print("dkv> ")
		if !scanner.Scan() {
			break // EOF or error
		}

		line := scanner.Text()
		trimmed := strings.TrimSpace(line)

		if strings.EqualFold(trimmed, "QUIT") || strings.EqualFold(trimmed, "EXIT") {
			fmt.Println("Bye!")
			break
		}

		if trimmed == "" {
			continue
		}

		// Send to server
		_, err := fmt.Fprintf(conn, "%s\n", line)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Connection error: %v\n", err)
			break
		}

		// Read response
		response, err := reader.ReadString('\n')
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error reading response: %v\n", err)
			break
		}

		fmt.Println(strings.TrimRight(response, "\r\n"))
	}

	if err := scanner.Err(); err != nil {
		fmt.Fprintf(os.Stderr, "Error reading stdin: %v\n", err)
	}
}
