package protocol

import (
	"testing"
)

func TestParseCommand_Set(t *testing.T) {
	t.Parallel()
	cmd, err := ParseCommand("SET mykey myval")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cmd.Type != CmdSet || cmd.Key != "mykey" || cmd.Value != "myval" {
		t.Errorf("unexpected command: %+v", cmd)
	}
}

func TestParseCommand_Get(t *testing.T) {
	t.Parallel()
	cmd, err := ParseCommand("GET mykey")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cmd.Type != CmdGet || cmd.Key != "mykey" {
		t.Errorf("unexpected command: %+v", cmd)
	}
}

func TestParseCommand_Delete(t *testing.T) {
	t.Parallel()
	cmd, err := ParseCommand("DELETE mykey")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cmd.Type != CmdDelete || cmd.Key != "mykey" {
		t.Errorf("unexpected command: %+v", cmd)
	}
}

func TestParseCommand_Ping(t *testing.T) {
	t.Parallel()
	cmd, err := ParseCommand("PING")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cmd.Type != CmdPing {
		t.Errorf("unexpected command: %+v", cmd)
	}
}

func TestParseCommand_CaseInsensitive(t *testing.T) {
	t.Parallel()
	tests := []struct {
		input    string
		expected CmdType
	}{
		{"set k v", CmdSet},
		{"SeT k v", CmdSet},
		{"get k", CmdGet},
		{"deLEte k", CmdDelete},
		{"piNg", CmdPing},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.input, func(t *testing.T) {
			t.Parallel()
			cmd, err := ParseCommand(tt.input)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if cmd.Type != tt.expected {
				t.Errorf("expected %v, got %v", tt.expected, cmd.Type)
			}
		})
	}
}

func TestParseCommand_InvalidCommands(t *testing.T) {
	t.Parallel()
	tests := []struct {
		input string
	}{
		{""},
		{"   "},
		{"UNKNOWN k v"},
		{"SET k"},
		{"SET k v extra"},
		{"GET"},
		{"GET k extra"},
		{"DELETE"},
		{"DELETE k extra"},
		{"PING extra"},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.input, func(t *testing.T) {
			t.Parallel()
			_, err := ParseCommand(tt.input)
			if err == nil {
				t.Errorf("expected error for input %q, got nil", tt.input)
			}
		})
	}
}

func TestParseCommand_Whitespace(t *testing.T) {
	t.Parallel()
	cmd, err := ParseCommand("   SET   mykey    myval   ")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cmd.Type != CmdSet || cmd.Key != "mykey" || cmd.Value != "myval" {
		t.Errorf("unexpected command: %+v", cmd)
	}
}

func TestFormatOK(t *testing.T) {
	t.Parallel()
	expected := "+OK\n"
	if got := FormatOK(); got != expected {
		t.Errorf("expected %q, got %q", expected, got)
	}
}

func TestFormatValue(t *testing.T) {
	t.Parallel()
	expected := "+val\n"
	if got := FormatValue("val"); got != expected {
		t.Errorf("expected %q, got %q", expected, got)
	}
}

func TestFormatError(t *testing.T) {
	t.Parallel()
	expected := "-ERR bad\n"
	if got := FormatError("bad"); got != expected {
		t.Errorf("expected %q, got %q", expected, got)
	}
}

func TestFormatPong(t *testing.T) {
	t.Parallel()
	expected := "+PONG\n"
	if got := FormatPong(); got != expected {
		t.Errorf("expected %q, got %q", expected, got)
	}
}

func TestCmdType_String(t *testing.T) {
	t.Parallel()
	tests := []struct {
		cmd      CmdType
		expected string
	}{
		{CmdSet, "SET"},
		{CmdGet, "GET"},
		{CmdDelete, "DELETE"},
		{CmdPing, "PING"},
		{CmdType(99), "UNKNOWN"},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.expected, func(t *testing.T) {
			t.Parallel()
			if got := tt.cmd.String(); got != tt.expected {
				t.Errorf("expected %q, got %q", tt.expected, got)
			}
		})
	}
}
