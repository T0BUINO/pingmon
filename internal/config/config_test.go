package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestLoadInvalidIntegerReturnsError(t *testing.T) {
	path := filepath.Join(t.TempDir(), "supervisor.toml")
	if err := os.WriteFile(path, []byte("retention_days = nope\n"), 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}

	_, err := Load(path, "toml")
	if err == nil || !strings.Contains(err.Error(), "line 1") || !strings.Contains(err.Error(), "retention_days") {
		t.Fatalf("Load error = %v, want field-specific integer error", err)
	}
}

func TestLoadAgentInvalidIntegerReturnsError(t *testing.T) {
	path := filepath.Join(t.TempDir(), "agent.toml")
	if err := os.WriteFile(path, []byte("poll_interval_seconds = later\n"), 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}

	_, err := LoadAgent(path, "toml")
	if err == nil || !strings.Contains(err.Error(), "line 1") || !strings.Contains(err.Error(), "poll_interval_seconds") {
		t.Fatalf("LoadAgent error = %v, want field-specific integer error", err)
	}
}
