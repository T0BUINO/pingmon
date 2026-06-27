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

func TestLoadAgentProbeConcurrency(t *testing.T) {
	path := filepath.Join(t.TempDir(), "agent.toml")
	if err := os.WriteFile(path, []byte("probe_concurrency = 7\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	cfg, err := LoadAgent(path, "toml")
	if err != nil {
		t.Fatalf("LoadAgent: %v", err)
	}
	if cfg.ProbeConcurrency != 7 {
		t.Fatalf("ProbeConcurrency = %d, want 7", cfg.ProbeConcurrency)
	}
}

func TestLoadAgentSecurityAndQueueSettings(t *testing.T) {
	path := filepath.Join(t.TempDir(), "agent.toml")
	if err := os.WriteFile(path, []byte("agent_token = \"secret\"\nmax_pending_results = 42\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	cfg, err := LoadAgent(path, "toml")
	if err != nil {
		t.Fatal(err)
	}
	if cfg.AgentToken != "secret" || cfg.MaxPendingResults != 42 {
		t.Fatalf("config = %+v", cfg)
	}
}
