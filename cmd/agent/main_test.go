package main

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"pingmon/internal/config"
	"pingmon/internal/model"
)

func TestFetchTasksIncludesAgentIdentity(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if got := r.URL.Query().Get("agent"); got != "agent-1" {
			t.Errorf("agent = %q", got)
		}
		if got := r.URL.Query().Get("agent_ip"); got != "203.0.113.1" {
			t.Errorf("agent_ip = %q", got)
		}
		_ = json.NewEncoder(w).Encode([]model.Task{{Params: model.PingParams{ScheduleSeconds: 12}}})
	}))
	defer server.Close()

	tasks, err := fetchTasks(server.URL, "agent-1", "203.0.113.1")
	if err != nil {
		t.Fatalf("fetchTasks: %v", err)
	}
	if got := nextPollInterval(tasks, 30); got != 12 {
		t.Fatalf("nextPollInterval = %d, want 12", got)
	}
}

func TestUploadResultsUsesClientTimeout(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(100 * time.Millisecond)
	}))
	defer server.Close()

	previous := supervisorHTTPClient
	supervisorHTTPClient = &http.Client{Timeout: 20 * time.Millisecond}
	t.Cleanup(func() { supervisorHTTPClient = previous })

	err := uploadResults(server.URL, []model.Result{{Agent: "agent-1"}})
	if err == nil {
		t.Fatal("uploadResults succeeded, want timeout")
	}
}

func TestFetchAgentIPCombinesDualStackResults(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/v4" {
			_, _ = w.Write([]byte("203.0.113.1"))
			return
		}
		_, _ = w.Write([]byte("2001:db8::1"))
	}))
	defer server.Close()

	got := fetchAgentIP(config.AgentConfig{
		PublicIPv4URL: server.URL + "/v4",
		PublicIPv6URL: server.URL + "/v6",
	})
	if got != "203.0.113.1 / 2001:db8::1" {
		t.Fatalf("fetchAgentIP = %q", got)
	}
}
