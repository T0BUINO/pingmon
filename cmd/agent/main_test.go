package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
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

func TestProbeTasksLimitsConcurrencyAndKeepsAllResults(t *testing.T) {
	const taskCount = 17
	const concurrency = 3
	tasks := make([]model.Task, taskCount)
	var active atomic.Int32
	var peak atomic.Int32
	probe := func(agent string, task model.Task) model.Result {
		current := active.Add(1)
		for {
			old := peak.Load()
			if current <= old || peak.CompareAndSwap(old, current) {
				break
			}
		}
		time.Sleep(5 * time.Millisecond)
		active.Add(-1)
		return model.Result{Agent: agent, TargetName: task.Target.Name}
	}
	for i := range tasks {
		tasks[i].Target.Name = fmt.Sprintf("target-%d", i)
	}
	results := probeTasks(config.AgentConfig{AgentName: "agent-1", ProbeConcurrency: concurrency}, tasks, "203.0.113.1", probe)
	if len(results) != taskCount {
		t.Fatalf("results = %d, want %d", len(results), taskCount)
	}
	if got := peak.Load(); got > concurrency {
		t.Fatalf("peak concurrency = %d, want <= %d", got, concurrency)
	}
}

func TestFetchAgentIPCachesSuccessfulLookup(t *testing.T) {
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		_, _ = w.Write([]byte("203.0.113.9"))
	}))
	defer server.Close()

	agentIPCache.Lock()
	agentIPCache.ipv4URL, agentIPCache.ipv6URL, agentIPCache.value, agentIPCache.expiresAt = "", "", "", time.Time{}
	agentIPCache.Unlock()
	cfg := config.AgentConfig{PublicIPv4URL: server.URL}
	if first, second := fetchAgentIP(cfg), fetchAgentIP(cfg); first != second || first != "203.0.113.9" {
		t.Fatalf("cached values = %q, %q", first, second)
	}
	if got := calls.Load(); got != 1 {
		t.Fatalf("lookup calls = %d, want 1", got)
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
