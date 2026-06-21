package main

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"pingmon/internal/config"
	"pingmon/internal/model"
)

type heartbeatCall struct {
	agent   string
	agentIP string
	seenAt  time.Time
}

type fakeStore struct {
	heartbeats        []heartbeatCall
	results           []model.Result
	deleted           []string
	resultsSinceCalls int
}

func (s *fakeStore) SaveAgentHeartbeat(agent, agentIP string, seenAt time.Time) error {
	s.heartbeats = append(s.heartbeats, heartbeatCall{agent: agent, agentIP: agentIP, seenAt: seenAt})
	return nil
}

func (s *fakeStore) ListAgentStatuses() ([]model.AgentStatus, error) {
	return nil, nil
}

func (s *fakeStore) DeleteAgent(agent string) error {
	s.deleted = append(s.deleted, agent)
	return nil
}

func (s *fakeStore) SaveResult(result model.Result) error {
	s.results = append(s.results, result)
	return nil
}

func TestHandleDeleteAgentDeletesAgentData(t *testing.T) {
	store := &fakeStore{}
	s := &server{cfg: config.DefaultConfig(), store: store, hub: newWebsocketHub()}
	req := httptest.NewRequest(http.MethodDelete, "/api/agents?agent=agent-1", nil)
	rr := httptest.NewRecorder()

	s.handleAgents(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200: %s", rr.Code, rr.Body.String())
	}
	if len(store.deleted) != 1 || store.deleted[0] != "agent-1" {
		t.Fatalf("deleted = %+v, want agent-1", store.deleted)
	}
}

func (s *fakeStore) ResultsSince(since time.Time) ([]model.Result, error) {
	s.resultsSinceCalls++
	return nil, nil
}

func (s *fakeStore) ResultsSinceForAgent(since time.Time, agent string) ([]model.Result, error) {
	s.resultsSinceCalls++
	return nil, nil
}

func (s *fakeStore) ResultsSinceCompacted(since, rawCutoff time.Time) ([]model.Result, error) {
	s.resultsSinceCalls++
	return nil, nil
}

func (s *fakeStore) ResultsSinceCompactedForAgent(since, rawCutoff time.Time, agent string) ([]model.Result, error) {
	s.resultsSinceCalls++
	return nil, nil
}

func (s *fakeStore) StreamResultsSince(since time.Time, agent string, fn func(model.Result) error) error {
	s.resultsSinceCalls++
	return nil
}

func (s *fakeStore) StreamResultsSinceCompacted(since, rawCutoff time.Time, agent string, fn func(model.Result) error) error {
	s.resultsSinceCalls++
	return nil
}

func (s *fakeStore) RollupBefore(cutoff time.Time, interval time.Duration) (int, error) {
	return 0, nil
}

func (s *fakeStore) DeleteBefore(cutoff time.Time) (int, error) {
	return 0, nil
}

func (s *fakeStore) DeleteRollupsBefore(cutoff time.Time) (int, error) {
	return 0, nil
}

func (s *fakeStore) Vacuum() error {
	return nil
}

func (s *fakeStore) ConsecutiveFailures(targetName, address string, port int) (int, error) {
	return 0, nil
}

func TestDashboardResultCacheServesExactBaseAndDeltaPoints(t *testing.T) {
	cache := &dashboardResultCache{dir: t.TempDir()}
	since := time.Date(2026, 6, 20, 0, 0, 0, 0, time.UTC)
	key := dashboardCacheKey{SelectedRange: "3d", Agent: "agent-1"}
	base := []model.Result{
		{
			Agent:            "agent-1",
			TargetName:       "web",
			Address:          "198.51.100.10",
			Port:             443,
			CheckedAt:        since.Add(time.Hour),
			SuccessCount:     1,
			AverageLatencyMS: 10,
			SuccessRate:      1,
		},
		{
			Agent:            "agent-1",
			TargetName:       "web",
			Address:          "198.51.100.10",
			Port:             443,
			CheckedAt:        since.Add(2 * time.Hour),
			SuccessCount:     1,
			AverageLatencyMS: 11,
			SuccessRate:      1,
		},
	}
	rr := httptest.NewRecorder()
	if err := cache.refresh(key, since, func(fn func(model.Result) error) error {
		for _, result := range base {
			if err := fn(result); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		t.Fatalf("cache refresh: %v", err)
	}
	if _, err := cache.writeIfReady(rr, key); err != nil {
		t.Fatalf("cache write: %v", err)
	}
	var got []model.Result
	if err := json.Unmarshal(rr.Body.Bytes(), &got); err != nil {
		t.Fatalf("decode base cache response: %v", err)
	}
	if len(got) != len(base) {
		t.Fatalf("base cache returned %d points, want %d", len(got), len(base))
	}

	time.Sleep(time.Millisecond)
	delta := model.Result{
		Agent:            "agent-1",
		TargetName:       "web",
		Address:          "198.51.100.10",
		Port:             443,
		CheckedAt:        since.Add(3 * time.Hour),
		SuccessCount:     1,
		AverageLatencyMS: 12,
		SuccessRate:      1,
	}
	cache.appendDelta([]model.Result{delta})
	rr = httptest.NewRecorder()
	if _, err := cache.writeIfReady(rr, key); err != nil {
		t.Fatalf("cache write with delta: %v", err)
	}
	if err := json.Unmarshal(rr.Body.Bytes(), &got); err != nil {
		t.Fatalf("decode delta cache response: %v", err)
	}
	if len(got) != len(base)+1 {
		t.Fatalf("delta cache returned %d points, want %d", len(got), len(base)+1)
	}
	if got[0].AverageLatencyMS != delta.AverageLatencyMS {
		t.Fatalf("first point latency = %v, want delta %v", got[0].AverageLatencyMS, delta.AverageLatencyMS)
	}

	time.Sleep(time.Millisecond)
	rebuilt := append([]model.Result{delta}, base...)
	if err := cache.refresh(key, since, func(fn func(model.Result) error) error {
		for _, result := range rebuilt {
			if err := fn(result); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		t.Fatalf("cache refresh with delta: %v", err)
	}
	rr = httptest.NewRecorder()
	if _, err := cache.writeIfReady(rr, key); err != nil {
		t.Fatalf("cache write after compact: %v", err)
	}
	if err := json.Unmarshal(rr.Body.Bytes(), &got); err != nil {
		t.Fatalf("decode compacted cache response: %v", err)
	}
	if len(got) != len(rebuilt) {
		t.Fatalf("compacted cache returned %d points, want %d", len(got), len(rebuilt))
	}
}

func TestDashboardResultCachePersistsAcrossInstances(t *testing.T) {
	dir := t.TempDir()
	key := dashboardCacheKey{SelectedRange: "7d"}
	result := model.Result{
		Agent:            "agent-1",
		TargetName:       "web",
		Address:          "198.51.100.10",
		Port:             443,
		CheckedAt:        time.Date(2026, 6, 20, 0, 0, 0, 0, time.UTC),
		SuccessCount:     1,
		AverageLatencyMS: 10,
		SuccessRate:      1,
	}
	first := &dashboardResultCache{dir: dir}
	if err := first.refresh(key, result.CheckedAt.Add(-time.Hour), func(fn func(model.Result) error) error {
		return fn(result)
	}); err != nil {
		t.Fatalf("cache refresh: %v", err)
	}

	second := &dashboardResultCache{dir: dir}
	rr := httptest.NewRecorder()
	if _, err := second.writeIfReady(rr, key); err != nil {
		t.Fatalf("cache write after restart: %v", err)
	}
	var got []model.Result
	if err := json.Unmarshal(rr.Body.Bytes(), &got); err != nil {
		t.Fatalf("decode persisted cache response: %v", err)
	}
	if len(got) != 1 || !got[0].CheckedAt.Equal(result.CheckedAt) {
		t.Fatalf("persisted cache = %+v, want one original result", got)
	}
}

func TestDashboardResultCacheServesStaleBaseForBackgroundRefresh(t *testing.T) {
	cache := &dashboardResultCache{dir: t.TempDir()}
	key := dashboardCacheKey{SelectedRange: "7d"}
	result := model.Result{
		Agent:            "agent-1",
		TargetName:       "web",
		Address:          "198.51.100.10",
		Port:             443,
		CheckedAt:        time.Now().UTC(),
		SuccessCount:     1,
		AverageLatencyMS: 10,
		SuccessRate:      1,
	}
	if err := cache.refresh(key, result.CheckedAt.Add(-time.Hour), func(fn func(model.Result) error) error {
		return fn(result)
	}); err != nil {
		t.Fatalf("cache refresh: %v", err)
	}
	meta := dashboardCacheMeta{
		Key:     key,
		Since:   result.CheckedAt.Add(-time.Hour),
		BuiltAt: time.Now().UTC().Add(-dashboardCacheFreshness - time.Second),
		Version: dashboardCacheVersion,
	}
	metaData, err := json.Marshal(meta)
	if err != nil {
		t.Fatalf("marshal meta: %v", err)
	}
	if err := os.WriteFile(cache.metaPath(key), metaData, 0644); err != nil {
		t.Fatalf("write stale meta: %v", err)
	}

	stale, err := cache.writeIfReady(httptest.NewRecorder(), key)
	if err != nil {
		t.Fatalf("stale cache write: %v", err)
	}
	if !stale {
		t.Fatal("stale cache was not reported stale")
	}
}

func TestHandleDashboardResultsMissQueuesBuildWithoutQueryingStore(t *testing.T) {
	store := &fakeStore{}
	s := &server{
		cfg:       config.DefaultConfig(),
		store:     store,
		dashCache: &dashboardResultCache{dir: t.TempDir()},
		dashJobs:  make(chan dashboardCacheKey, 1),
	}
	req := httptest.NewRequest(http.MethodGet, "/api/results?dashboard=1&range=7d&agent=agent-1", nil)
	rr := httptest.NewRecorder()

	s.handleResults(rr, req)

	if rr.Code != http.StatusAccepted {
		t.Fatalf("status = %d, want 202: %s", rr.Code, rr.Body.String())
	}
	if store.resultsSinceCalls != 0 {
		t.Fatalf("resultsSinceCalls = %d, want 0", store.resultsSinceCalls)
	}
	select {
	case key := <-s.dashJobs:
		if key.SelectedRange != "7d" || key.Agent != "agent-1" {
			t.Fatalf("queued key = %+v", key)
		}
	default:
		t.Fatal("cache build was not queued")
	}
}

func TestHandleDashboardResultsServesStaleCacheAndQueuesRefresh(t *testing.T) {
	dir := t.TempDir()
	key := dashboardCacheKey{SelectedRange: "7d", Agent: "agent-1"}
	result := model.Result{
		Agent:            "agent-1",
		TargetName:       "web",
		Address:          "198.51.100.10",
		Port:             443,
		CheckedAt:        time.Now().UTC(),
		SuccessCount:     1,
		AverageLatencyMS: 10,
		SuccessRate:      1,
	}
	cache := &dashboardResultCache{dir: dir}
	if err := cache.refresh(key, result.CheckedAt.Add(-time.Hour), func(fn func(model.Result) error) error {
		return fn(result)
	}); err != nil {
		t.Fatalf("cache refresh: %v", err)
	}
	meta := dashboardCacheMeta{
		Key:     key,
		Since:   result.CheckedAt.Add(-time.Hour),
		BuiltAt: time.Now().UTC().Add(-dashboardCacheFreshness - time.Second),
		Version: dashboardCacheVersion,
	}
	metaData, err := json.Marshal(meta)
	if err != nil {
		t.Fatalf("marshal meta: %v", err)
	}
	if err := os.WriteFile(cache.metaPath(key), metaData, 0644); err != nil {
		t.Fatalf("write stale meta: %v", err)
	}
	store := &fakeStore{}
	s := &server{
		cfg:       config.DefaultConfig(),
		store:     store,
		dashCache: cache,
		dashJobs:  make(chan dashboardCacheKey, 1),
	}
	req := httptest.NewRequest(http.MethodGet, "/api/results?dashboard=1&range=7d&agent=agent-1", nil)
	rr := httptest.NewRecorder()

	s.handleResults(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200: %s", rr.Code, rr.Body.String())
	}
	var got []model.Result
	if err := json.Unmarshal(rr.Body.Bytes(), &got); err != nil {
		t.Fatalf("decode stale cache response: %v", err)
	}
	if len(got) != 1 || got[0].Agent != result.Agent {
		t.Fatalf("stale cache response = %+v", got)
	}
	if store.resultsSinceCalls != 0 {
		t.Fatalf("resultsSinceCalls = %d, want 0", store.resultsSinceCalls)
	}
	select {
	case queued := <-s.dashJobs:
		if queued != key {
			t.Fatalf("queued key = %+v, want %+v", queued, key)
		}
	default:
		t.Fatal("stale cache refresh was not queued")
	}
}

func TestHandleTasksSavesAgentHeartbeat(t *testing.T) {
	store := &fakeStore{}
	s := &server{cfg: config.DefaultConfig(), store: store}
	req := httptest.NewRequest(http.MethodGet, "/api/tasks?agent=agent-1&agent_ip=203.0.113.10", nil)
	rr := httptest.NewRecorder()

	s.handleTasks(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200: %s", rr.Code, rr.Body.String())
	}
	if len(store.heartbeats) != 1 {
		t.Fatalf("heartbeats = %d, want 1", len(store.heartbeats))
	}
	if store.heartbeats[0].agent != "agent-1" || store.heartbeats[0].agentIP != "203.0.113.10" {
		t.Fatalf("heartbeat = %+v", store.heartbeats[0])
	}
}

func TestHandleReportSavesAgentHeartbeat(t *testing.T) {
	store := &fakeStore{}
	s := &server{cfg: config.DefaultConfig(), store: store, hub: newWebsocketHub()}
	body := `{"agent":"agent-1","agent_ip":"203.0.113.10","target_name":"web","address":"198.51.100.10","port":443,"checked_at":"2026-06-20T10:00:00Z","success_count":3,"failure_count":0,"average_latency_ms":4.2,"success_rate":1}`
	req := httptest.NewRequest(http.MethodPost, "/api/report", strings.NewReader(body))
	rr := httptest.NewRecorder()

	s.handleReport(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200: %s", rr.Code, rr.Body.String())
	}
	if len(store.results) != 1 {
		t.Fatalf("results = %d, want 1", len(store.results))
	}
	if len(store.heartbeats) != 1 {
		t.Fatalf("heartbeats = %d, want 1", len(store.heartbeats))
	}
	if store.heartbeats[0].agent != "agent-1" || store.heartbeats[0].agentIP != "203.0.113.10" {
		t.Fatalf("heartbeat = %+v", store.heartbeats[0])
	}
}

func TestHandleReportRejectsEmptyObject(t *testing.T) {
	store := &fakeStore{}
	status, body := postReport(t, store, `{}`)

	if status != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400: %s", status, body)
	}
	assertNoReportWrites(t, store)
}

func TestHandleReportRejectsNull(t *testing.T) {
	store := &fakeStore{}
	status, body := postReport(t, store, `null`)

	if status != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400: %s", status, body)
	}
	assertNoReportWrites(t, store)
}

func TestHandleReportRejectsMissingRequiredFieldsInArray(t *testing.T) {
	store := &fakeStore{}
	status, body := postReport(t, store, `[{}]`)

	if status != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400: %s", status, body)
	}
	assertNoReportWrites(t, store)
}

func TestHandleReportRejectsInvalidPort(t *testing.T) {
	for _, port := range []int{0, 70000} {
		t.Run("port", func(t *testing.T) {
			store := &fakeStore{}
			result := validReportResult()
			result.Port = port

			status, body := postReport(t, store, reportJSON(t, result))

			if status != http.StatusBadRequest {
				t.Fatalf("status = %d, want 400: %s", status, body)
			}
			assertNoReportWrites(t, store)
		})
	}
}

func TestHandleReportRejectsNegativeCounts(t *testing.T) {
	tests := []struct {
		name string
		edit func(*model.Result)
	}{
		{name: "success_count", edit: func(result *model.Result) { result.SuccessCount = -1 }},
		{name: "failure_count", edit: func(result *model.Result) { result.FailureCount = -1 }},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			store := &fakeStore{}
			result := validReportResult()
			tt.edit(&result)

			status, body := postReport(t, store, reportJSON(t, result))

			if status != http.StatusBadRequest {
				t.Fatalf("status = %d, want 400: %s", status, body)
			}
			assertNoReportWrites(t, store)
		})
	}
}

func TestHandleReportRejectsZeroSamples(t *testing.T) {
	store := &fakeStore{}
	result := validReportResult()
	result.SuccessCount = 0
	result.FailureCount = 0

	status, body := postReport(t, store, reportJSON(t, result))

	if status != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400: %s", status, body)
	}
	assertNoReportWrites(t, store)
}

func TestHandleReportRejectsInvalidSuccessRate(t *testing.T) {
	for _, successRate := range []float64{-0.1, 1.1} {
		t.Run("success_rate", func(t *testing.T) {
			store := &fakeStore{}
			result := validReportResult()
			result.SuccessRate = successRate

			status, body := postReport(t, store, reportJSON(t, result))

			if status != http.StatusBadRequest {
				t.Fatalf("status = %d, want 400: %s", status, body)
			}
			assertNoReportWrites(t, store)
		})
	}
}

func TestHandleReportRejectsBatchWithOneInvalidResultWithoutPartialWrite(t *testing.T) {
	store := &fakeStore{}
	valid := validReportResult()
	body := `[` + reportJSON(t, valid) + `,{}]`

	status, response := postReport(t, store, body)

	if status != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400: %s", status, response)
	}
	assertNoReportWrites(t, store)
}

func TestValidateReportResult(t *testing.T) {
	tests := []struct {
		name string
		edit func(*model.Result)
	}{
		{name: "valid", edit: func(*model.Result) {}},
		{name: "agent required", edit: func(result *model.Result) { result.Agent = " \t" }},
		{name: "target_name required", edit: func(result *model.Result) { result.TargetName = "" }},
		{name: "address required", edit: func(result *model.Result) { result.Address = "" }},
		{name: "invalid port low", edit: func(result *model.Result) { result.Port = 0 }},
		{name: "invalid port high", edit: func(result *model.Result) { result.Port = 65536 }},
		{name: "negative success count", edit: func(result *model.Result) { result.SuccessCount = -1 }},
		{name: "negative failure count", edit: func(result *model.Result) { result.FailureCount = -1 }},
		{name: "zero samples", edit: func(result *model.Result) { result.SuccessCount = 0 }},
		{name: "negative latency", edit: func(result *model.Result) { result.AverageLatencyMS = -0.01 }},
		{name: "success rate low", edit: func(result *model.Result) { result.SuccessRate = -0.01 }},
		{name: "success rate high", edit: func(result *model.Result) { result.SuccessRate = 1.01 }},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := validReportResult()
			tt.edit(&result)

			err := validateReportResult(result)

			if tt.name == "valid" && err != nil {
				t.Fatalf("validateReportResult() error = %v, want nil", err)
			}
			if tt.name != "valid" && err == nil {
				t.Fatalf("validateReportResult() error = nil, want error")
			}
		})
	}
}

func validReportResult() model.Result {
	return model.Result{
		Agent:            "agent-1",
		AgentIP:          "203.0.113.10",
		TargetName:       "web",
		Address:          "198.51.100.10",
		Port:             443,
		CheckedAt:        time.Date(2026, 6, 20, 10, 0, 0, 0, time.UTC),
		SuccessCount:     3,
		FailureCount:     0,
		AverageLatencyMS: 4.2,
		SuccessRate:      1,
	}
}

func reportJSON(t *testing.T, result model.Result) string {
	t.Helper()
	body, err := json.Marshal(result)
	if err != nil {
		t.Fatalf("marshal report: %v", err)
	}
	return string(body)
}

func postReport(t *testing.T, store *fakeStore, body string) (int, string) {
	t.Helper()
	s := &server{cfg: config.DefaultConfig(), store: store, hub: newWebsocketHub()}
	req := httptest.NewRequest(http.MethodPost, "/api/report", strings.NewReader(body))
	rr := httptest.NewRecorder()

	s.handleReport(rr, req)

	return rr.Code, rr.Body.String()
}

func assertNoReportWrites(t *testing.T, store *fakeStore) {
	t.Helper()
	if len(store.results) != 0 {
		t.Fatalf("results = %d, want 0", len(store.results))
	}
	if len(store.heartbeats) != 0 {
		t.Fatalf("heartbeats = %d, want 0", len(store.heartbeats))
	}
}
