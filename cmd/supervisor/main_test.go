package main

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
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

func (s *fakeStore) SaveResults(results []model.Result, seenAt time.Time) ([]model.Result, error) {
	seen := make(map[string]bool, len(results))
	for _, result := range results {
		s.results = append(s.results, result)
		if !seen[result.Agent] {
			seen[result.Agent] = true
			s.heartbeats = append(s.heartbeats, heartbeatCall{agent: result.Agent, agentIP: result.AgentIP, seenAt: seenAt})
		}
	}
	return results, nil
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

func (s *fakeStore) ConsecutiveFailures(targetName, address string, port int, limit int) (int, error) {
	return 0, nil
}

func TestDashboardResultCacheServesExactBaseAndDeltaPoints(t *testing.T) {
	cache := &dashboardResultCache{dir: t.TempDir()}
	since := time.Date(2026, 6, 20, 0, 0, 0, 0, time.UTC)
	key := dashboardCacheKey{Agent: "agent-1"}
	base := []model.Result{
		{
			Agent:            "agent-1",
			AgentIP:          "203.0.113.1",
			TargetName:       "web",
			Address:          "198.51.100.10",
			Port:             443,
			Labels:           []string{"prod", "edge"},
			CheckedAt:        since.Add(time.Hour + 123456789*time.Nanosecond),
			SuccessCount:     1,
			AverageLatencyMS: 10,
			SuccessRate:      1,
		},
		{
			Agent:            "agent-1",
			AgentIP:          "203.0.113.1",
			TargetName:       "web",
			Address:          "198.51.100.10",
			Port:             443,
			Labels:           []string{"prod", "edge"},
			CheckedAt:        since.Add(2 * time.Hour),
			SuccessCount:     1,
			FailureCount:     1,
			AverageLatencyMS: 11,
			SuccessRate:      0.5,
			Error:            "timeout",
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
	if _, err := cache.writeIfReady(rr, key, since); err != nil {
		t.Fatalf("cache write: %v", err)
	}
	got := decodeDashboardResultsForTest(t, rr.Body.Bytes())
	if len(got) != len(base) {
		t.Fatalf("base cache returned %d points, want %d", len(got), len(base))
	}
	for i := range base {
		assertResultEqual(t, got[i], base[i])
	}

	time.Sleep(time.Millisecond)
	delta := model.Result{
		Agent:            "agent-1",
		AgentIP:          "203.0.113.1",
		TargetName:       "web",
		Address:          "198.51.100.10",
		Port:             443,
		Labels:           []string{"prod", "edge"},
		CheckedAt:        since.Add(3 * time.Hour),
		SuccessCount:     1,
		AverageLatencyMS: 12,
		SuccessRate:      1,
	}
	cache.appendDelta([]model.Result{delta})
	rr = httptest.NewRecorder()
	if _, err := cache.writeIfReady(rr, key, since); err != nil {
		t.Fatalf("cache write with delta: %v", err)
	}
	got = decodeDashboardResultsForTest(t, rr.Body.Bytes())
	if len(got) != len(base)+1 {
		t.Fatalf("delta cache returned %d points, want %d", len(got), len(base)+1)
	}
	assertResultEqual(t, got[0], delta)
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
	if _, err := cache.writeIfReady(rr, key, since); err != nil {
		t.Fatalf("cache write after compact: %v", err)
	}
	got = decodeDashboardResultsForTest(t, rr.Body.Bytes())
	if len(got) != len(rebuilt) {
		t.Fatalf("compacted cache returned %d points, want %d", len(got), len(rebuilt))
	}
	for i := range rebuilt {
		assertResultEqual(t, got[i], rebuilt[i])
	}
}

func TestDashboardResultCachePersistsAcrossInstances(t *testing.T) {
	dir := t.TempDir()
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
	key := dashboardCacheKey{Agent: result.Agent}
	cacheSince := result.CheckedAt.Add(-time.Hour)
	first := &dashboardResultCache{dir: dir}
	if err := first.refresh(key, cacheSince, func(fn func(model.Result) error) error {
		return fn(result)
	}); err != nil {
		t.Fatalf("cache refresh: %v", err)
	}

	second := &dashboardResultCache{dir: dir}
	rr := httptest.NewRecorder()
	if _, err := second.writeIfReady(rr, key, cacheSince); err != nil {
		t.Fatalf("cache write after restart: %v", err)
	}
	got := decodeDashboardResultsForTest(t, rr.Body.Bytes())
	if len(got) != 1 || !got[0].CheckedAt.Equal(result.CheckedAt) {
		t.Fatalf("persisted cache = %+v, want one original result", got)
	}
}

func TestDashboardResultCacheLoadsPersistedDeltaRows(t *testing.T) {
	dir := t.TempDir()
	key := dashboardCacheKey{Agent: "agent-1"}
	base := model.Result{
		Agent:            "agent-1",
		TargetName:       "web",
		Address:          "198.51.100.10",
		Port:             443,
		CheckedAt:        time.Date(2026, 6, 20, 0, 0, 0, 0, time.UTC),
		SuccessCount:     1,
		AverageLatencyMS: 10,
		SuccessRate:      1,
	}
	cacheSince := base.CheckedAt.Add(-time.Hour)
	delta := model.Result{
		Agent:            "agent-1",
		AgentIP:          "203.0.113.1",
		TargetName:       "web",
		Address:          "198.51.100.10",
		Port:             443,
		Labels:           []string{"prod"},
		CheckedAt:        base.CheckedAt.Add(time.Hour + 987654321*time.Nanosecond),
		SuccessCount:     1,
		FailureCount:     1,
		AverageLatencyMS: 12,
		SuccessRate:      0.5,
		Error:            "timeout",
	}
	first := &dashboardResultCache{dir: dir}
	if err := first.refresh(key, cacheSince, func(fn func(model.Result) error) error {
		return fn(base)
	}); err != nil {
		t.Fatalf("cache refresh: %v", err)
	}
	time.Sleep(time.Millisecond)
	first.appendDelta([]model.Result{delta})

	second := &dashboardResultCache{dir: dir}
	rr := httptest.NewRecorder()
	if _, err := second.writeIfReady(rr, key, cacheSince); err != nil {
		t.Fatalf("cache write after restart: %v", err)
	}
	got := decodeDashboardResultsForTest(t, rr.Body.Bytes())
	if len(got) != 2 {
		t.Fatalf("persisted delta response returned %d points, want 2", len(got))
	}
	assertResultEqual(t, got[0], delta)
	assertResultEqual(t, got[1], base)
}

func TestDashboardResultCacheServesStaleBaseForBackgroundRefresh(t *testing.T) {
	cache := &dashboardResultCache{dir: t.TempDir()}
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
	key := dashboardCacheKey{Agent: result.Agent}
	cacheSince := result.CheckedAt.Add(-time.Hour)
	if err := cache.refresh(key, cacheSince, func(fn func(model.Result) error) error {
		return fn(result)
	}); err != nil {
		t.Fatalf("cache refresh: %v", err)
	}
	meta := dashboardCacheMeta{
		Key:     key,
		Since:   cacheSince,
		BuiltAt: time.Now().UTC().Add(-dashboardCacheRefreshAfter - time.Second),
		Version: dashboardCacheVersion,
	}
	metaData, err := json.Marshal(meta)
	if err != nil {
		t.Fatalf("marshal meta: %v", err)
	}
	if err := os.WriteFile(cache.metaPath(key), metaData, 0644); err != nil {
		t.Fatalf("write stale meta: %v", err)
	}

	stale, err := cache.writeIfReady(httptest.NewRecorder(), key, cacheSince)
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
		if key.Agent != "agent-1" {
			t.Fatalf("queued key = %+v", key)
		}
	default:
		t.Fatal("cache build was not queued")
	}
}

func TestDashboardResultCacheClearsIncompatibleVersion(t *testing.T) {
	cache := &dashboardResultCache{dir: t.TempDir()}
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
	key := dashboardCacheKey{Agent: result.Agent}
	cacheSince := result.CheckedAt.Add(-time.Hour)
	if err := cache.refresh(key, cacheSince, func(fn func(model.Result) error) error {
		return fn(result)
	}); err != nil {
		t.Fatalf("cache refresh: %v", err)
	}
	meta := dashboardCacheMeta{
		Key:     key,
		Since:   cacheSince,
		Until:   time.Now().UTC().Add(time.Hour),
		BuiltAt: time.Now().UTC(),
		Version: dashboardCacheVersion - 1,
	}
	metaData, err := json.Marshal(meta)
	if err != nil {
		t.Fatalf("marshal meta: %v", err)
	}
	if err := os.WriteFile(cache.metaPath(key), metaData, 0644); err != nil {
		t.Fatalf("write incompatible meta: %v", err)
	}

	cache.clearIncompatible()

	if _, err := os.Stat(cache.dir); !os.IsNotExist(err) {
		t.Fatalf("cache dir still exists after incompatible clear: %v", err)
	}
}

func TestDashboardResultCacheClearsPartialCacheWithoutMeta(t *testing.T) {
	cache := &dashboardResultCache{dir: t.TempDir()}
	key := dashboardCacheKey{}
	bucketDir := cache.bucketDir(key)
	if err := os.MkdirAll(bucketDir, 0755); err != nil {
		t.Fatalf("mkdir bucket dir: %v", err)
	}
	bucketPath := cache.bucketPath(key, time.Now())
	if err := os.MkdirAll(filepath.Dir(bucketPath), 0755); err != nil {
		t.Fatalf("mkdir bucket parent: %v", err)
	}
	if err := os.WriteFile(bucketPath, []byte("[]"), 0644); err != nil {
		t.Fatalf("write partial cache: %v", err)
	}

	cache.clearIncompatible()

	if _, err := os.Stat(cache.dir); !os.IsNotExist(err) {
		t.Fatalf("cache dir still exists after partial clear: %v", err)
	}
}

func TestHandleDashboardResultsServesStaleCacheAndQueuesRefresh(t *testing.T) {
	dir := t.TempDir()
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
	key := dashboardCacheKey{Agent: result.Agent, Range: "7d"}
	cacheSince := result.CheckedAt.Add(-time.Hour)
	cache := &dashboardResultCache{dir: dir}
	if err := cache.refresh(key, cacheSince, func(fn func(model.Result) error) error {
		return fn(result)
	}); err != nil {
		t.Fatalf("cache refresh: %v", err)
	}
	meta := dashboardCacheMeta{
		Key:     key,
		Since:   cacheSince,
		Until:   time.Now().UTC().Add(time.Hour),
		BuiltAt: time.Now().UTC().Add(-dashboardCacheRefreshAfter - time.Second),
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
	got := decodeDashboardResultsForTest(t, rr.Body.Bytes())
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

func decodeDashboardResultsForTest(t *testing.T, data []byte) []model.Result {
	t.Helper()
	var rows [][]json.RawMessage
	if err := json.Unmarshal(data, &rows); err != nil {
		t.Fatalf("decode dashboard cache response: %v", err)
	}
	results := make([]model.Result, 0, len(rows))
	for i, row := range rows {
		if len(row) != 12 {
			t.Fatalf("row %d has %d columns, want 12", i, len(row))
		}
		var result model.Result
		var checkedAt string
		unmarshalColumn := func(column int, dst any) {
			t.Helper()
			if err := json.Unmarshal(row[column], dst); err != nil {
				t.Fatalf("decode row %d column %d: %v", i, column, err)
			}
		}
		unmarshalColumn(0, &result.Agent)
		unmarshalColumn(1, &result.AgentIP)
		unmarshalColumn(2, &result.TargetName)
		unmarshalColumn(3, &result.Address)
		unmarshalColumn(4, &result.Port)
		unmarshalColumn(5, &result.Labels)
		unmarshalColumn(6, &checkedAt)
		parsedAt, err := parseDashboardTimeForTest(checkedAt)
		if err != nil {
			t.Fatalf("parse row %d checked_at: %v", i, err)
		}
		result.CheckedAt = parsedAt
		unmarshalColumn(7, &result.SuccessCount)
		unmarshalColumn(8, &result.FailureCount)
		unmarshalColumn(9, &result.AverageLatencyMS)
		unmarshalColumn(10, &result.SuccessRate)
		unmarshalColumn(11, &result.Error)
		results = append(results, result)
	}
	return results
}

func parseDashboardTimeForTest(value string) (time.Time, error) {
	if parsed, err := time.Parse(time.RFC3339Nano, value); err == nil {
		return parsed, nil
	}
	nanos, err := strconv.ParseInt(value, 36, 64)
	if err != nil {
		return time.Time{}, err
	}
	return time.Unix(0, nanos).UTC(), nil
}

func assertResultEqual(t *testing.T, got, want model.Result) {
	t.Helper()
	if got.Agent != want.Agent ||
		got.AgentIP != want.AgentIP ||
		got.TargetName != want.TargetName ||
		got.Address != want.Address ||
		got.Port != want.Port ||
		!equalLabels(got.Labels, want.Labels) ||
		!got.CheckedAt.Equal(want.CheckedAt) ||
		got.SuccessCount != want.SuccessCount ||
		got.FailureCount != want.FailureCount ||
		got.AverageLatencyMS != want.AverageLatencyMS ||
		got.SuccessRate != want.SuccessRate ||
		got.Error != want.Error {
		t.Fatalf("result = %+v, want %+v", got, want)
	}
}

func equalLabels(got, want []string) bool {
	if len(got) == 0 && len(want) == 0 {
		return true
	}
	return reflect.DeepEqual(got, want)
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
