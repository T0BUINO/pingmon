package main

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
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
	streamedResults   []model.Result
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
	for _, result := range s.streamedResults {
		if agent != "" && result.Agent != agent {
			continue
		}
		if result.CheckedAt.Before(since) {
			continue
		}
		if err := fn(result); err != nil {
			return err
		}
	}
	return nil
}

func (s *fakeStore) StreamResultsSinceCompacted(since, rawCutoff time.Time, agent string, fn func(model.Result) error) error {
	return s.StreamResultsSince(since, agent, fn)
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

func TestHandleDashboardResultsStreamsFromDB(t *testing.T) {
	store := &fakeStore{
		streamedResults: []model.Result{
			{
				Agent:            "agent-1",
				AgentIP:          "203.0.113.1",
				TargetName:       "web",
				Address:          "198.51.100.10",
				Port:             443,
				Labels:           []string{"prod"},
				CheckedAt:        time.Date(2026, 6, 20, 0, 0, 0, 0, time.UTC),
				SuccessCount:     1,
				AverageLatencyMS: 10,
				SuccessRate:      1,
			},
		},
	}
	s := &server{cfg: config.DefaultConfig(), store: store}
	req := httptest.NewRequest(http.MethodGet, "/api/results?dashboard=1&range=7d&agent=agent-1", nil)
	rr := httptest.NewRecorder()

	s.handleResults(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200: %s", rr.Code, rr.Body.String())
	}
	if store.resultsSinceCalls != 1 {
		t.Fatalf("resultsSinceCalls = %d, want 1", store.resultsSinceCalls)
	}
	got := decodeDashboardResultsForTest(t, rr.Body.Bytes())
	if len(got) != 1 || got[0].Agent != "agent-1" {
		t.Fatalf("streamed response = %+v", got)
	}
}

func TestHandleDashboardResultsLandingStreamsAllAgents(t *testing.T) {
	store := &fakeStore{
		streamedResults: []model.Result{
			{
				Agent:            "agent-1",
				AgentIP:          "203.0.113.1",
				TargetName:       "web",
				Address:          "198.51.100.10",
				Port:             443,
				CheckedAt:        time.Now().UTC(),
				SuccessCount:     1,
				AverageLatencyMS: 10,
				SuccessRate:      1,
			},
		},
	}
	s := &server{cfg: config.DefaultConfig(), store: store}
	req := httptest.NewRequest(http.MethodGet, "/api/results?dashboard=1&range=24h", nil)
	rr := httptest.NewRecorder()

	s.handleResults(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200: %s", rr.Code, rr.Body.String())
	}
	if store.resultsSinceCalls != 1 {
		t.Fatalf("resultsSinceCalls = %d, want 1", store.resultsSinceCalls)
	}
	got := decodeDashboardResultsForTest(t, rr.Body.Bytes())
	if len(got) != 1 || got[0].Agent != "agent-1" {
		t.Fatalf("landing streamed response = %+v", got)
	}
}

func TestDashboardMemCacheServesFromCacheAfterFirstRequest(t *testing.T) {
	now := time.Now().UTC()
	results := []model.Result{
		{Agent: "agent-1", AgentIP: "203.0.113.1", TargetName: "web", Address: "198.51.100.10", Port: 443, CheckedAt: now, SuccessCount: 1, AverageLatencyMS: 10, SuccessRate: 1},
		{Agent: "agent-1", AgentIP: "203.0.113.1", TargetName: "web", Address: "198.51.100.10", Port: 443, CheckedAt: now.Add(-30 * time.Minute), SuccessCount: 1, FailureCount: 1, AverageLatencyMS: 11, SuccessRate: 0.5, Error: "timeout"},
		{Agent: "agent-1", AgentIP: "203.0.113.1", TargetName: "db", Address: "198.51.100.20", Port: 5432, CheckedAt: now.Add(-2 * time.Hour), SuccessCount: 1, AverageLatencyMS: 5, SuccessRate: 1},
	}
	store := &fakeStore{streamedResults: results}
	s := &server{cfg: config.DefaultConfig(), store: store, dashMem: newDashMemCache()}

	req := httptest.NewRequest(http.MethodGet, "/api/results?dashboard=1&range=24h&agent=agent-1", nil)
	rr := httptest.NewRecorder()
	s.handleResults(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("first request status = %d, want 200", rr.Code)
	}
	firstGot := decodeDashboardResultsForTest(t, rr.Body.Bytes())
	if len(firstGot) != 3 {
		t.Fatalf("first request returned %d rows, want 3", len(firstGot))
	}
	firstCalls := store.resultsSinceCalls

	req2 := httptest.NewRequest(http.MethodGet, "/api/results?dashboard=1&range=1h&agent=agent-1", nil)
	rr2 := httptest.NewRecorder()
	s.handleResults(rr2, req2)
	if rr2.Code != http.StatusOK {
		t.Fatalf("second request status = %d, want 200", rr2.Code)
	}
	secondGot := decodeDashboardResultsForTest(t, rr2.Body.Bytes())
	if len(secondGot) != 2 {
		t.Fatalf("second request (1h) returned %d rows, want 2", len(secondGot))
	}
	if store.resultsSinceCalls != firstCalls {
		t.Fatalf("second request hit DB (%d calls, want %d)", store.resultsSinceCalls, firstCalls)
	}
}

func TestDashboardMemCacheLongRangeBypassesCache(t *testing.T) {
	now := time.Now().UTC()
	store := &fakeStore{
		streamedResults: []model.Result{
			{Agent: "agent-1", AgentIP: "203.0.113.1", TargetName: "web", Address: "198.51.100.10", Port: 443, CheckedAt: now, SuccessCount: 1, AverageLatencyMS: 10, SuccessRate: 1},
		},
	}
	s := &server{cfg: config.DefaultConfig(), store: store, dashMem: newDashMemCache()}

	req := httptest.NewRequest(http.MethodGet, "/api/results?dashboard=1&range=365d&agent=agent-1", nil)
	rr := httptest.NewRecorder()
	s.handleResults(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rr.Code)
	}
	if store.resultsSinceCalls != 1 {
		t.Fatalf("resultsSinceCalls = %d, want 1", store.resultsSinceCalls)
	}
}

func TestDashboardMemCacheInvalidatesOnNewData(t *testing.T) {
	now := time.Now().UTC()
	store := &fakeStore{
		streamedResults: []model.Result{
			{Agent: "agent-1", AgentIP: "203.0.113.1", TargetName: "web", Address: "198.51.100.10", Port: 443, CheckedAt: now, SuccessCount: 1, AverageLatencyMS: 10, SuccessRate: 1},
		},
	}
	s := &server{cfg: config.DefaultConfig(), store: store, dashMem: newDashMemCache(), hub: newWebsocketHub()}

	req := httptest.NewRequest(http.MethodGet, "/api/results?dashboard=1&range=24h&agent=agent-1", nil)
	rr := httptest.NewRecorder()
	s.handleResults(rr, req)
	callsAfterFirst := store.resultsSinceCalls

	body := `{"agent":"agent-1","agent_ip":"203.0.113.1","target_name":"web","address":"198.51.100.10","port":443,"checked_at":"2026-06-20T10:00:00Z","success_count":1,"failure_count":0,"average_latency_ms":1,"success_rate":1}`
	reportReq := httptest.NewRequest(http.MethodPost, "/api/report", strings.NewReader(body))
	reportRR := httptest.NewRecorder()
	s.handleReport(reportRR, reportReq)

	req2 := httptest.NewRequest(http.MethodGet, "/api/results?dashboard=1&range=24h&agent=agent-1", nil)
	rr2 := httptest.NewRecorder()
	s.handleResults(rr2, req2)
	if store.resultsSinceCalls <= callsAfterFirst {
		t.Fatalf("cache should be invalidated after new data (calls: %d, was: %d)", store.resultsSinceCalls, callsAfterFirst)
	}
}

func TestAggregateRowsByTime(t *testing.T) {
	now := time.Now().UnixNano()
	rows := make([]agentRow, 5000)
	for i := 0; i < 5000; i++ {
		rows[i] = agentRow{checkedAt: now - int64(i)*int64(time.Second)}
	}
	since := time.Unix(0, now-int64(5000)*int64(time.Second))
	result := aggregateRowsByTime(rows, since, 1000)
	if len(result) > 1000 {
		t.Fatalf("aggregated to %d rows, want <= 1000", len(result))
	}
	if len(result) < 900 {
		t.Fatalf("aggregated to only %d rows, want close to 1000", len(result))
	}
}

func TestAggregateRowsByTimeNoChangeIfSmall(t *testing.T) {
	now := time.Now().UnixNano()
	rows := make([]agentRow, 10)
	for i := 0; i < 10; i++ {
		rows[i] = agentRow{checkedAt: now - int64(i)*int64(time.Second)}
	}
	since := time.Unix(0, now-int64(10)*int64(time.Second))
	result := aggregateRowsByTime(rows, since, 1000)
	if len(result) != 10 {
		t.Fatalf("small set changed from %d to %d, want 10", len(rows), len(result))
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
