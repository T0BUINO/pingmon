package storage

import (
	"database/sql"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"pingmon/internal/model"
)

func TestSQLiteRollupAndCompactedResults(t *testing.T) {
	store, err := NewSQLiteStore(filepath.Join(t.TempDir(), "pingmon.db"))
	if err != nil {
		t.Fatalf("NewSQLiteStore: %v", err)
	}
	defer store.db.Close()

	oldBucket := time.Date(2026, 6, 1, 10, 0, 0, 0, time.UTC)
	newTime := time.Date(2026, 6, 19, 10, 0, 0, 0, time.UTC)
	for _, result := range []model.Result{
		{
			Agent:            "agent-1",
			AgentIP:          "203.0.113.1",
			TargetName:       "web",
			Address:          "198.51.100.10",
			Port:             443,
			Labels:           []string{"prod"},
			CheckedAt:        oldBucket.Add(5 * time.Minute),
			SuccessCount:     3,
			AverageLatencyMS: 10,
			SuccessRate:      1,
		},
		{
			Agent:            "agent-1",
			AgentIP:          "203.0.113.1",
			TargetName:       "web",
			Address:          "198.51.100.10",
			Port:             443,
			Labels:           []string{"prod"},
			CheckedAt:        oldBucket.Add(15 * time.Minute),
			SuccessCount:     1,
			FailureCount:     2,
			AverageLatencyMS: 30,
			SuccessRate:      1.0 / 3.0,
			Error:            "timeout",
		},
		{
			Agent:            "agent-1",
			AgentIP:          "203.0.113.1",
			TargetName:       "web",
			Address:          "198.51.100.10",
			Port:             443,
			Labels:           []string{"prod"},
			CheckedAt:        newTime,
			SuccessCount:     3,
			AverageLatencyMS: 7,
			SuccessRate:      1,
		},
	} {
		if err := store.SaveResult(result); err != nil {
			t.Fatalf("SaveResult: %v", err)
		}
	}

	rawCutoff := time.Date(2026, 6, 10, 0, 0, 0, 0, time.UTC)
	rolled, err := store.RollupBefore(rawCutoff, time.Hour)
	if err != nil {
		t.Fatalf("RollupBefore: %v", err)
	}
	if rolled != 1 {
		t.Fatalf("rolled = %d, want 1", rolled)
	}
	deleted, err := store.DeleteBefore(rawCutoff)
	if err != nil {
		t.Fatalf("DeleteBefore: %v", err)
	}
	if deleted != 2 {
		t.Fatalf("deleted = %d, want 2", deleted)
	}

	results, err := store.ResultsSinceCompacted(time.Date(2026, 6, 1, 0, 0, 0, 0, time.UTC), rawCutoff)
	if err != nil {
		t.Fatalf("ResultsSinceCompacted: %v", err)
	}
	if len(results) != 2 {
		t.Fatalf("len(results) = %d, want 2", len(results))
	}

	rollup := results[1]
	if !rollup.CheckedAt.Equal(oldBucket) {
		t.Fatalf("rollup bucket = %s, want %s", rollup.CheckedAt, oldBucket)
	}
	if rollup.SuccessCount != 4 || rollup.FailureCount != 2 {
		t.Fatalf("rollup counts = %d/%d, want 4/2", rollup.SuccessCount, rollup.FailureCount)
	}
	if rollup.AverageLatencyMS != 15 {
		t.Fatalf("rollup latency = %v, want 15", rollup.AverageLatencyMS)
	}
	if rollup.SuccessRate != 4.0/6.0 {
		t.Fatalf("rollup success rate = %v, want %v", rollup.SuccessRate, 4.0/6.0)
	}
	if rollup.Error == "" {
		t.Fatal("rollup error marker is empty")
	}
}

func TestSQLiteCompactedResultsForAgent(t *testing.T) {
	store, err := NewSQLiteStore(filepath.Join(t.TempDir(), "pingmon.db"))
	if err != nil {
		t.Fatalf("NewSQLiteStore: %v", err)
	}
	defer store.db.Close()

	oldBucket := time.Date(2026, 6, 1, 10, 0, 0, 0, time.UTC)
	newTime := time.Date(2026, 6, 19, 10, 0, 0, 0, time.UTC)
	for _, result := range []model.Result{
		{
			Agent:            "agent-1",
			AgentIP:          "203.0.113.1",
			TargetName:       "web",
			Address:          "198.51.100.10",
			Port:             443,
			CheckedAt:        oldBucket.Add(5 * time.Minute),
			SuccessCount:     3,
			AverageLatencyMS: 10,
			SuccessRate:      1,
		},
		{
			Agent:            "agent-1",
			AgentIP:          "203.0.113.1",
			TargetName:       "web",
			Address:          "198.51.100.10",
			Port:             443,
			CheckedAt:        newTime,
			SuccessCount:     3,
			AverageLatencyMS: 7,
			SuccessRate:      1,
		},
		{
			Agent:            "agent-2",
			AgentIP:          "203.0.113.2",
			TargetName:       "web",
			Address:          "198.51.100.10",
			Port:             443,
			CheckedAt:        oldBucket.Add(10 * time.Minute),
			SuccessCount:     1,
			AverageLatencyMS: 20,
			SuccessRate:      1,
		},
		{
			Agent:            "agent-2",
			AgentIP:          "203.0.113.2",
			TargetName:       "web",
			Address:          "198.51.100.10",
			Port:             443,
			CheckedAt:        newTime.Add(time.Minute),
			SuccessCount:     1,
			AverageLatencyMS: 9,
			SuccessRate:      1,
		},
	} {
		if err := store.SaveResult(result); err != nil {
			t.Fatalf("SaveResult: %v", err)
		}
	}

	rawCutoff := time.Date(2026, 6, 10, 0, 0, 0, 0, time.UTC)
	if _, err := store.RollupBefore(rawCutoff, time.Hour); err != nil {
		t.Fatalf("RollupBefore: %v", err)
	}
	if _, err := store.DeleteBefore(rawCutoff); err != nil {
		t.Fatalf("DeleteBefore: %v", err)
	}

	results, err := store.ResultsSinceCompactedForAgent(time.Date(2026, 6, 1, 0, 0, 0, 0, time.UTC), rawCutoff, "agent-1")
	if err != nil {
		t.Fatalf("ResultsSinceCompactedForAgent: %v", err)
	}
	if len(results) != 2 {
		t.Fatalf("len(results) = %d, want 2", len(results))
	}
	for _, result := range results {
		if result.Agent != "agent-1" {
			t.Fatalf("result agent = %q, want agent-1", result.Agent)
		}
	}
	if !results[0].CheckedAt.Equal(newTime) || !results[1].CheckedAt.Equal(oldBucket) {
		t.Fatalf("result times = %s, %s", results[0].CheckedAt, results[1].CheckedAt)
	}
}

func TestSQLiteAgentHeartbeat(t *testing.T) {
	store, err := NewSQLiteStore(filepath.Join(t.TempDir(), "pingmon.db"))
	if err != nil {
		t.Fatalf("NewSQLiteStore: %v", err)
	}
	defer store.db.Close()

	firstSeen := time.Date(2026, 6, 20, 10, 0, 0, 0, time.UTC)
	lastSeen := firstSeen.Add(2 * time.Minute)
	if err := store.SaveAgentHeartbeat("agent-1", "203.0.113.1", firstSeen); err != nil {
		t.Fatalf("SaveAgentHeartbeat first: %v", err)
	}
	if err := store.SaveAgentHeartbeat("agent-1", "203.0.113.2", lastSeen); err != nil {
		t.Fatalf("SaveAgentHeartbeat second: %v", err)
	}
	if err := store.SaveAgentHeartbeat("agent-2", "2001:db8::2", firstSeen.Add(time.Minute)); err != nil {
		t.Fatalf("SaveAgentHeartbeat agent-2: %v", err)
	}

	statuses, err := store.ListAgentStatuses()
	if err != nil {
		t.Fatalf("ListAgentStatuses: %v", err)
	}
	if len(statuses) != 2 {
		t.Fatalf("len(statuses) = %d, want 2", len(statuses))
	}
	if statuses[0].Agent != "agent-1" || statuses[0].AgentIP != "203.0.113.2" {
		t.Fatalf("latest status = %+v, want updated agent-1", statuses[0])
	}
	if !statuses[0].FirstSeenAt.Equal(firstSeen) {
		t.Fatalf("first_seen_at = %s, want %s", statuses[0].FirstSeenAt, firstSeen)
	}
	if !statuses[0].LastSeenAt.Equal(lastSeen) {
		t.Fatalf("last_seen_at = %s, want %s", statuses[0].LastSeenAt, lastSeen)
	}
}

func TestSQLiteDeleteAgentRemovesStatusesAndResults(t *testing.T) {
	store, err := NewSQLiteStore(filepath.Join(t.TempDir(), "pingmon.db"))
	if err != nil {
		t.Fatalf("NewSQLiteStore: %v", err)
	}
	defer store.db.Close()

	seenAt := time.Date(2026, 6, 20, 10, 0, 0, 0, time.UTC)
	for _, agent := range []string{"agent-1", "agent-2"} {
		if err := store.SaveAgentHeartbeat(agent, "203.0.113.1", seenAt); err != nil {
			t.Fatalf("SaveAgentHeartbeat %s: %v", agent, err)
		}
		if err := store.SaveResult(model.Result{
			Agent:            agent,
			AgentIP:          "203.0.113.1",
			TargetName:       "web",
			Address:          "198.51.100.10",
			Port:             443,
			CheckedAt:        seenAt,
			SuccessCount:     3,
			AverageLatencyMS: 5,
			SuccessRate:      1,
		}); err != nil {
			t.Fatalf("SaveResult %s: %v", agent, err)
		}
	}

	if err := store.DeleteAgent("agent-1"); err != nil {
		t.Fatalf("DeleteAgent: %v", err)
	}
	statuses, err := store.ListAgentStatuses()
	if err != nil {
		t.Fatalf("ListAgentStatuses: %v", err)
	}
	if len(statuses) != 1 || statuses[0].Agent != "agent-2" {
		t.Fatalf("statuses = %+v, want only agent-2", statuses)
	}
	results, err := store.ResultsSince(seenAt.Add(-time.Minute))
	if err != nil {
		t.Fatalf("ResultsSince: %v", err)
	}
	if len(results) != 1 || results[0].Agent != "agent-2" {
		t.Fatalf("results = %+v, want only agent-2", results)
	}
}

func TestSQLiteMigratesOldSchemaToSeries(t *testing.T) {
	path := filepath.Join(t.TempDir(), "pingmon.db")
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatalf("open old-schema db: %v", err)
	}
	_, err = db.Exec(`
CREATE TABLE results (
	id INTEGER PRIMARY KEY AUTOINCREMENT,
	agent TEXT NOT NULL,
	agent_ip TEXT,
	target_name TEXT NOT NULL,
	address TEXT NOT NULL,
	port INTEGER NOT NULL,
	labels TEXT,
	checked_at TEXT NOT NULL,
	success_count INTEGER NOT NULL,
	failure_count INTEGER NOT NULL,
	average_latency_ms REAL NOT NULL,
	success_rate REAL NOT NULL,
	error TEXT
);
CREATE TABLE result_rollups (
	id INTEGER PRIMARY KEY AUTOINCREMENT,
	agent TEXT NOT NULL,
	agent_ip TEXT NOT NULL,
	target_name TEXT NOT NULL,
	address TEXT NOT NULL,
	port INTEGER NOT NULL,
	labels TEXT NOT NULL,
	bucket_start TEXT NOT NULL,
	interval_seconds INTEGER NOT NULL,
	sample_count INTEGER NOT NULL,
	success_count INTEGER NOT NULL,
	failure_count INTEGER NOT NULL,
	average_latency_ms REAL NOT NULL,
	success_rate REAL NOT NULL,
	error TEXT
);
INSERT INTO results (
	agent, agent_ip, target_name, address, port, labels, checked_at, success_count,
	failure_count, average_latency_ms, success_rate, error
) VALUES
	('agent-1', '203.0.113.1', 'web', '198.51.100.10', 443, '["prod"]', '2026-06-19T10:00:00Z', 3, 0, 7, 1, ''),
	('agent-1', '203.0.113.1', 'web', '198.51.100.10', 443, '["prod"]', '2026-06-19T10:01:00Z', 0, 3, 0, 0, 'timeout');
INSERT INTO result_rollups (
	agent, agent_ip, target_name, address, port, labels, bucket_start, interval_seconds,
	sample_count, success_count, failure_count, average_latency_ms, success_rate, error
) VALUES
	('agent-1', '203.0.113.1', 'web', '198.51.100.10', 443, '["prod"]', '2026-06-18T10:00:00Z', 3600, 2, 4, 2, 15, 0.6666666667, 'rollup contains problem samples');
`)
	if closeErr := db.Close(); closeErr != nil {
		t.Fatalf("close old-schema db: %v", closeErr)
	}
	if err != nil {
		t.Fatalf("seed old-schema db: %v", err)
	}

	migrated, err := MigrateSQLite(path)
	if err != nil {
		t.Fatalf("MigrateSQLite: %v", err)
	}
	if !migrated {
		t.Fatal("MigrateSQLite migrated = false, want true")
	}
	migrated, err = MigrateSQLite(path)
	if err != nil {
		t.Fatalf("second MigrateSQLite: %v", err)
	}
	if migrated {
		t.Fatal("second MigrateSQLite migrated = true, want false")
	}

	store, err := NewSQLiteStore(path)
	if err != nil {
		t.Fatalf("NewSQLiteStore: %v", err)
	}
	defer store.db.Close()

	results, err := store.ResultsSince(time.Date(2026, 6, 19, 0, 0, 0, 0, time.UTC))
	if err != nil {
		t.Fatalf("ResultsSince: %v", err)
	}
	if len(results) != 2 {
		t.Fatalf("len(results) = %d, want 2", len(results))
	}
	if results[0].Agent != "agent-1" || results[0].AgentIP != "203.0.113.1" || results[0].Labels[0] != "prod" {
		t.Fatalf("migrated result metadata = %+v", results[0])
	}

	var seriesCount int
	if err := store.db.QueryRow(`SELECT COUNT(*) FROM result_series`).Scan(&seriesCount); err != nil {
		t.Fatalf("count series: %v", err)
	}
	if seriesCount != 1 {
		t.Fatalf("series count = %d, want 1", seriesCount)
	}

	var schema string
	if err := store.db.QueryRow(`SELECT sql FROM sqlite_master WHERE name = 'results'`).Scan(&schema); err != nil {
		t.Fatalf("read schema: %v", err)
	}
	if strings.Contains(strings.ToUpper(schema), "AUTOINCREMENT") {
		t.Fatalf("results schema still uses AUTOINCREMENT: %s", schema)
	}
}
