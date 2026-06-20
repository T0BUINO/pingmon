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

func TestSQLiteMigratesLegacySchemaToSeries(t *testing.T) {
	path := filepath.Join(t.TempDir(), "pingmon.db")
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatalf("open legacy db: %v", err)
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
		t.Fatalf("close legacy db: %v", closeErr)
	}
	if err != nil {
		t.Fatalf("seed legacy db: %v", err)
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
