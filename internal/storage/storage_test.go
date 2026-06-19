package storage

import (
	"path/filepath"
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
