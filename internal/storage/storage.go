package storage

import (
	"bufio"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"pingmon/internal/model"

	_ "modernc.org/sqlite"
)

type Store interface {
	SaveResult(model.Result) error
	RecentResults(limit int) ([]model.Result, error)
	ResultsSince(since time.Time) ([]model.Result, error)
	ResultsSinceCompacted(since, rawCutoff time.Time) ([]model.Result, error)
	RollupBefore(cutoff time.Time, interval time.Duration) (int, error)
	DeleteBefore(cutoff time.Time) (int, error)
	DeleteRollupsBefore(cutoff time.Time) (int, error)
	Vacuum() error
	ConsecutiveFailures(targetName, address string, port int) (int, error)
}

func New(kind, dataFile, sqlitePath string) (Store, error) {
	switch kind {
	case "", "sqlite":
		return NewSQLiteStore(sqlitePath)
	case "file":
		return NewFileStore(dataFile)
	default:
		return nil, errors.New("unknown storage backend: " + kind)
	}
}

type SQLiteStore struct {
	db *sql.DB
}

func NewSQLiteStore(path string) (*SQLiteStore, error) {
	if path == "" {
		path = "data/pingmon.db"
	}
	if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
		return nil, err
	}
	db, err := sql.Open("sqlite", path)
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(1)
	store := &SQLiteStore{db: db}
	if err := store.init(); err != nil {
		_ = db.Close()
		return nil, err
	}
	return store, nil
}

func (s *SQLiteStore) init() error {
	_, err := s.db.Exec(`
CREATE TABLE IF NOT EXISTS results (
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
CREATE INDEX IF NOT EXISTS idx_results_checked_at ON results(checked_at DESC);
CREATE INDEX IF NOT EXISTS idx_results_target ON results(target_name, address, port, checked_at DESC);

CREATE TABLE IF NOT EXISTS result_rollups (
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
CREATE UNIQUE INDEX IF NOT EXISTS idx_result_rollups_unique
ON result_rollups(agent, agent_ip, target_name, address, port, labels, bucket_start, interval_seconds);
CREATE INDEX IF NOT EXISTS idx_result_rollups_bucket ON result_rollups(bucket_start DESC);
`)
	if err != nil {
		return err
	}
	_, err = s.db.Exec(`ALTER TABLE results ADD COLUMN agent_ip TEXT`)
	if err != nil && !isDuplicateColumnError(err) {
		return err
	}
	return nil
}

func (s *SQLiteStore) SaveResult(result model.Result) error {
	labels, err := json.Marshal(result.Labels)
	if err != nil {
		return err
	}
	_, err = s.db.Exec(`
INSERT INTO results (
	agent, agent_ip, target_name, address, port, labels, checked_at, success_count,
	failure_count, average_latency_ms, success_rate, error
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		result.Agent, result.AgentIP, result.TargetName, result.Address, result.Port, string(labels),
		result.CheckedAt.UTC().Format(time.RFC3339Nano), result.SuccessCount,
		result.FailureCount, result.AverageLatencyMS, result.SuccessRate, result.Error)
	return err
}

func (s *SQLiteStore) RecentResults(limit int) ([]model.Result, error) {
	query := `SELECT agent, COALESCE(agent_ip, ''), target_name, address, port, labels, checked_at, success_count,
failure_count, average_latency_ms, success_rate, error FROM results ORDER BY checked_at DESC`
	args := []any{}
	if limit > 0 {
		query += " LIMIT ?"
		args = append(args, limit)
	}
	rows, err := s.db.Query(query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var results []model.Result
	for rows.Next() {
		result, err := scanResult(rows)
		if err != nil {
			return nil, err
		}
		results = append(results, result)
	}
	return results, rows.Err()
}

func (s *SQLiteStore) ResultsSince(since time.Time) ([]model.Result, error) {
	rows, err := s.db.Query(`SELECT agent, COALESCE(agent_ip, ''), target_name, address, port, labels, checked_at, success_count,
failure_count, average_latency_ms, success_rate, error FROM results
WHERE checked_at >= ?
ORDER BY checked_at DESC`, since.UTC().Format(time.RFC3339Nano))
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var results []model.Result
	for rows.Next() {
		result, err := scanResult(rows)
		if err != nil {
			return nil, err
		}
		results = append(results, result)
	}
	return results, rows.Err()
}

func (s *SQLiteStore) ResultsSinceCompacted(since, rawCutoff time.Time) ([]model.Result, error) {
	if since.After(rawCutoff) || since.Equal(rawCutoff) {
		return s.ResultsSince(since)
	}
	results := make([]model.Result, 0)
	rollups, err := s.rollupsSince(since, rawCutoff)
	if err != nil {
		return nil, err
	}
	results = append(results, rollups...)
	raw, err := s.ResultsSince(rawCutoff)
	if err != nil {
		return nil, err
	}
	results = append(results, raw...)
	sort.Slice(results, func(i, j int) bool {
		return results[i].CheckedAt.After(results[j].CheckedAt)
	})
	return results, nil
}

func (s *SQLiteStore) rollupsSince(since, before time.Time) ([]model.Result, error) {
	rows, err := s.db.Query(`SELECT agent, agent_ip, target_name, address, port, labels, bucket_start,
success_count, failure_count, average_latency_ms, success_rate, COALESCE(error, '') FROM result_rollups
WHERE bucket_start >= ? AND bucket_start < ?
ORDER BY bucket_start DESC`, since.UTC().Format(time.RFC3339Nano), before.UTC().Format(time.RFC3339Nano))
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanResults(rows)
}

func (s *SQLiteStore) RollupBefore(cutoff time.Time, interval time.Duration) (int, error) {
	if interval <= 0 {
		return 0, nil
	}
	cutoff = truncateUTC(cutoff, interval)
	if cutoff.IsZero() {
		return 0, nil
	}
	seconds := int(interval.Seconds())
	res, err := s.db.Exec(`
INSERT OR IGNORE INTO result_rollups (
	agent, agent_ip, target_name, address, port, labels, bucket_start, interval_seconds,
	sample_count, success_count, failure_count, average_latency_ms, success_rate, error
)
SELECT
	agent,
	agent_ip,
	target_name,
	address,
	port,
	labels,
	bucket_start,
	?,
	COUNT(*),
	SUM(success_count),
	SUM(failure_count),
	CASE WHEN SUM(success_count) > 0
		THEN SUM(average_latency_ms * success_count) / SUM(success_count)
		ELSE 0
	END,
	CASE WHEN SUM(success_count) + SUM(failure_count) > 0
		THEN CAST(SUM(success_count) AS REAL) / CAST(SUM(success_count) + SUM(failure_count) AS REAL)
		ELSE 0
	END,
	CASE WHEN SUM(CASE WHEN failure_count > 0 OR success_rate < 1 OR COALESCE(error, '') != '' THEN 1 ELSE 0 END) > 0
		THEN 'rollup contains problem samples'
		ELSE ''
	END
FROM (
	SELECT
		agent,
		COALESCE(agent_ip, '') AS agent_ip,
		target_name,
		address,
		port,
		COALESCE(labels, '') AS labels,
		strftime('%Y-%m-%dT%H:%M:%SZ', (unixepoch(checked_at) / ?) * ?, 'unixepoch') AS bucket_start,
		success_count,
		failure_count,
		average_latency_ms,
		success_rate,
		error
	FROM results
	WHERE checked_at < ?
)
GROUP BY agent, agent_ip, target_name, address, port, labels, bucket_start`,
		seconds, seconds, seconds, cutoff.UTC().Format(time.RFC3339Nano))
	if err != nil {
		return 0, err
	}
	n, err := res.RowsAffected()
	return int(n), err
}

func (s *SQLiteStore) DeleteBefore(cutoff time.Time) (int, error) {
	res, err := s.db.Exec("DELETE FROM results WHERE checked_at < ?", cutoff.UTC().Format(time.RFC3339Nano))
	if err != nil {
		return 0, err
	}
	n, err := res.RowsAffected()
	return int(n), err
}

func (s *SQLiteStore) DeleteRollupsBefore(cutoff time.Time) (int, error) {
	res, err := s.db.Exec("DELETE FROM result_rollups WHERE bucket_start < ?", cutoff.UTC().Format(time.RFC3339Nano))
	if err != nil {
		return 0, err
	}
	n, err := res.RowsAffected()
	return int(n), err
}

func (s *SQLiteStore) Vacuum() error {
	_, err := s.db.Exec("VACUUM")
	return err
}

func (s *SQLiteStore) ConsecutiveFailures(targetName, address string, port int) (int, error) {
	rows, err := s.db.Query(`
SELECT success_count FROM results
WHERE target_name = ? AND address = ? AND port = ?
ORDER BY checked_at DESC`, targetName, address, port)
	if err != nil {
		return 0, err
	}
	defer rows.Close()
	failures := 0
	for rows.Next() {
		var successCount int
		if err := rows.Scan(&successCount); err != nil {
			return 0, err
		}
		if successCount > 0 {
			break
		}
		failures++
	}
	return failures, rows.Err()
}

type resultScanner interface {
	Scan(dest ...any) error
}

type resultRows interface {
	resultScanner
	Next() bool
	Err() error
}

func scanResults(rows resultRows) ([]model.Result, error) {
	var results []model.Result
	for rows.Next() {
		result, err := scanResult(rows)
		if err != nil {
			return nil, err
		}
		results = append(results, result)
	}
	return results, rows.Err()
}

func scanResult(scanner resultScanner) (model.Result, error) {
	var result model.Result
	var labels string
	var checkedAt string
	if err := scanner.Scan(
		&result.Agent, &result.AgentIP, &result.TargetName, &result.Address, &result.Port, &labels,
		&checkedAt, &result.SuccessCount, &result.FailureCount,
		&result.AverageLatencyMS, &result.SuccessRate, &result.Error,
	); err != nil {
		return result, err
	}
	if labels != "" {
		_ = json.Unmarshal([]byte(labels), &result.Labels)
	}
	t, err := time.Parse(time.RFC3339Nano, checkedAt)
	if err != nil {
		return result, fmt.Errorf("parse checked_at %q: %w", checkedAt, err)
	}
	result.CheckedAt = t
	return result, nil
}

func isDuplicateColumnError(err error) bool {
	return strings.Contains(strings.ToLower(err.Error()), "duplicate column")
}

func truncateUTC(t time.Time, d time.Duration) time.Time {
	return t.UTC().Truncate(d)
}

type FileStore struct {
	path string
	mu   sync.Mutex
}

func NewFileStore(path string) (*FileStore, error) {
	if path == "" {
		path = "data/results.jsonl"
	}
	if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
		return nil, err
	}
	file, err := os.OpenFile(path, os.O_CREATE, 0644)
	if err != nil {
		return nil, err
	}
	_ = file.Close()
	return &FileStore{path: path}, nil
}

func (s *FileStore) SaveResult(result model.Result) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	file, err := os.OpenFile(s.path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		return err
	}
	defer file.Close()
	b, err := json.Marshal(result)
	if err != nil {
		return err
	}
	if _, err := file.Write(append(b, '\n')); err != nil {
		return err
	}
	return nil
}

func (s *FileStore) RecentResults(limit int) ([]model.Result, error) {
	results, err := s.readAll()
	if err != nil {
		return nil, err
	}
	sort.Slice(results, func(i, j int) bool {
		return results[i].CheckedAt.After(results[j].CheckedAt)
	})
	if limit > 0 && len(results) > limit {
		results = results[:limit]
	}
	return results, nil
}

func (s *FileStore) ResultsSince(since time.Time) ([]model.Result, error) {
	results, err := s.RecentResults(0)
	if err != nil {
		return nil, err
	}
	filtered := make([]model.Result, 0, len(results))
	for _, result := range results {
		if !result.CheckedAt.Before(since) {
			filtered = append(filtered, result)
		}
	}
	return filtered, nil
}

func (s *FileStore) ResultsSinceCompacted(since, rawCutoff time.Time) ([]model.Result, error) {
	return s.ResultsSince(since)
}

func (s *FileStore) RollupBefore(cutoff time.Time, interval time.Duration) (int, error) {
	return 0, nil
}

func (s *FileStore) DeleteBefore(cutoff time.Time) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	results, err := s.readAllLocked()
	if err != nil {
		return 0, err
	}
	kept := make([]model.Result, 0, len(results))
	deleted := 0
	for _, result := range results {
		if result.CheckedAt.Before(cutoff) {
			deleted++
			continue
		}
		kept = append(kept, result)
	}
	tmp := s.path + ".tmp"
	file, err := os.OpenFile(tmp, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0644)
	if err != nil {
		return 0, err
	}
	enc := json.NewEncoder(file)
	for _, result := range kept {
		if err := enc.Encode(result); err != nil {
			_ = file.Close()
			return 0, err
		}
	}
	if err := file.Close(); err != nil {
		return 0, err
	}
	return deleted, os.Rename(tmp, s.path)
}

func (s *FileStore) DeleteRollupsBefore(cutoff time.Time) (int, error) {
	return 0, nil
}

func (s *FileStore) Vacuum() error {
	return nil
}

func (s *FileStore) ConsecutiveFailures(targetName, address string, port int) (int, error) {
	results, err := s.RecentResults(0)
	if err != nil {
		return 0, err
	}
	failures := 0
	for _, result := range results {
		if result.TargetName != targetName || result.Address != address || result.Port != port {
			continue
		}
		if result.SuccessCount > 0 {
			break
		}
		failures++
	}
	return failures, nil
}

func (s *FileStore) readAll() ([]model.Result, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.readAllLocked()
}

func (s *FileStore) readAllLocked() ([]model.Result, error) {
	file, err := os.Open(s.path)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	var results []model.Result
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		var result model.Result
		if err := json.Unmarshal(scanner.Bytes(), &result); err != nil {
			continue
		}
		results = append(results, result)
	}
	return results, scanner.Err()
}
