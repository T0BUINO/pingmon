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
	DeleteBefore(cutoff time.Time) (int, error)
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

func (s *SQLiteStore) DeleteBefore(cutoff time.Time) (int, error) {
	res, err := s.db.Exec("DELETE FROM results WHERE checked_at < ?", cutoff.UTC().Format(time.RFC3339Nano))
	if err != nil {
		return 0, err
	}
	n, err := res.RowsAffected()
	return int(n), err
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
