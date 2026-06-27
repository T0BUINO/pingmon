package storage

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"pingmon/internal/model"

	_ "modernc.org/sqlite"
)

type Store interface {
	SaveAgentHeartbeat(agent, agentIP string, seenAt time.Time) error
	ListAgentStatuses() ([]model.AgentStatus, error)
	DeleteAgent(agent string) error
	SaveResults(results []model.Result, seenAt time.Time) ([]model.Result, error)
	ResultsSince(since time.Time) ([]model.Result, error)
	ResultsSinceForAgent(since time.Time, agent string) ([]model.Result, error)
	ResultsSinceCompacted(since, rawCutoff time.Time) ([]model.Result, error)
	ResultsSinceCompactedForAgent(since, rawCutoff time.Time, agent string) ([]model.Result, error)
	StreamResultsSince(since time.Time, agent string, fn func(model.Result) error) error
	StreamResultsSinceCompacted(since, rawCutoff time.Time, agent string, fn func(model.Result) error) error
	RollupBefore(cutoff time.Time, interval time.Duration) (int, error)
	DeleteBefore(cutoff time.Time) (int, error)
	DeleteRollupsBefore(cutoff time.Time) (int, error)
	Vacuum() error
	ConsecutiveFailures(targetName, address string, port int, limit int) (int, error)
}

func New(sqlitePath string) (Store, error) {
	return NewSQLiteStore(sqlitePath)
}

type SQLiteStore struct {
	db          *sql.DB
	seriesCache sync.Map
}

func NewSQLiteStore(path string) (*SQLiteStore, error) {
	if path == "" {
		path = "data/pingmon.db"
	}
	if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
		return nil, err
	}
	db, err := sql.Open("sqlite", sqliteDSN(path))
	if err != nil {
		return nil, err
	}
	store := &SQLiteStore{db: db}
	if err := store.init(); err != nil {
		_ = db.Close()
		return nil, err
	}
	return store, nil
}

func sqliteDSN(path string) string {
	q := url.Values{}
	q.Add("_pragma", "busy_timeout(5000)")
	q.Add("_pragma", "journal_mode(WAL)")
	q.Add("_pragma", "synchronous(NORMAL)")
	q.Add("_pragma", "temp_store(MEMORY)")
	q.Add("_pragma", "cache_size(-65536)")
	q.Add("_pragma", "wal_autocheckpoint(1000)")
	return path + "?" + q.Encode()
}

func (s *SQLiteStore) init() error {
	if _, err := MigrateSQLiteDB(s.db); err != nil {
		return err
	}
	_, err := s.db.Exec(`
CREATE TABLE IF NOT EXISTS result_series (
	id INTEGER PRIMARY KEY,
	agent TEXT NOT NULL,
	agent_ip TEXT NOT NULL,
	target_name TEXT NOT NULL,
	address TEXT NOT NULL,
	port INTEGER NOT NULL,
	labels TEXT NOT NULL,
	UNIQUE(agent, agent_ip, target_name, address, port, labels)
);
CREATE INDEX IF NOT EXISTS idx_result_series_agent ON result_series(agent);
CREATE INDEX IF NOT EXISTS idx_result_series_target ON result_series(target_name, address, port);
CREATE TABLE IF NOT EXISTS results (
	id INTEGER PRIMARY KEY,
	series_id INTEGER NOT NULL,
	checked_at TEXT NOT NULL,
	success_count INTEGER NOT NULL,
	failure_count INTEGER NOT NULL,
	average_latency_ms REAL NOT NULL,
	success_rate REAL NOT NULL,
	error TEXT,
	FOREIGN KEY(series_id) REFERENCES result_series(id)
);
CREATE INDEX IF NOT EXISTS idx_results_checked_at ON results(checked_at DESC);
CREATE INDEX IF NOT EXISTS idx_results_series_checked_at ON results(series_id, checked_at DESC);

CREATE TABLE IF NOT EXISTS result_rollups (
	id INTEGER PRIMARY KEY,
	series_id INTEGER NOT NULL,
	bucket_start TEXT NOT NULL,
	interval_seconds INTEGER NOT NULL,
	sample_count INTEGER NOT NULL,
	success_count INTEGER NOT NULL,
	failure_count INTEGER NOT NULL,
	average_latency_ms REAL NOT NULL,
	success_rate REAL NOT NULL,
	error TEXT,
	FOREIGN KEY(series_id) REFERENCES result_series(id)
);
CREATE UNIQUE INDEX IF NOT EXISTS idx_result_rollups_unique
ON result_rollups(series_id, bucket_start, interval_seconds);
CREATE INDEX IF NOT EXISTS idx_result_rollups_bucket ON result_rollups(bucket_start DESC);

CREATE TABLE IF NOT EXISTS agent_statuses (
	agent TEXT PRIMARY KEY,
	agent_ip TEXT NOT NULL,
	first_seen_at TEXT NOT NULL,
	last_seen_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_agent_statuses_last_seen_at ON agent_statuses(last_seen_at DESC);
`)
	if err != nil {
		return err
	}
	return nil
}

func (s *SQLiteStore) SaveAgentHeartbeat(agent, agentIP string, seenAt time.Time) error {
	agent = strings.TrimSpace(agent)
	if agent == "" {
		return nil
	}
	seen := seenAt.UTC().Format(time.RFC3339Nano)
	_, err := s.db.Exec(`
INSERT INTO agent_statuses (agent, agent_ip, first_seen_at, last_seen_at)
VALUES (?, ?, ?, ?)
ON CONFLICT(agent) DO UPDATE SET
	agent_ip = excluded.agent_ip,
	last_seen_at = excluded.last_seen_at`,
		agent, agentIP, seen, seen)
	return err
}

func (s *SQLiteStore) ListAgentStatuses() ([]model.AgentStatus, error) {
	rows, err := s.db.Query(`
SELECT agent, agent_ip, first_seen_at, last_seen_at
FROM agent_statuses
ORDER BY last_seen_at DESC`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanAgentStatuses(rows)
}

func (s *SQLiteStore) DeleteAgent(agent string) error {
	agent = strings.TrimSpace(agent)
	if agent == "" {
		return nil
	}
	tx, err := s.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if _, err := tx.Exec(`DELETE FROM results WHERE series_id IN (SELECT id FROM result_series WHERE agent = ?)`, agent); err != nil {
		return err
	}
	if _, err := tx.Exec(`DELETE FROM result_rollups WHERE series_id IN (SELECT id FROM result_series WHERE agent = ?)`, agent); err != nil {
		return err
	}
	if _, err := tx.Exec(`DELETE FROM result_series WHERE agent = ?`, agent); err != nil {
		return err
	}
	if _, err := tx.Exec(`DELETE FROM agent_statuses WHERE agent = ?`, agent); err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return err
	}
	s.seriesCache.Range(func(k, _ any) bool { s.seriesCache.Delete(k); return true })
	return nil
}

type dbExec interface {
	Exec(query string, args ...any) (sql.Result, error)
	QueryRow(query string, args ...any) *sql.Row
}

func seriesKey(agent, agentIP, targetName, address string, port int, labels string) string {
	return agent + "\x00" + agentIP + "\x00" + targetName + "\x00" + address + "\x00" + strconv.Itoa(port) + "\x00" + labels
}

func (s *SQLiteStore) seriesID(q dbExec, agent, agentIP, targetName, address string, port int, labels string) (int64, error) {
	key := seriesKey(agent, agentIP, targetName, address, port, labels)
	if v, ok := s.seriesCache.Load(key); ok {
		return v.(int64), nil
	}
	var id int64
	err := q.QueryRow(`
INSERT INTO result_series (agent, agent_ip, target_name, address, port, labels)
VALUES (?, ?, ?, ?, ?, ?)
ON CONFLICT(agent, agent_ip, target_name, address, port, labels) DO UPDATE SET agent_ip = excluded.agent_ip
RETURNING id`, agent, agentIP, targetName, address, port, labels).Scan(&id)
	if err != nil {
		return 0, err
	}
	s.seriesCache.Store(key, id)
	return id, nil
}

func (s *SQLiteStore) SaveResult(result model.Result) error {
	labels, err := json.Marshal(result.Labels)
	if err != nil {
		return err
	}
	seriesID, err := s.seriesID(s.db, result.Agent, result.AgentIP, result.TargetName, result.Address, result.Port, string(labels))
	if err != nil {
		return err
	}
	_, err = s.db.Exec(`
INSERT INTO results (
	series_id, checked_at, success_count, failure_count, average_latency_ms, success_rate, error
) VALUES (?, ?, ?, ?, ?, ?, ?)`,
		seriesID, result.CheckedAt.UTC().Format(time.RFC3339Nano), result.SuccessCount,
		result.FailureCount, result.AverageLatencyMS, result.SuccessRate, result.Error)
	return err
}

func (s *SQLiteStore) SaveResults(results []model.Result, seenAt time.Time) ([]model.Result, error) {
	if len(results) == 0 {
		return nil, nil
	}
	tx, err := s.db.Begin()
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	heartbeats := make(map[string]string, len(results))
	for i := range results {
		result := results[i]
		labels, err := json.Marshal(result.Labels)
		if err != nil {
			return nil, err
		}
		seriesID, err := s.seriesID(tx, result.Agent, result.AgentIP, result.TargetName, result.Address, result.Port, string(labels))
		if err != nil {
			return nil, err
		}
		if _, err := tx.Exec(`
INSERT INTO results (
	series_id, checked_at, success_count, failure_count, average_latency_ms, success_rate, error
) VALUES (?, ?, ?, ?, ?, ?, ?)`,
			seriesID, result.CheckedAt.UTC().Format(time.RFC3339Nano), result.SuccessCount,
			result.FailureCount, result.AverageLatencyMS, result.SuccessRate, result.Error); err != nil {
			return nil, err
		}
		heartbeats[result.Agent] = result.AgentIP
	}
	seen := seenAt.UTC().Format(time.RFC3339Nano)
	for agent, agentIP := range heartbeats {
		agent = strings.TrimSpace(agent)
		if agent == "" {
			continue
		}
		if _, err := tx.Exec(`
INSERT INTO agent_statuses (agent, agent_ip, first_seen_at, last_seen_at)
VALUES (?, ?, ?, ?)
ON CONFLICT(agent) DO UPDATE SET
	agent_ip = excluded.agent_ip,
	last_seen_at = excluded.last_seen_at`,
			agent, agentIP, seen, seen); err != nil {
			return nil, err
		}
	}
	if err := tx.Commit(); err != nil {
		return nil, err
	}
	return results, nil
}

func (s *SQLiteStore) ResultsSince(since time.Time) ([]model.Result, error) {
	rows, err := s.db.Query(`SELECT rs.agent, rs.agent_ip, rs.target_name, rs.address, rs.port, rs.labels, r.checked_at,
r.success_count, r.failure_count, r.average_latency_ms, r.success_rate, COALESCE(r.error, '')
FROM results r JOIN result_series rs ON rs.id = r.series_id
WHERE r.checked_at >= ?
ORDER BY r.checked_at DESC`, since.UTC().Format(time.RFC3339Nano))
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanResults(rows)
}

func (s *SQLiteStore) ResultsSinceForAgent(since time.Time, agent string) ([]model.Result, error) {
	rows, err := s.db.Query(`SELECT rs.agent, rs.agent_ip, rs.target_name, rs.address, rs.port, rs.labels, r.checked_at,
r.success_count, r.failure_count, r.average_latency_ms, r.success_rate, COALESCE(r.error, '')
FROM result_series rs JOIN results r ON r.series_id = rs.id
WHERE rs.agent = ? AND r.checked_at >= ?
ORDER BY r.checked_at DESC`, agent, since.UTC().Format(time.RFC3339Nano))
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanResults(rows)
}

func (s *SQLiteStore) ResultsSinceCompacted(since, rawCutoff time.Time) ([]model.Result, error) {
	if since.After(rawCutoff) || since.Equal(rawCutoff) {
		return s.ResultsSince(since)
	}
	rollups, err := s.rollupsSince(since, rawCutoff)
	if err != nil {
		return nil, err
	}
	raw, err := s.ResultsSince(rawCutoff)
	if err != nil {
		return nil, err
	}
	return mergeResultsDesc(raw, rollups), nil
}

func (s *SQLiteStore) ResultsSinceCompactedForAgent(since, rawCutoff time.Time, agent string) ([]model.Result, error) {
	if since.After(rawCutoff) || since.Equal(rawCutoff) {
		return s.ResultsSinceForAgent(since, agent)
	}
	rollups, err := s.rollupsSinceForAgent(since, rawCutoff, agent)
	if err != nil {
		return nil, err
	}
	raw, err := s.ResultsSinceForAgent(rawCutoff, agent)
	if err != nil {
		return nil, err
	}
	return mergeResultsDesc(raw, rollups), nil
}

func mergeResultsDesc(a, b []model.Result) []model.Result {
	result := make([]model.Result, 0, len(a)+len(b))
	i, j := 0, 0
	for i < len(a) && j < len(b) {
		if a[i].CheckedAt.After(b[j].CheckedAt) {
			result = append(result, a[i])
			i++
		} else {
			result = append(result, b[j])
			j++
		}
	}
	result = append(result, a[i:]...)
	result = append(result, b[j:]...)
	return result
}

func (s *SQLiteStore) rollupsSince(since, before time.Time) ([]model.Result, error) {
	rows, err := s.db.Query(`SELECT rs.agent, rs.agent_ip, rs.target_name, rs.address, rs.port, rs.labels, rr.bucket_start,
rr.success_count, rr.failure_count, rr.average_latency_ms, rr.success_rate, COALESCE(rr.error, '')
FROM result_rollups rr JOIN result_series rs ON rs.id = rr.series_id
WHERE rr.bucket_start >= ? AND rr.bucket_start < ?
ORDER BY rr.bucket_start DESC`, since.UTC().Format(time.RFC3339Nano), before.UTC().Format(time.RFC3339Nano))
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanResults(rows)
}

func (s *SQLiteStore) rollupsSinceForAgent(since, before time.Time, agent string) ([]model.Result, error) {
	rows, err := s.db.Query(`SELECT rs.agent, rs.agent_ip, rs.target_name, rs.address, rs.port, rs.labels, rr.bucket_start,
rr.success_count, rr.failure_count, rr.average_latency_ms, rr.success_rate, COALESCE(rr.error, '')
FROM result_series rs JOIN result_rollups rr ON rr.series_id = rs.id
WHERE rs.agent = ? AND rr.bucket_start >= ? AND rr.bucket_start < ?
ORDER BY rr.bucket_start DESC`, agent, since.UTC().Format(time.RFC3339Nano), before.UTC().Format(time.RFC3339Nano))
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
	series_id, bucket_start, interval_seconds, sample_count, success_count, failure_count,
	average_latency_ms, success_rate, error
)
SELECT
	series_id,
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
		series_id,
		strftime('%Y-%m-%dT%H:%M:%SZ', (unixepoch(checked_at) / ?) * ?, 'unixepoch') AS bucket_start,
		success_count,
		failure_count,
		average_latency_ms,
		success_rate,
		error
	FROM results
	WHERE checked_at < ?
)
GROUP BY series_id, bucket_start`,
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

func (s *SQLiteStore) ConsecutiveFailures(targetName, address string, port int, limit int) (int, error) {
	query := `
SELECT r.success_count FROM results r
JOIN result_series rs ON rs.id = r.series_id
WHERE rs.target_name = ? AND rs.address = ? AND rs.port = ?
ORDER BY r.checked_at DESC`
	args := []any{targetName, address, port}
	if limit > 0 {
		query += "\nLIMIT ?"
		args = append(args, limit)
	}
	rows, err := s.db.Query(query, args...)
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
	results := make([]model.Result, 0, 256)
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

func scanAgentStatuses(rows resultRows) ([]model.AgentStatus, error) {
	statuses := make([]model.AgentStatus, 0, 16)
	for rows.Next() {
		var status model.AgentStatus
		var firstSeenAt string
		var lastSeenAt string
		if err := rows.Scan(&status.Agent, &status.AgentIP, &firstSeenAt, &lastSeenAt); err != nil {
			return nil, err
		}
		first, err := time.Parse(time.RFC3339Nano, firstSeenAt)
		if err != nil {
			return nil, fmt.Errorf("parse first_seen_at %q: %w", firstSeenAt, err)
		}
		last, err := time.Parse(time.RFC3339Nano, lastSeenAt)
		if err != nil {
			return nil, fmt.Errorf("parse last_seen_at %q: %w", lastSeenAt, err)
		}
		status.FirstSeenAt = first
		status.LastSeenAt = last
		statuses = append(statuses, status)
	}
	return statuses, rows.Err()
}

func truncateUTC(t time.Time, d time.Duration) time.Time {
	return t.UTC().Truncate(d)
}
