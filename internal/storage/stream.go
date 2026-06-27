package storage

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"time"

	"pingmon/internal/model"
)

const (
	streamQueryWindow   = 6 * time.Hour
	maxResultsPerWindow = 50000
)

func (s *SQLiteStore) StreamResultsSince(since time.Time, agent string, fn func(model.Result) error) error {
	return s.streamResultsWindowed(since, time.Now().UTC(), agent, fn)
}

func (s *SQLiteStore) StreamResultsSinceCompacted(since, rawCutoff time.Time, agent string, fn func(model.Result) error) error {
	if since.After(rawCutoff) || since.Equal(rawCutoff) {
		return s.StreamResultsSince(since, agent, fn)
	}
	if err := s.streamResultsWindowed(rawCutoff, time.Now().UTC().Add(time.Nanosecond), agent, fn); err != nil {
		return err
	}
	return s.streamRollupsSince(since, rawCutoff, agent, fn)
}

func (s *SQLiteStore) streamResultsWindowed(since, before time.Time, agent string, fn func(model.Result) error) error {
	since = since.UTC()
	cursor := before.UTC()

	// A range starting at or after its end cannot contain results.
	if !cursor.After(since) {
		return nil
	}

	for cursor.After(since) {
		start := cursor.Add(-streamQueryWindow)
		if start.Before(since) {
			start = since
		}
		if err := s.streamResultsWindow(start, cursor, agent, fn); err != nil {
			return err
		}
		cursor = start
	}
	return nil
}

func (s *SQLiteStore) streamResultsWindow(since, before time.Time, agent string, fn func(model.Result) error) error {
	var cursorTime string
	var cursorID int64
	for {
		var rows *sql.Rows
		var err error
		cursorClause := ""
		args := make([]any, 0, 7)
		if agent != "" {
			args = append(args, agent)
		}
		args = append(args, since.UTC().Format(time.RFC3339Nano), before.UTC().Format(time.RFC3339Nano))
		if cursorTime != "" {
			cursorClause = " AND (r.checked_at < ? OR (r.checked_at = ? AND r.id < ?))"
			args = append(args, cursorTime, cursorTime, cursorID)
		}
		args = append(args, maxResultsPerWindow)
		if agent == "" {
			rows, err = s.db.Query(`SELECT r.id, rs.agent, rs.agent_ip, rs.target_name, rs.address, rs.port, rs.labels, r.checked_at,
r.success_count, r.failure_count, r.average_latency_ms, r.success_rate, COALESCE(r.error, '')
FROM results r JOIN result_series rs ON rs.id = r.series_id
				WHERE r.checked_at >= ? AND r.checked_at < ?`+cursorClause+`
ORDER BY r.checked_at DESC, r.id DESC LIMIT ?`, args...)
		} else {
			rows, err = s.db.Query(`SELECT r.id, rs.agent, rs.agent_ip, rs.target_name, rs.address, rs.port, rs.labels, r.checked_at,
r.success_count, r.failure_count, r.average_latency_ms, r.success_rate, COALESCE(r.error, '')
FROM result_series rs JOIN results r ON r.series_id = rs.id
				WHERE rs.agent = ? AND r.checked_at >= ? AND r.checked_at < ?`+cursorClause+`
ORDER BY r.checked_at DESC, r.id DESC LIMIT ?`, args...)
		}
		if err != nil {
			return err
		}
		count, lastTime, lastID, err := streamResultsWithCursor(rows, fn)
		rows.Close()
		if err != nil {
			return err
		}
		if count < maxResultsPerWindow {
			return nil
		}
		cursorTime, cursorID = lastTime, lastID
	}
}

func (s *SQLiteStore) streamRollupsSince(since, before time.Time, agent string, fn func(model.Result) error) error {
	since = since.UTC()
	cursor := before.UTC()
	for cursor.After(since) {
		start := cursor.Add(-streamQueryWindow)
		if start.Before(since) {
			start = since
		}
		if err := s.streamRollupsWindow(start, cursor, agent, fn); err != nil {
			return err
		}
		cursor = start
	}
	return nil
}

func (s *SQLiteStore) streamRollupsWindow(since, before time.Time, agent string, fn func(model.Result) error) error {
	var cursorTime string
	var cursorID int64
	for {
		var rows *sql.Rows
		var err error
		cursorClause := ""
		args := make([]any, 0, 7)
		if agent != "" {
			args = append(args, agent)
		}
		args = append(args, since.UTC().Format(time.RFC3339Nano), before.UTC().Format(time.RFC3339Nano))
		if cursorTime != "" {
			cursorClause = " AND (rr.bucket_start < ? OR (rr.bucket_start = ? AND rr.id < ?))"
			args = append(args, cursorTime, cursorTime, cursorID)
		}
		args = append(args, maxResultsPerWindow)
		if agent == "" {
			rows, err = s.db.Query(`SELECT rr.id, rs.agent, rs.agent_ip, rs.target_name, rs.address, rs.port, rs.labels, rr.bucket_start,
rr.success_count, rr.failure_count, rr.average_latency_ms, rr.success_rate, COALESCE(rr.error, '')
FROM result_rollups rr JOIN result_series rs ON rs.id = rr.series_id
				WHERE rr.bucket_start >= ? AND rr.bucket_start < ?`+cursorClause+`
ORDER BY rr.bucket_start DESC, rr.id DESC LIMIT ?`, args...)
		} else {
			rows, err = s.db.Query(`SELECT rr.id, rs.agent, rs.agent_ip, rs.target_name, rs.address, rs.port, rs.labels, rr.bucket_start,
rr.success_count, rr.failure_count, rr.average_latency_ms, rr.success_rate, COALESCE(rr.error, '')
FROM result_series rs JOIN result_rollups rr ON rr.series_id = rs.id
				WHERE rs.agent = ? AND rr.bucket_start >= ? AND rr.bucket_start < ?`+cursorClause+`
ORDER BY rr.bucket_start DESC, rr.id DESC LIMIT ?`, args...)
		}
		if err != nil {
			return err
		}
		count, lastTime, lastID, err := streamResultsWithCursor(rows, fn)
		rows.Close()
		if err != nil {
			return err
		}
		if count < maxResultsPerWindow {
			return nil
		}
		cursorTime, cursorID = lastTime, lastID
	}
}

func streamResultsWithCursor(rows resultRows, fn func(model.Result) error) (int, string, int64, error) {
	count := 0
	var lastTime string
	var lastID int64
	for rows.Next() {
		result, id, checkedAt, err := scanResultWithID(rows)
		if err != nil {
			return count, lastTime, lastID, err
		}
		if err := fn(result); err != nil {
			return count, lastTime, lastID, err
		}
		count++
		lastTime, lastID = checkedAt, id
	}
	return count, lastTime, lastID, rows.Err()
}

func scanResultWithID(scanner resultScanner) (model.Result, int64, string, error) {
	var id int64
	var result model.Result
	var labels, checkedAt string
	if err := scanner.Scan(&id, &result.Agent, &result.AgentIP, &result.TargetName, &result.Address, &result.Port,
		&labels, &checkedAt, &result.SuccessCount, &result.FailureCount, &result.AverageLatencyMS, &result.SuccessRate, &result.Error); err != nil {
		return result, 0, "", err
	}
	if labels != "" {
		_ = json.Unmarshal([]byte(labels), &result.Labels)
	}
	t, err := time.Parse(time.RFC3339Nano, checkedAt)
	if err != nil {
		return result, 0, "", fmt.Errorf("parse checked_at %q: %w", checkedAt, err)
	}
	result.CheckedAt = t
	return result, id, checkedAt, nil
}

func streamResults(rows resultRows, fn func(model.Result) error) (int, error) {
	count := 0
	for rows.Next() {
		result, err := scanResult(rows)
		if err != nil {
			return count, err
		}
		if err := fn(result); err != nil {
			return count, err
		}
		count++
	}
	return count, rows.Err()
}
