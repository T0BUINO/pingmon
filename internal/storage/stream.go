package storage

import (
	"database/sql"
	"time"

	"pingmon/internal/model"
)

const (
	streamQueryWindow = 6 * time.Hour
)

func (s *SQLiteStore) StreamResultsSince(since time.Time, agent string, fn func(model.Result) error) error {
	return s.streamResultsWindowed(since, time.Now().UTC().Add(time.Nanosecond), agent, fn)
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
	var rows *sql.Rows
	var err error
	if agent == "" {
		rows, err = s.db.Query(`SELECT rs.agent, rs.agent_ip, rs.target_name, rs.address, rs.port, rs.labels, r.checked_at,
r.success_count, r.failure_count, r.average_latency_ms, r.success_rate, COALESCE(r.error, '')
FROM results r JOIN result_series rs ON rs.id = r.series_id
WHERE r.checked_at >= ? AND r.checked_at < ?
ORDER BY r.checked_at DESC`, since.UTC().Format(time.RFC3339Nano), before.UTC().Format(time.RFC3339Nano))
	} else {
		rows, err = s.db.Query(`SELECT rs.agent, rs.agent_ip, rs.target_name, rs.address, rs.port, rs.labels, r.checked_at,
r.success_count, r.failure_count, r.average_latency_ms, r.success_rate, COALESCE(r.error, '')
FROM result_series rs JOIN results r ON r.series_id = rs.id
WHERE rs.agent = ? AND r.checked_at >= ? AND r.checked_at < ?
ORDER BY r.checked_at DESC`, agent, since.UTC().Format(time.RFC3339Nano), before.UTC().Format(time.RFC3339Nano))
	}
	if err != nil {
		return err
	}
	defer rows.Close()
	return streamResults(rows, fn)
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
	var rows *sql.Rows
	var err error
	if agent == "" {
		rows, err = s.db.Query(`SELECT rs.agent, rs.agent_ip, rs.target_name, rs.address, rs.port, rs.labels, rr.bucket_start,
rr.success_count, rr.failure_count, rr.average_latency_ms, rr.success_rate, COALESCE(rr.error, '')
FROM result_rollups rr JOIN result_series rs ON rs.id = rr.series_id
WHERE rr.bucket_start >= ? AND rr.bucket_start < ?
ORDER BY rr.bucket_start DESC`, since.UTC().Format(time.RFC3339Nano), before.UTC().Format(time.RFC3339Nano))
	} else {
		rows, err = s.db.Query(`SELECT rs.agent, rs.agent_ip, rs.target_name, rs.address, rs.port, rs.labels, rr.bucket_start,
rr.success_count, rr.failure_count, rr.average_latency_ms, rr.success_rate, COALESCE(rr.error, '')
FROM result_series rs JOIN result_rollups rr ON rr.series_id = rs.id
WHERE rs.agent = ? AND rr.bucket_start >= ? AND rr.bucket_start < ?
ORDER BY rr.bucket_start DESC`, agent, since.UTC().Format(time.RFC3339Nano), before.UTC().Format(time.RFC3339Nano))
	}
	if err != nil {
		return err
	}
	defer rows.Close()
	return streamResults(rows, fn)
}

func streamResults(rows resultRows, fn func(model.Result) error) error {
	for rows.Next() {
		result, err := scanResult(rows)
		if err != nil {
			return err
		}
		if err := fn(result); err != nil {
			return err
		}
	}
	return rows.Err()
}
