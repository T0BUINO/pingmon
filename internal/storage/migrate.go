package storage

import (
	"database/sql"
	"os"
	"path/filepath"
)

func MigrateSQLite(path string) (bool, error) {
	if path == "" {
		path = "data/pingmon.db"
	}
	if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
		return false, err
	}
	db, err := sql.Open("sqlite", path)
	if err != nil {
		return false, err
	}
	defer db.Close()
	db.SetMaxOpenConns(1)
	return MigrateSQLiteDB(db)
}

func MigrateSQLiteDB(db *sql.DB) (bool, error) {
	agentsReady, err := hasTable(db, "agent_statuses")
	if err != nil {
		return false, err
	}
	resultsNeedSeriesMigration, err := hasColumn(db, "results", "agent")
	if err != nil {
		return false, err
	}
	rollupsNeedSeriesMigration, err := hasColumn(db, "result_rollups", "agent")
	if err != nil {
		return false, err
	}
	if !resultsNeedSeriesMigration && !rollupsNeedSeriesMigration && agentsReady {
		return false, nil
	}

	tx, err := db.Begin()
	if err != nil {
		return false, err
	}
	defer tx.Rollback()

	if resultsNeedSeriesMigration {
		hasAgentIP, err := hasColumnTx(tx, "results", "agent_ip")
		if err != nil {
			return false, err
		}
		if !hasAgentIP {
			if _, err := tx.Exec(`ALTER TABLE results ADD COLUMN agent_ip TEXT`); err != nil {
				return false, err
			}
		}
	}

	if _, err := tx.Exec(`
CREATE TABLE IF NOT EXISTS result_series (
	id INTEGER PRIMARY KEY,
	agent TEXT NOT NULL,
	agent_ip TEXT NOT NULL,
	target_name TEXT NOT NULL,
	address TEXT NOT NULL,
	port INTEGER NOT NULL,
	labels TEXT NOT NULL,
	UNIQUE(agent, agent_ip, target_name, address, port, labels)
)`); err != nil {
		return false, err
	}

	if resultsNeedSeriesMigration {
		if _, err := tx.Exec(`
INSERT OR IGNORE INTO result_series (agent, agent_ip, target_name, address, port, labels)
SELECT DISTINCT agent, COALESCE(agent_ip, ''), target_name, address, port, COALESCE(labels, '')
FROM results`); err != nil {
			return false, err
		}
		if _, err := tx.Exec(`
CREATE TABLE results_new (
	id INTEGER PRIMARY KEY,
	series_id INTEGER NOT NULL,
	checked_at TEXT NOT NULL,
	success_count INTEGER NOT NULL,
	failure_count INTEGER NOT NULL,
	average_latency_ms REAL NOT NULL,
	success_rate REAL NOT NULL,
	error TEXT,
	FOREIGN KEY(series_id) REFERENCES result_series(id)
)`); err != nil {
			return false, err
		}
		if _, err := tx.Exec(`
INSERT INTO results_new (
	id, series_id, checked_at, success_count, failure_count, average_latency_ms, success_rate, error
)
SELECT r.id, rs.id, r.checked_at, r.success_count, r.failure_count, r.average_latency_ms, r.success_rate, r.error
FROM results r
JOIN result_series rs ON rs.agent = r.agent
	AND rs.agent_ip = COALESCE(r.agent_ip, '')
	AND rs.target_name = r.target_name
	AND rs.address = r.address
	AND rs.port = r.port
	AND rs.labels = COALESCE(r.labels, '')`); err != nil {
			return false, err
		}
		if _, err := tx.Exec(`DROP TABLE results`); err != nil {
			return false, err
		}
		if _, err := tx.Exec(`ALTER TABLE results_new RENAME TO results`); err != nil {
			return false, err
		}
	}

	if rollupsNeedSeriesMigration {
		if _, err := tx.Exec(`
INSERT OR IGNORE INTO result_series (agent, agent_ip, target_name, address, port, labels)
SELECT DISTINCT agent, COALESCE(agent_ip, ''), target_name, address, port, COALESCE(labels, '')
FROM result_rollups`); err != nil {
			return false, err
		}
		if _, err := tx.Exec(`
CREATE TABLE result_rollups_new (
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
)`); err != nil {
			return false, err
		}
		if _, err := tx.Exec(`
INSERT INTO result_rollups_new (
	id, series_id, bucket_start, interval_seconds, sample_count, success_count,
	failure_count, average_latency_ms, success_rate, error
)
SELECT rr.id, rs.id, rr.bucket_start, rr.interval_seconds, rr.sample_count, rr.success_count,
	rr.failure_count, rr.average_latency_ms, rr.success_rate, rr.error
FROM result_rollups rr
JOIN result_series rs ON rs.agent = rr.agent
	AND rs.agent_ip = COALESCE(rr.agent_ip, '')
	AND rs.target_name = rr.target_name
	AND rs.address = rr.address
	AND rs.port = rr.port
	AND rs.labels = COALESCE(rr.labels, '')`); err != nil {
			return false, err
		}
		if _, err := tx.Exec(`DROP TABLE result_rollups`); err != nil {
			return false, err
		}
		if _, err := tx.Exec(`ALTER TABLE result_rollups_new RENAME TO result_rollups`); err != nil {
			return false, err
		}
	}

	if !agentsReady {
		if _, err := tx.Exec(`
CREATE TABLE IF NOT EXISTS agent_statuses (
	agent TEXT PRIMARY KEY,
	agent_ip TEXT NOT NULL,
	first_seen_at TEXT NOT NULL,
	last_seen_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_agent_statuses_last_seen_at ON agent_statuses(last_seen_at DESC)`); err != nil {
			return false, err
		}
	}

	if resultsNeedSeriesMigration || rollupsNeedSeriesMigration {
		if _, err := tx.Exec(`DELETE FROM sqlite_sequence WHERE name IN ('results', 'result_rollups')`); err != nil {
			return false, err
		}
	}
	if err := tx.Commit(); err != nil {
		return false, err
	}
	return true, nil
}

func hasTable(db *sql.DB, table string) (bool, error) {
	var name string
	err := db.QueryRow("SELECT name FROM sqlite_master WHERE type = 'table' AND name = ?", table).Scan(&name)
	if err == sql.ErrNoRows {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return true, nil
}

func hasColumn(db *sql.DB, table, column string) (bool, error) {
	rows, err := db.Query("PRAGMA table_info(" + table + ")")
	if err != nil {
		return false, err
	}
	defer rows.Close()
	return scanColumnInfo(rows, column)
}

func hasColumnTx(tx *sql.Tx, table, column string) (bool, error) {
	rows, err := tx.Query("PRAGMA table_info(" + table + ")")
	if err != nil {
		return false, err
	}
	defer rows.Close()
	return scanColumnInfo(rows, column)
}

func scanColumnInfo(rows *sql.Rows, column string) (bool, error) {
	for rows.Next() {
		var cid int
		var name string
		var typ string
		var notNull int
		var dfltValue any
		var pk int
		if err := rows.Scan(&cid, &name, &typ, &notNull, &dfltValue, &pk); err != nil {
			return false, err
		}
		if name == column {
			return true, nil
		}
	}
	return false, rows.Err()
}
