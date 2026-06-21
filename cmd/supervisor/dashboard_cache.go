package main

import (
	"bufio"
	"crypto/sha1"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"sync"
	"time"

	"pingmon/internal/model"
)

const (
	dashboardCacheVersion         = 5
	dashboardCacheRefreshAfter    = 6 * time.Hour
	dashboardCacheBuildYieldEvery = 1000
	dashboardCacheBuildRowsPerSec = 25000
	dashboardMaxCacheRange        = 365 * 24 * time.Hour
	dashboardCacheDayBucket       = 24 * time.Hour
)

func bucketDayPath(t time.Time) string {
	utc := t.UTC().Truncate(dashboardCacheDayBucket)
	y, m, d := utc.Date()
	return fmt.Sprintf("%04d/%02d/%02d", y, m, d)
}

func bucketDay(t time.Time) string {
	return t.UTC().Truncate(dashboardCacheDayBucket).Format("2006-01-02")
}

type dashboardCacheKey struct {
	Agent string `json:"agent,omitempty"`
}

type dashboardCacheMeta struct {
	Key     dashboardCacheKey `json:"key"`
	Since   time.Time         `json:"since"`
	BuiltAt time.Time         `json:"built_at"`
	Version int               `json:"version"`
}

type dashboardDeltaEntry struct {
	SavedAt time.Time       `json:"saved_at"`
	Agent   string          `json:"agent"`
	Row     json.RawMessage `json:"row"`
}

type dashboardResultCache struct {
	dir        string
	mu         sync.Mutex
	generation int64
	pending    map[dashboardCacheKey]struct{}
	deltas     []dashboardDeltaEntry
	deltasRead bool
}

func newDashboardResultCache(sqlitePath string) *dashboardResultCache {
	if sqlitePath == "" {
		sqlitePath = "data/pingmon.db"
	}
	return &dashboardResultCache{dir: filepath.Join(filepath.Dir(sqlitePath), "dashboard-cache"), pending: make(map[dashboardCacheKey]struct{})}
}

func (c *dashboardResultCache) refresh(key dashboardCacheKey, since time.Time, build func(func(model.Result) error) error) error {
	if c == nil {
		return nil
	}
	generation := c.currentGeneration()
	_, err := c.build(key, since, generation, build)
	return err
}

func (c *dashboardResultCache) currentGeneration() int64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.generation
}

func (c *dashboardResultCache) markPending(key dashboardCacheKey) bool {
	if c == nil {
		return false
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.pending == nil {
		c.pending = make(map[dashboardCacheKey]struct{})
	}
	if _, ok := c.pending[key]; ok {
		return false
	}
	c.pending[key] = struct{}{}
	return true
}

func (c *dashboardResultCache) unmarkPending(key dashboardCacheKey) {
	if c == nil {
		return
	}
	c.mu.Lock()
	delete(c.pending, key)
	c.mu.Unlock()
}

func (c *dashboardResultCache) cacheState(key dashboardCacheKey) string {
	if c == nil {
		return "none"
	}
	c.mu.Lock()
	_, pending := c.pending[key]
	c.mu.Unlock()
	if pending {
		return "building"
	}
	meta, err := c.readMeta(key)
	if err != nil {
		return "none"
	}
	if time.Since(meta.BuiltAt) > dashboardCacheRefreshAfter {
		return "stale"
	}
	return "ready"
}

func (c *dashboardResultCache) ensureCache(key dashboardCacheKey, enqueue func()) string {
	state := c.cacheState(key)
	if state == "none" || state == "stale" {
		enqueue()
		if state == "none" && c.isPending(key) {
			state = "building"
		}
		if state == "stale" {
			return "stale"
		}
	}
	return state
}

func (c *dashboardResultCache) isPending(key dashboardCacheKey) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	_, ok := c.pending[key]
	return ok
}

func (c *dashboardResultCache) build(key dashboardCacheKey, since time.Time, generation int64, buildResults func(func(model.Result) error) error) (dashboardCacheMeta, error) {
	bucketDir := c.bucketDir(key)
	if err := os.MkdirAll(bucketDir, 0755); err != nil {
		return dashboardCacheMeta{}, err
	}
	builtAt := time.Now().UTC()
	meta := dashboardCacheMeta{
		Key:     key,
		Since:   since.UTC(),
		BuiltAt: builtAt,
		Version: dashboardCacheVersion,
	}

	type dayWriter struct {
		file  *os.File
		buf   *bufio.Writer
		tmp   string
		final string
		day   string
	}
	var current *dayWriter
	written := 0
	buildStarted := time.Now()

	closeWriter := func(dw *dayWriter) error {
		if dw == nil {
			return nil
		}
		if err := dw.buf.Flush(); err != nil {
			dw.file.Close()
			os.Remove(dw.tmp)
			return err
		}
		if err := dw.file.Close(); err != nil {
			os.Remove(dw.tmp)
			return err
		}
		if err := os.Rename(dw.tmp, dw.final); err != nil {
			os.Remove(dw.tmp)
			return err
		}
		return nil
	}

	cleanup := func() {
		if current != nil {
			current.file.Close()
			os.Remove(current.tmp)
		}
	}

	if err := buildResults(func(result model.Result) error {
		day := bucketDay(result.CheckedAt)
		if current == nil || current.day != day {
			if err := closeWriter(current); err != nil {
				return err
			}
			current = nil
			dw := &dayWriter{day: day, final: c.bucketPath(key, result.CheckedAt)}
			dw.tmp = dw.final + fmt.Sprintf(".%d.tmp", time.Now().UnixNano())
			if err := os.MkdirAll(filepath.Dir(dw.tmp), 0755); err != nil {
				return err
			}
			var err error
			dw.file, err = os.Create(dw.tmp)
			if err != nil {
				return err
			}
			dw.buf = bufio.NewWriter(dw.file)
			current = dw
		}
		line, err := marshalDashboardResult(result)
		if err != nil {
			return err
		}
		if _, err := current.buf.Write(line); err != nil {
			return err
		}
		if err := current.buf.WriteByte('\n'); err != nil {
			return err
		}
		written++
		if written%dashboardCacheBuildYieldEvery == 0 {
			if err := current.buf.Flush(); err != nil {
				return err
			}
			throttleDashboardCacheBuild(buildStarted, written)
		}
		return nil
	}); err != nil {
		cleanup()
		return dashboardCacheMeta{}, err
	}
	if err := closeWriter(current); err != nil {
		cleanup()
		return dashboardCacheMeta{}, err
	}

	metaData, err := json.Marshal(meta)
	if err != nil {
		cleanup()
		return dashboardCacheMeta{}, err
	}
	tmpMetaPath := c.metaPath(key) + fmt.Sprintf(".%d.tmp", time.Now().UnixNano())
	if err := os.WriteFile(tmpMetaPath, metaData, 0644); err != nil {
		cleanup()
		return dashboardCacheMeta{}, err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if generation != c.generation {
		os.Remove(tmpMetaPath)
		return meta, nil
	}
	if err := os.Rename(tmpMetaPath, c.metaPath(key)); err != nil {
		os.Remove(tmpMetaPath)
		return dashboardCacheMeta{}, err
	}
	if err := c.compactDeltasLocked(); err != nil {
		log.Printf("dashboard cache delta compact: %v", err)
	}
	return meta, nil
}

func (c *dashboardResultCache) writeIfReady(w http.ResponseWriter, key dashboardCacheKey, since time.Time) (bool, error) {
	if c == nil {
		return false, fmt.Errorf("dashboard cache is not configured")
	}
	meta, err := c.readMeta(key)
	if err != nil {
		return false, err
	}
	stale := time.Since(meta.BuiltAt) > dashboardCacheRefreshAfter
	w.Header().Set("Content-Type", "application/json")
	if _, err := w.Write([]byte("[")); err != nil {
		return false, err
	}
	first := true
	writeLine := func(line []byte) error {
		line = bytesTrimSpace(line)
		if len(line) == 0 {
			return nil
		}
		if !first {
			if _, err := w.Write([]byte(",")); err != nil {
				return err
			}
		}
		first = false
		_, err := w.Write(line)
		return err
	}
	if err := c.writeDeltas(meta, writeLine); err != nil {
		return false, err
	}
	if err := c.writeBaseFiltered(key, since, writeLine); err != nil {
		return false, err
	}
	_, err = w.Write([]byte("]"))
	return stale, err
}

func (c *dashboardResultCache) writeBaseFiltered(key dashboardCacheKey, since time.Time, writeLine func([]byte) error) error {
	now := time.Now().UTC()
	cursor := now.Truncate(dashboardCacheDayBucket)
	stop := since.Truncate(dashboardCacheDayBucket)
	for !cursor.Before(stop) {
		if err := c.streamDayBucket(key, cursor, writeLine); err != nil {
			if os.IsNotExist(err) {
				cursor = cursor.Add(-dashboardCacheDayBucket)
				continue
			}
			return err
		}
		// For the bucket containing 'since', rows are sorted DESC so we
		// stop streaming as soon as we pass the cutoff. streamDayBucket
		// already skips rows older than 'since' internally.
		cursor = cursor.Add(-dashboardCacheDayBucket)
	}
	return nil
}

func (c *dashboardResultCache) streamDayBucket(key dashboardCacheKey, day time.Time, writeLine func([]byte) error) error {
	file, err := os.Open(c.bucketPath(key, day))
	if err != nil {
		return err
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	buf := make([]byte, 0, 1024*1024)
	scanner.Buffer(buf, 16*1024*1024)
	for scanner.Scan() {
		line := bytesTrimSpace(scanner.Bytes())
		if len(line) == 0 {
			continue
		}
		if err := writeLine(line); err != nil {
			return err
		}
	}
	return scanner.Err()
}

func (c *dashboardResultCache) appendDelta(results []model.Result) {
	if c == nil || len(results) == 0 {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if err := c.loadDeltasLocked(); err != nil {
		log.Printf("dashboard cache delta load: %v", err)
	}
	if err := os.MkdirAll(c.dir, 0755); err != nil {
		log.Printf("dashboard cache delta mkdir: %v", err)
		return
	}
	file, err := os.OpenFile(c.deltaPath(), os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		log.Printf("dashboard cache delta open: %v", err)
		return
	}
	defer file.Close()
	writer := bufio.NewWriter(file)
	savedAt := time.Now().UTC()
	for _, result := range results {
		row, err := marshalDashboardResult(result)
		if err != nil {
			log.Printf("dashboard cache delta encode: %v", err)
			return
		}
		entry := dashboardDeltaEntry{SavedAt: savedAt, Agent: result.Agent, Row: append([]byte(nil), row...)}
		line, err := json.Marshal(entry)
		if err != nil {
			log.Printf("dashboard cache delta encode: %v", err)
			return
		}
		if _, err := writer.Write(line); err != nil {
			log.Printf("dashboard cache delta write: %v", err)
			return
		}
		if err := writer.WriteByte('\n'); err != nil {
			log.Printf("dashboard cache delta write: %v", err)
			return
		}
		c.deltas = append(c.deltas, entry)
	}
	if err := writer.Flush(); err != nil {
		log.Printf("dashboard cache delta flush: %v", err)
	}
}

func (c *dashboardResultCache) clear() {
	if c == nil {
		return
	}
	c.mu.Lock()
	c.pending = make(map[dashboardCacheKey]struct{})
	c.deltas = nil
	c.deltasRead = false
	c.generation++
	defer c.mu.Unlock()
	if err := os.RemoveAll(c.dir); err != nil {
		log.Printf("dashboard cache clear: %v", err)
	}
}

func (c *dashboardResultCache) clearIncompatible() {
	if c == nil {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	entries, err := os.ReadDir(c.dir)
	if os.IsNotExist(err) {
		return
	}
	if err != nil {
		log.Printf("dashboard cache inspect: %v", err)
		return
	}
	compatibleMetaCount := 0
	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}
		metaPath := filepath.Join(c.dir, entry.Name(), "cache.meta.json")
		data, err := os.ReadFile(metaPath)
		if err != nil {
			continue
		}
		var meta dashboardCacheMeta
		if err := json.Unmarshal(data, &meta); err != nil || meta.Version != dashboardCacheVersion {
			c.clearLocked()
			return
		}
		compatibleMetaCount++
	}
	if len(entries) > 0 && compatibleMetaCount == 0 {
		c.clearLocked()
	}
}

func (c *dashboardResultCache) clearLocked() {
	c.pending = make(map[dashboardCacheKey]struct{})
	c.deltas = nil
	c.deltasRead = false
	c.generation++
	if err := os.RemoveAll(c.dir); err != nil {
		log.Printf("dashboard cache clear: %v", err)
	}
}

func (c *dashboardResultCache) writeDeltas(meta dashboardCacheMeta, writeLine func([]byte) error) error {
	rows, err := c.deltaRows(meta)
	if err != nil {
		return err
	}
	for _, row := range rows {
		if err := writeLine(row); err != nil {
			return err
		}
	}
	return nil
}

func (c *dashboardResultCache) deltaRows(meta dashboardCacheMeta) ([][]byte, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if err := c.loadDeltasLocked(); err != nil {
		return nil, err
	}
	rows := make([][]byte, 0)
	for _, entry := range c.deltas {
		if !entry.SavedAt.After(meta.BuiltAt) {
			continue
		}
		if meta.Key.Agent != "" && entry.Agent != meta.Key.Agent {
			continue
		}
		rows = append(rows, entry.Row)
	}
	return rows, nil
}

func (c *dashboardResultCache) loadDeltasLocked() error {
	if c.deltasRead {
		return nil
	}
	c.deltas = nil
	c.deltasRead = true
	file, err := os.Open(c.deltaPath())
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		return err
	}
	defer file.Close()
	scanner := bufio.NewScanner(file)
	buf := make([]byte, 0, 1024*1024)
	scanner.Buffer(buf, 16*1024*1024)
	for scanner.Scan() {
		var entry dashboardDeltaEntry
		if err := json.Unmarshal(scanner.Bytes(), &entry); err != nil {
			continue
		}
		if entry.SavedAt.IsZero() || entry.Agent == "" || len(entry.Row) == 0 {
			continue
		}
		// Row bytes are immutable after loading so request paths can share them without copying.
		entry.Row = append([]byte(nil), entry.Row...)
		c.deltas = append(c.deltas, entry)
	}
	return scanner.Err()
}

func (c *dashboardResultCache) compactDeltasLocked() error {
	cutoff, ok := c.oldestCacheBuildTimeLocked()
	if !ok {
		return nil
	}
	if err := c.loadDeltasLocked(); err != nil {
		return err
	}
	deltaPath := c.deltaPath()
	if len(c.deltas) == 0 {
		if err := os.Remove(deltaPath); err != nil && !os.IsNotExist(err) {
			return err
		}
		return nil
	}

	tmpPath := deltaPath + fmt.Sprintf(".%d.tmp", time.Now().UnixNano())
	tmp, err := os.Create(tmpPath)
	if err != nil {
		return err
	}
	writer := bufio.NewWriter(tmp)
	kept := c.deltas[:0]
	for _, entry := range c.deltas {
		if !entry.SavedAt.After(cutoff) {
			continue
		}
		line, err := json.Marshal(entry)
		if err != nil {
			tmp.Close()
			os.Remove(tmpPath)
			return err
		}
		if _, err := writer.Write(line); err != nil {
			tmp.Close()
			os.Remove(tmpPath)
			return err
		}
		if err := writer.WriteByte('\n'); err != nil {
			tmp.Close()
			os.Remove(tmpPath)
			return err
		}
		kept = append(kept, entry)
	}
	if err := writer.Flush(); err != nil {
		tmp.Close()
		os.Remove(tmpPath)
		return err
	}
	if err := tmp.Close(); err != nil {
		os.Remove(tmpPath)
		return err
	}
	if err := os.Rename(tmpPath, deltaPath); err != nil {
		os.Remove(tmpPath)
		return err
	}
	c.deltas = kept
	return nil
}

func (c *dashboardResultCache) oldestCacheBuildTimeLocked() (time.Time, bool) {
	entries, err := os.ReadDir(c.dir)
	if err != nil {
		return time.Time{}, false
	}
	var oldest time.Time
	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}
		metaPath := filepath.Join(c.dir, entry.Name(), "cache.meta.json")
		data, err := os.ReadFile(metaPath)
		if err != nil {
			continue
		}
		var meta dashboardCacheMeta
		if err := json.Unmarshal(data, &meta); err != nil {
			continue
		}
		if meta.BuiltAt.IsZero() {
			continue
		}
		if oldest.IsZero() || meta.BuiltAt.Before(oldest) {
			oldest = meta.BuiltAt
		}
	}
	if oldest.IsZero() {
		return time.Time{}, false
	}
	return oldest, true
}

func (c *dashboardResultCache) readMeta(key dashboardCacheKey) (dashboardCacheMeta, error) {
	data, err := os.ReadFile(c.metaPath(key))
	if err != nil {
		return dashboardCacheMeta{}, err
	}
	var meta dashboardCacheMeta
	if err := json.Unmarshal(data, &meta); err != nil {
		return dashboardCacheMeta{}, err
	}
	if meta.Key != key {
		return dashboardCacheMeta{}, fmt.Errorf("dashboard cache key mismatch")
	}
	if meta.Version != dashboardCacheVersion {
		return dashboardCacheMeta{}, fmt.Errorf("dashboard cache version mismatch")
	}
	return meta, nil
}

func (c *dashboardResultCache) bucketDir(key dashboardCacheKey) string {
	return filepath.Join(c.dir, c.keyHash(key))
}

func (c *dashboardResultCache) bucketPath(key dashboardCacheKey, day time.Time) string {
	return filepath.Join(c.bucketDir(key), bucketDayPath(day)+".jsonl")
}

func (c *dashboardResultCache) metaPath(key dashboardCacheKey) string {
	return filepath.Join(c.bucketDir(key), "cache.meta.json")
}

func (c *dashboardResultCache) deltaPath() string {
	return filepath.Join(c.dir, "delta.jsonl")
}

func (c *dashboardResultCache) keyHash(key dashboardCacheKey) string {
	sum := sha1.Sum([]byte(key.Agent))
	return fmt.Sprintf("%x", sum)
}

func marshalDashboardResult(result model.Result) ([]byte, error) {
	data := make([]byte, 0, 256)
	data = append(data, '[')
	data = appendJSONString(data, result.Agent)
	data = append(data, ',')
	data = appendJSONString(data, result.AgentIP)
	data = append(data, ',')
	data = appendJSONString(data, result.TargetName)
	data = append(data, ',')
	data = appendJSONString(data, result.Address)
	data = append(data, ',')
	data = strconv.AppendInt(data, int64(result.Port), 10)
	data = append(data, ',')
	data = appendLabels(data, result.Labels)
	data = append(data, ',')
	data = appendJSONString(data, strconv.FormatInt(result.CheckedAt.UTC().UnixNano(), 36))
	data = append(data, ',')
	data = strconv.AppendInt(data, int64(result.SuccessCount), 10)
	data = append(data, ',')
	data = strconv.AppendInt(data, int64(result.FailureCount), 10)
	data = append(data, ',')
	data = strconv.AppendFloat(data, result.AverageLatencyMS, 'f', -1, 64)
	data = append(data, ',')
	data = strconv.AppendFloat(data, result.SuccessRate, 'f', -1, 64)
	data = append(data, ',')
	data = appendJSONString(data, result.Error)
	data = append(data, ']')
	return data, nil
}

func appendJSONString(data []byte, value string) []byte {
	return strconv.AppendQuote(data, value)
}

func appendLabels(data []byte, labels []string) []byte {
	if len(labels) == 0 {
		return append(data, '[', ']')
	}
	data = append(data, '[')
	for i, label := range labels {
		if i > 0 {
			data = append(data, ',')
		}
		data = appendJSONString(data, label)
	}
	return append(data, ']')
}

func throttleDashboardCacheBuild(started time.Time, written int) {
	targetElapsed := time.Duration(int64(written) * int64(time.Second) / dashboardCacheBuildRowsPerSec)
	if sleep := targetElapsed - time.Since(started); sleep > 0 {
		if sleep > 100*time.Millisecond {
			sleep = 100 * time.Millisecond
		}
		time.Sleep(sleep)
		return
	}
	runtime.Gosched()
}

func bytesTrimSpace(data []byte) []byte {
	for len(data) > 0 {
		switch data[0] {
		case ' ', '\t', '\n', '\r':
			data = data[1:]
		default:
			goto trimRight
		}
	}
trimRight:
	for len(data) > 0 {
		switch data[len(data)-1] {
		case ' ', '\t', '\n', '\r':
			data = data[:len(data)-1]
		default:
			return data
		}
	}
	return data
}
