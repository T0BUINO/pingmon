package main

import (
	"bufio"
	"crypto/sha1"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"pingmon/internal/model"
)

const (
	dashboardCacheVersion         = 1
	dashboardCacheFreshness       = 5 * time.Minute
	dashboardCacheBuildYieldEvery = 100
)

type dashboardCacheKey struct {
	SelectedRange string `json:"selected_range"`
	Agent         string `json:"agent,omitempty"`
}

type dashboardCacheMeta struct {
	Key     dashboardCacheKey `json:"key"`
	Since   time.Time         `json:"since"`
	BuiltAt time.Time         `json:"built_at"`
	Version int               `json:"version"`
}

type dashboardDeltaEntry struct {
	SavedAt time.Time    `json:"saved_at"`
	Result  model.Result `json:"result"`
}

type dashboardResultCache struct {
	dir        string
	mu         sync.Mutex
	generation int64
	pending    map[dashboardCacheKey]struct{}
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

func (c *dashboardResultCache) build(key dashboardCacheKey, since time.Time, generation int64, buildResults func(func(model.Result) error) error) (dashboardCacheMeta, error) {
	if err := os.MkdirAll(c.dir, 0755); err != nil {
		return dashboardCacheMeta{}, err
	}
	builtAt := time.Now().UTC()
	meta := dashboardCacheMeta{
		Key:     key,
		Since:   since.UTC(),
		BuiltAt: builtAt,
		Version: dashboardCacheVersion,
	}
	dataPath := c.dataPath(key)
	tmpDataPath := dataPath + fmt.Sprintf(".%d.tmp", time.Now().UnixNano())
	file, err := os.Create(tmpDataPath)
	if err != nil {
		return dashboardCacheMeta{}, err
	}
	writer := bufio.NewWriter(file)
	written := 0
	first := true
	if err := writer.WriteByte('['); err != nil {
		file.Close()
		os.Remove(tmpDataPath)
		return dashboardCacheMeta{}, err
	}
	if err := buildResults(func(result model.Result) error {
		line, err := json.Marshal(result)
		if err != nil {
			return err
		}
		if !first {
			if err := writer.WriteByte(','); err != nil {
				return err
			}
		}
		first = false
		if _, err := writer.Write(line); err != nil {
			return err
		}
		written++
		if written%dashboardCacheBuildYieldEvery == 0 {
			if err := writer.Flush(); err != nil {
				return err
			}
			time.Sleep(20 * time.Millisecond)
		}
		return nil
	}); err != nil {
		file.Close()
		os.Remove(tmpDataPath)
		return dashboardCacheMeta{}, err
	}
	if err := writer.WriteByte(']'); err != nil {
		file.Close()
		os.Remove(tmpDataPath)
		return dashboardCacheMeta{}, err
	}
	if err := writer.Flush(); err != nil {
		file.Close()
		os.Remove(tmpDataPath)
		return dashboardCacheMeta{}, err
	}
	if err := file.Close(); err != nil {
		os.Remove(tmpDataPath)
		return dashboardCacheMeta{}, err
	}
	metaData, err := json.Marshal(meta)
	if err != nil {
		os.Remove(tmpDataPath)
		return dashboardCacheMeta{}, err
	}
	tmpMetaPath := c.metaPath(key) + fmt.Sprintf(".%d.tmp", time.Now().UnixNano())
	if err := os.WriteFile(tmpMetaPath, metaData, 0644); err != nil {
		os.Remove(tmpDataPath)
		return dashboardCacheMeta{}, err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if generation != c.generation {
		os.Remove(tmpDataPath)
		os.Remove(tmpMetaPath)
		return meta, nil
	}
	if err := os.Rename(tmpDataPath, dataPath); err != nil {
		os.Remove(tmpDataPath)
		os.Remove(tmpMetaPath)
		return dashboardCacheMeta{}, err
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

func (c *dashboardResultCache) writeIfReady(w http.ResponseWriter, key dashboardCacheKey) (bool, error) {
	if c == nil {
		return false, fmt.Errorf("dashboard cache is not configured")
	}
	c.mu.Lock()
	meta, err := c.readMeta(key)
	c.mu.Unlock()
	if err != nil {
		return false, err
	}
	stale := time.Since(meta.BuiltAt) > dashboardCacheFreshness
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
	if err := c.writeBaseArray(key, w, func() error {
		if !first {
			if _, err := w.Write([]byte(",")); err != nil {
				return err
			}
		}
		first = false
		return nil
	}); err != nil {
		return false, err
	}
	_, err = w.Write([]byte("]"))
	return stale, err
}

func (c *dashboardResultCache) appendDelta(results []model.Result) {
	if c == nil || len(results) == 0 {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
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
		line, err := json.Marshal(dashboardDeltaEntry{SavedAt: savedAt, Result: result})
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
	c.generation++
	defer c.mu.Unlock()
	if err := os.RemoveAll(c.dir); err != nil {
		log.Printf("dashboard cache clear: %v", err)
	}
}

func (c *dashboardResultCache) writeBaseArray(key dashboardCacheKey, w io.Writer, beforeBody func() error) error {
	file, err := os.Open(c.dataPath(key))
	if err != nil {
		return err
	}
	defer file.Close()
	stat, err := file.Stat()
	if err != nil {
		return err
	}
	if stat.Size() <= 2 {
		return nil
	}
	firstByte := make([]byte, 1)
	if _, err := io.ReadFull(file, firstByte); err != nil {
		return err
	}
	if firstByte[0] != '[' {
		return fmt.Errorf("dashboard cache base is not a JSON array")
	}
	if err := beforeBody(); err != nil {
		return err
	}
	_, err = io.CopyN(w, file, stat.Size()-2)
	return err
}

func (c *dashboardResultCache) writeDeltas(meta dashboardCacheMeta, writeLine func([]byte) error) error {
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
		if !entry.SavedAt.After(meta.BuiltAt) {
			continue
		}
		if meta.Key.Agent != "" && entry.Result.Agent != meta.Key.Agent {
			continue
		}
		line, err := json.Marshal(entry.Result)
		if err != nil {
			return err
		}
		if err := writeLine(line); err != nil {
			return err
		}
	}
	return scanner.Err()
}

func (c *dashboardResultCache) compactDeltasLocked() error {
	cutoff, ok := c.oldestCacheBuildTimeLocked()
	if !ok {
		return nil
	}
	deltaPath := c.deltaPath()
	file, err := os.Open(deltaPath)
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		return err
	}
	defer file.Close()

	tmpPath := deltaPath + fmt.Sprintf(".%d.tmp", time.Now().UnixNano())
	tmp, err := os.Create(tmpPath)
	if err != nil {
		return err
	}
	writer := bufio.NewWriter(tmp)
	scanner := bufio.NewScanner(file)
	buf := make([]byte, 0, 1024*1024)
	scanner.Buffer(buf, 16*1024*1024)
	for scanner.Scan() {
		line := append([]byte(nil), scanner.Bytes()...)
		var entry dashboardDeltaEntry
		if err := json.Unmarshal(line, &entry); err != nil {
			continue
		}
		if !entry.SavedAt.After(cutoff) {
			continue
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
	}
	if err := scanner.Err(); err != nil {
		tmp.Close()
		os.Remove(tmpPath)
		return err
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
	return nil
}

func (c *dashboardResultCache) oldestCacheBuildTimeLocked() (time.Time, bool) {
	entries, err := os.ReadDir(c.dir)
	if err != nil {
		return time.Time{}, false
	}
	var oldest time.Time
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".meta.json") {
			continue
		}
		data, err := os.ReadFile(filepath.Join(c.dir, entry.Name()))
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
	if _, err := os.Stat(c.dataPath(key)); err != nil {
		return dashboardCacheMeta{}, err
	}
	return meta, nil
}

func (c *dashboardResultCache) dataPath(key dashboardCacheKey) string {
	return filepath.Join(c.dir, c.keyHash(key)+".json")
}

func (c *dashboardResultCache) metaPath(key dashboardCacheKey) string {
	return filepath.Join(c.dir, c.keyHash(key)+".meta.json")
}

func (c *dashboardResultCache) deltaPath() string {
	return filepath.Join(c.dir, "delta.jsonl")
}

func (c *dashboardResultCache) keyHash(key dashboardCacheKey) string {
	sum := sha1.Sum([]byte(key.SelectedRange + "\x00" + key.Agent))
	return fmt.Sprintf("%x", sum)
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
