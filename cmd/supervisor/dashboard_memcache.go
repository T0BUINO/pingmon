package main

import (
	"bufio"
	"log"
	"net/http"
	"sync"
	"time"

	"pingmon/internal/model"
)

const dashMemMaxMB = 64
const maxChartPoints = 3000

type agentRow struct {
	checkedAt int64
	data      []byte
}

type dashMemEntry struct {
	lastAccess time.Time
	rows       []agentRow
}

type dashMemCache struct {
	mu     sync.Mutex
	agents map[string]*dashMemEntry
	curMB  int
}

func newDashMemCache() *dashMemCache {
	return &dashMemCache{agents: make(map[string]*dashMemEntry)}
}

func (c *dashMemCache) get(agent string) []agentRow {
	c.mu.Lock()
	defer c.mu.Unlock()
	entry, ok := c.agents[agent]
	if !ok {
		return nil
	}
	entry.lastAccess = time.Now()
	return entry.rows
}

func (c *dashMemCache) set(agent string, rows []agentRow) {
	c.mu.Lock()
	defer c.mu.Unlock()
	entryMB := estimateMB(rows)
	for c.curMB+entryMB > dashMemMaxMB && len(c.agents) > 0 {
		c.evictLocked()
	}
	if existing, ok := c.agents[agent]; ok {
		c.curMB -= estimateMB(existing.rows)
	}
	c.agents[agent] = &dashMemEntry{
		lastAccess: time.Now(),
		rows:       rows,
	}
	c.curMB += entryMB
}

func (c *dashMemCache) invalidate(agent string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if entry, ok := c.agents[agent]; ok {
		c.curMB -= estimateMB(entry.rows)
		delete(c.agents, agent)
	}
}

func (c *dashMemCache) evictLocked() {
	var oldest time.Time
	var oldestAgent string
	for agent, entry := range c.agents {
		if oldestAgent == "" || entry.lastAccess.Before(oldest) {
			oldest = entry.lastAccess
			oldestAgent = agent
		}
	}
	if entry, ok := c.agents[oldestAgent]; ok {
		c.curMB -= estimateMB(entry.rows)
		delete(c.agents, oldestAgent)
	}
}

func (c *dashMemCache) clear() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.agents = make(map[string]*dashMemEntry)
	c.curMB = 0
}

func estimateMB(rows []agentRow) int {
	size := 0
	for _, r := range rows {
		size += len(r.data) + 16
	}
	return size / (1024 * 1024)
}

func (s *server) serveStreamDirect(w http.ResponseWriter, since time.Time, agent string) {
	var rows []agentRow
	if err := s.streamResultsSince(since, agent, func(result model.Result) error {
		line, err := marshalDashboardResult(result)
		if err != nil {
			return err
		}
		rows = append(rows, agentRow{
			checkedAt: result.CheckedAt.UnixNano(),
			data:      append([]byte(nil), line...),
		})
		return nil
	}); err != nil {
		log.Printf("stream dashboard results: %v", err)
	}
	rows = aggregateRowsByTime(rows, since, maxChartPoints)
	writeRows(w, rows)
}

func (s *server) buildCache(agent string, since time.Time) []agentRow {
	var rows []agentRow
	if err := s.streamResultsSince(since, agent, func(result model.Result) error {
		line, err := marshalDashboardResult(result)
		if err != nil {
			return err
		}
		rows = append(rows, agentRow{
			checkedAt: result.CheckedAt.UnixNano(),
			data:      append([]byte(nil), line...),
		})
		return nil
	}); err != nil {
		log.Printf("build cache agent=%q: %v", agent, err)
	}
	if len(rows) > 0 {
		s.dashMem.set(agent, rows)
	}
	return rows
}

func serveFromCache(w http.ResponseWriter, rows []agentRow, since time.Time) {
	sinceUnix := since.UnixNano()
	cut := findFirstLessThan(rows, sinceUnix)
	filtered := rows[:cut]
	filtered = aggregateRowsByTime(filtered, since, maxChartPoints)
	writeRows(w, filtered)
}

func writeRows(w http.ResponseWriter, rows []agentRow) {
	bw := bufio.NewWriter(w)
	if _, err := bw.Write([]byte("[")); err != nil {
		return
	}
	for i := range rows {
		if i > 0 {
			if _, err := bw.Write([]byte(",")); err != nil {
				return
			}
		}
		if _, err := bw.Write(rows[i].data); err != nil {
			return
		}
	}
	if _, err := bw.Write([]byte("]")); err != nil {
		return
	}
	if err := bw.Flush(); err != nil {
		log.Printf("flush cache response: %v", err)
	}
}

func aggregateRowsByTime(rows []agentRow, since time.Time, targetCount int) []agentRow {
	n := len(rows)
	if n <= targetCount {
		return rows
	}
	step := n / targetCount
	if step < 1 {
		step = 1
	}
	result := make([]agentRow, 0, targetCount)
	for i := 0; i < n; i += step {
		result = append(result, rows[i])
	}
	return result
}

func findFirstLessThan(rows []agentRow, targetUnix int64) int {
	lo, hi := 0, len(rows)
	for lo < hi {
		mid := (lo + hi) / 2
		if rows[mid].checkedAt >= targetUnix {
			lo = mid + 1
		} else {
			hi = mid
		}
	}
	return lo
}
