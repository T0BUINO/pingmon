package main

import (
	"bufio"
	"encoding/json"
	"log"
	"net/http"
	"sort"
	"sync"
	"time"

	"pingmon/internal/model"
)

const dashMemMaxMB = 64
const maxChartPoints = 3000

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

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
	log.Printf("serveStreamDirect: agent=%q, since=%v", agent, since)

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
	log.Printf("serveStreamDirect: got %d rows from stream", len(rows))

	if agent != "" {
		rows = aggregateRowsByTime(rows, since, maxChartPoints)
		log.Printf("serveStreamDirect: after aggregation: %d rows", len(rows))
	} else {
		log.Printf("serveStreamDirect: skipping aggregation for all-agents query (%d rows)", len(rows))
	}

	writeRows(w, rows)
}

func (s *server) buildCache(agent string, since time.Time) []agentRow {
	var rows []agentRow
	log.Printf("buildCache called with agent=%q, since=%v", agent, since)

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
	log.Printf("buildCache: agent=%q, total rows=%d", agent, len(rows))

	targets := make(map[string]bool)
	agents := make(map[string]bool)
	for _, row := range rows {
		var parts []interface{}
		if err := json.Unmarshal(row.data, &parts); err == nil {
			if len(parts) >= 3 {
				if s, ok := parts[2].(string); ok {
					targets[s] = true
				}
				if s, ok := parts[0].(string); ok {
					agents[s] = true
				}
			}
		}
	}
	log.Printf("buildCache: agents in cache: %d unique agents", len(agents))
	log.Printf("buildCache: targets in cache: %d unique targets", len(targets))

	// 检查几个实际的数据行
	for i := 0; i < min(3, len(rows)); i++ {
		log.Printf("buildCache: sample row %d: %s", i, string(rows[i].data))
	}

	if len(rows) > 0 {
		s.dashMem.set(agent, rows)
	}
	return rows
}

func serveFromCache(w http.ResponseWriter, rows []agentRow, since time.Time) {
	sinceUnix := since.UnixNano()
	log.Printf("serveFromCache: total rows=%d, since=%d", len(rows), sinceUnix)

	cut := findFirstLessThan(rows, sinceUnix)
	log.Printf("findFirstLessAt: cut=%d", cut)

	filtered := rows[:cut]
	log.Printf("After time filter: rows=%d", len(filtered))

	// 检查过滤后的数据中的时间范围
	if len(filtered) > 0 {
		minTime := filtered[len(filtered)-1].checkedAt
		maxTime := filtered[0].checkedAt
		log.Printf("Filtered data time range: min=%d, max=%d", minTime, maxTime)
	}

	filtered = aggregateRowsByTime(filtered, since, maxChartPoints)
	log.Printf("After aggregation: rows=%d", len(filtered))

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
	log.Printf("aggregateRowsByTime: input=%d, targetCount=%d", n, targetCount)

	if n <= targetCount || targetCount < 1 {
		log.Printf("aggregateRowsByTime: no aggregation needed, returning all %d rows", n)
		return rows
	}

	// A chart row belongs to one target series. Aggregating all rows together
	// makes the first target in each time bucket overwrite every other target,
	// which leaves the browser with only one curve for large result sets.
	type series struct {
		rows   []agentRow
		budget int
	}
	seriesByKey := make(map[string]int)
	seriesList := make([]series, 0)
	for _, row := range rows {
		key := aggregationSeriesKey(row.data)
		index, ok := seriesByKey[key]
		if !ok {
			index = len(seriesList)
			seriesByKey[key] = index
			seriesList = append(seriesList, series{})
		}
		seriesList[index].rows = append(seriesList[index].rows, row)
	}

	// Reserve enough points to draw every series before distributing the rest
	// proportionally. In normal dashboard use the target count is far below the
	// 3,000-point response cap, so every target gets at least two points.
	remaining := targetCount
	minimum := 1
	if len(seriesList)*2 <= targetCount {
		minimum = 2
	}
	for i := range seriesList {
		if remaining == 0 {
			break
		}
		seriesList[i].budget = min(minimum, len(seriesList[i].rows))
		if seriesList[i].budget > remaining {
			seriesList[i].budget = remaining
		}
		remaining -= seriesList[i].budget
	}
	for remaining > 0 {
		best := -1
		bestScore := float64(-1)
		for i := range seriesList {
			if seriesList[i].budget >= len(seriesList[i].rows) {
				continue
			}
			score := float64(len(seriesList[i].rows)) / float64(seriesList[i].budget+1)
			if score > bestScore {
				best, bestScore = i, score
			}
		}
		if best < 0 {
			break
		}
		seriesList[best].budget++
		remaining--
	}

	result := make([]agentRow, 0, targetCount)
	for i := range seriesList {
		if seriesList[i].budget == 0 {
			continue
		}
		result = append(result, aggregateSingleSeries(seriesList[i].rows, since, seriesList[i].budget)...)
	}
	sort.SliceStable(result, func(i, j int) bool { return result[i].checkedAt > result[j].checkedAt })
	if len(result) > targetCount {
		result = result[:targetCount]
	}
	log.Printf("aggregateRowsByTime: %d rows across %d series -> %d rows", n, len(seriesList), len(result))
	return result
}

func aggregationSeriesKey(data []byte) string {
	var parts []json.RawMessage
	if err := json.Unmarshal(data, &parts); err != nil || len(parts) < 5 {
		return ""
	}
	// Match the browser's target identity, with agent included for safety if
	// this helper is reused for a multi-agent response later.
	return string(parts[0]) + "\x00" + string(parts[2]) + "\x00" + string(parts[3]) + "\x00" + string(parts[4])
}

func aggregateSingleSeries(rows []agentRow, since time.Time, targetCount int) []agentRow {
	n := len(rows)
	if n <= targetCount {
		return rows
	}

	newestTime := rows[0].checkedAt
	requestedSpan := newestTime - since.UnixNano()
	if requestedSpan < 1 {
		return rows
	}
	oldestTime := rows[n-1].checkedAt
	span := newestTime - oldestTime + 1
	if span < 1 {
		return rows
	}

	// Base buckets on the data's actual time range. Using the selected range
	// (for example 30 days) wastes nearly all buckets when only a few days of
	// samples exist.
	bucketNanos := (span + int64(targetCount) - 1) / int64(targetCount)
	if bucketNanos < 1 {
		return rows
	}

	result := make([]agentRow, 0, targetCount)
	bucketEnd := newestTime
	startIdx := 0

	for b := 0; b < targetCount; b++ {
		bucketStart := bucketEnd - bucketNanos + 1

		for startIdx < n && rows[startIdx].checkedAt > bucketEnd {
			startIdx++
		}

		var sumLatency float64
		var validCount int
		var templateData []byte
		var fallback agentRow
		var hasFallback bool
		var bucketNewest int64
		var bucketOldest int64

		bucketIdx := startIdx
		for bucketIdx < n && rows[bucketIdx].checkedAt >= bucketStart {
			lat, ok := extractLatency(rows[bucketIdx].data)
			if ok {
				if validCount == 0 {
					templateData = rows[bucketIdx].data
				}
				sumLatency += lat
				validCount++
			}
			if !hasFallback {
				fallback = rows[bucketIdx]
				hasFallback = true
			}
			if bucketNewest == 0 {
				bucketNewest = rows[bucketIdx].checkedAt
			}
			bucketOldest = rows[bucketIdx].checkedAt
			bucketIdx++
		}

		if validCount > 0 {
			checkedAt := bucketOldest + (bucketNewest-bucketOldest)/2
			avgData := buildAggregatedData(templateData, sumLatency/float64(validCount), checkedAt)
			result = append(result, agentRow{
				checkedAt: checkedAt,
				data:      avgData,
			})
		} else if hasFallback {
			result = append(result, fallback)
		}

		startIdx = bucketIdx
		bucketEnd = bucketStart - 1
		if startIdx >= n {
			break
		}
	}
	return result
}

func extractLatency(data []byte) (float64, bool) {
	var parts []interface{}
	if err := json.Unmarshal(data, &parts); err != nil {
		return 0, false
	}
	if len(parts) < 10 {
		return 0, false
	}
	lat, ok := parts[9].(float64)
	return lat, ok
}

func buildAggregatedData(original []byte, avgLatency float64, checkedAt int64) []byte {
	var parts []interface{}
	if err := json.Unmarshal(original, &parts); err != nil {
		return original
	}
	if len(parts) >= 10 {
		parts[6] = time.Unix(0, checkedAt).UTC().Format(time.RFC3339Nano)
		parts[7] = float64(1)
		parts[9] = avgLatency
	}
	data, err := json.Marshal(parts)
	if err != nil {
		return original
	}
	return data
}

func findFirstLessThan(rows []agentRow, targetUnix int64) int {
	lo, hi := 0, len(rows)
	for lo < hi {
		mid := (lo + hi) / 2
		if rows[mid].checkedAt < targetUnix {
			hi = mid
		} else {
			lo = mid + 1
		}
	}
	return lo
}
