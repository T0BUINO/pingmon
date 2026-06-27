package main

// Dashboard result streaming and bounded chart aggregation.

import (
	"bytes"
	"sort"
	"strconv"
	"time"

	"pingmon/internal/model"
)

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
	result    model.Result
	seriesKey string
	latency   float64
}

func newAgentRow(result model.Result) (agentRow, error) {
	line, err := marshalDashboardResult(result)
	if err != nil {
		return agentRow{}, err
	}
	return agentRow{
		checkedAt: result.CheckedAt.UnixNano(),
		data:      line,
		result:    result,
		seriesKey: result.Agent + "\x00" + result.TargetName + "\x00" + result.Address + "\x00" + strconv.Itoa(result.Port),
		latency:   result.AverageLatencyMS,
	}, nil
}

func (s *server) dashboardResultsData(since time.Time, agent string) ([]byte, error) {
	rows := make([]agentRow, 0, maxChartPoints*2)
	if err := s.streamResultsSince(since, agent, func(result model.Result) error {
		row, err := newAgentRow(result)
		if err != nil {
			return err
		}
		rows = append(rows, row)
		if len(rows) >= maxChartPoints*2 {
			rows = aggregateRowsByTime(rows, since, maxChartPoints)
		}
		return nil
	}); err != nil {
		return nil, err
	}
	rows = aggregateRowsByTime(rows, since, maxChartPoints)
	return marshalRows(rows), nil
}

func marshalRows(rows []agentRow) []byte {
	var buf bytes.Buffer
	buf.Grow(len(rows)*128 + 2)
	buf.WriteByte('[')
	for i := range rows {
		if i > 0 {
			buf.WriteByte(',')
		}
		buf.Write(rows[i].data)
	}
	buf.WriteByte(']')
	return buf.Bytes()
}

func aggregateRowsByTime(rows []agentRow, since time.Time, targetCount int) []agentRow {
	n := len(rows)
	if n <= targetCount || targetCount < 1 {
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
		key := row.seriesKey
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
	return result
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

		var weightedLatency float64
		var successCount, failureCount int
		var sampleCount int
		var problem bool
		var template model.Result
		var fallback agentRow
		var hasFallback bool
		var bucketNewest int64
		var bucketOldest int64

		bucketIdx := startIdx
		for bucketIdx < n && rows[bucketIdx].checkedAt >= bucketStart {
			if sampleCount == 0 {
				template = rows[bucketIdx].result
			}
			result := rows[bucketIdx].result
			weightedLatency += rows[bucketIdx].latency * float64(result.SuccessCount)
			successCount += result.SuccessCount
			failureCount += result.FailureCount
			problem = problem || result.FailureCount > 0 || result.Error != ""
			sampleCount++
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

		if sampleCount > 0 {
			checkedAt := bucketOldest + (bucketNewest-bucketOldest)/2
			template.CheckedAt = time.Unix(0, checkedAt).UTC()
			template.SuccessCount = successCount
			template.FailureCount = failureCount
			if successCount > 0 {
				template.AverageLatencyMS = weightedLatency / float64(successCount)
			} else {
				template.AverageLatencyMS = 0
			}
			total := successCount + failureCount
			if total > 0 {
				template.SuccessRate = float64(successCount) / float64(total)
			}
			if problem {
				template.Error = "aggregated interval contains failures"
			} else {
				template.Error = ""
			}
			row, err := newAgentRow(template)
			if err == nil {
				result = append(result, row)
			}
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
