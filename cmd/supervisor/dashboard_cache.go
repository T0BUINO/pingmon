package main

import (
	"strconv"
	"sync"

	"pingmon/internal/model"
)

var rowBufPool = sync.Pool{
	New: func() interface{} {
		return make([]byte, 0, 256)
	},
}

func marshalDashboardResult(result model.Result) ([]byte, error) {
	buf := rowBufPool.Get().([]byte)
	buf = buf[:0]
	defer rowBufPool.Put(buf)

	buf = append(buf, '[')
	buf = appendJSONString(buf, result.Agent)
	buf = append(buf, ',')
	buf = appendJSONString(buf, result.AgentIP)
	buf = append(buf, ',')
	buf = appendJSONString(buf, result.TargetName)
	buf = append(buf, ',')
	buf = appendJSONString(buf, result.Address)
	buf = append(buf, ',')
	buf = strconv.AppendInt(buf, int64(result.Port), 10)
	buf = append(buf, ',')
	buf = appendLabels(buf, result.Labels)
	buf = append(buf, ',')
	buf = appendJSONString(buf, strconv.FormatInt(result.CheckedAt.UTC().UnixNano(), 36))
	buf = append(buf, ',')
	buf = strconv.AppendInt(buf, int64(result.SuccessCount), 10)
	buf = append(buf, ',')
	buf = strconv.AppendInt(buf, int64(result.FailureCount), 10)
	buf = append(buf, ',')
	buf = strconv.AppendFloat(buf, result.AverageLatencyMS, 'f', -1, 64)
	buf = append(buf, ',')
	buf = strconv.AppendFloat(buf, result.SuccessRate, 'f', -1, 64)
	buf = append(buf, ',')
	buf = appendJSONString(buf, result.Error)
	buf = append(buf, ']')

	return append([]byte(nil), buf...), nil
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
