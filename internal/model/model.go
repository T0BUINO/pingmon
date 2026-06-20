package model

import "time"

type PingTarget struct {
	Name    string   `json:"name"`
	Address string   `json:"address"`
	Port    int      `json:"port"`
	Labels  []string `json:"labels,omitempty"`
}

type PingParams struct {
	Count           int  `json:"count"`
	IntervalMillis  int  `json:"interval_millis"`
	TimeoutMillis   int  `json:"timeout_millis"`
	EnableIPv6      bool `json:"enable_ipv6"`
	ScheduleSeconds int  `json:"schedule_seconds"`
}

type Task struct {
	Target PingTarget `json:"target"`
	Params PingParams `json:"params"`
}

type Result struct {
	Agent            string    `json:"agent"`
	AgentIP          string    `json:"agent_ip,omitempty"`
	TargetName       string    `json:"target_name"`
	Address          string    `json:"address"`
	Port             int       `json:"port"`
	Labels           []string  `json:"labels,omitempty"`
	CheckedAt        time.Time `json:"checked_at"`
	SuccessCount     int       `json:"success_count"`
	FailureCount     int       `json:"failure_count"`
	AverageLatencyMS float64   `json:"average_latency_ms"`
	SuccessRate      float64   `json:"success_rate"`
	Error            string    `json:"error,omitempty"`
}

type AgentStatus struct {
	Agent       string    `json:"agent"`
	AgentIP     string    `json:"agent_ip,omitempty"`
	FirstSeenAt time.Time `json:"first_seen_at"`
	LastSeenAt  time.Time `json:"last_seen_at"`
}
