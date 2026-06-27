package config

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"pingmon/internal/model"
)

type Config struct {
	Listen              string             `json:"listen"`
	SQLitePath          string             `json:"sqlite_path"`
	DashboardUser       string             `json:"dashboard_user"`
	DashboardPassword   string             `json:"dashboard_password"`
	DashboardRanges     []string           `json:"dashboard_ranges"`
	DefaultRange        string             `json:"default_range"`
	RetentionDays       int                `json:"retention_days"`
	RawRetentionDays    int                `json:"raw_retention_days"`
	RollupIntervalMins  int                `json:"rollup_interval_minutes"`
	FailureThreshold    int                `json:"failure_threshold"`
	TaskIntervalSeconds int                `json:"task_interval_seconds"`
	Params              model.PingParams   `json:"params"`
	Targets             []model.PingTarget `json:"targets"`
}

type AgentConfig struct {
	SupervisorURL       string `json:"supervisor_url"`
	AgentName           string `json:"agent_name"`
	PollIntervalSeconds int    `json:"poll_interval_seconds"`
	ProbeConcurrency    int    `json:"probe_concurrency"`
	PublicIPv4URL       string `json:"public_ipv4_url"`
	PublicIPv6URL       string `json:"public_ipv6_url"`
}

func DefaultConfig() Config {
	return Config{
		Listen:              ":8080",
		SQLitePath:          "data/pingmon.db",
		DashboardUser:       "admin",
		DashboardPassword:   "admin",
		DashboardRanges:     []string{"5m", "15m", "30m", "12h", "24h", "3d", "7d", "14d", "30d", "60d", "180d", "365d"},
		DefaultRange:        "24h",
		RetentionDays:       365,
		RawRetentionDays:    30,
		RollupIntervalMins:  60,
		FailureThreshold:    3,
		TaskIntervalSeconds: 30,
		Params: model.PingParams{
			Count:           3,
			IntervalMillis:  1000,
			TimeoutMillis:   2000,
			EnableIPv6:      true,
			ScheduleSeconds: 30,
		},
	}
}

func DefaultAgentConfig() AgentConfig {
	host, err := os.Hostname()
	if err != nil || host == "" {
		host = "agent"
	}
	return AgentConfig{
		SupervisorURL:       "http://127.0.0.1:8080",
		AgentName:           host,
		PollIntervalSeconds: 30,
		ProbeConcurrency:    20,
		PublicIPv4URL:       "https://api-ipv4.ip.sb/ip",
		PublicIPv6URL:       "https://api-ipv6.ip.sb/ip",
	}
}

func Load(path, format string) (Config, error) {
	cfg := DefaultConfig()
	if path == "" {
		return cfg, nil
	}
	format = detectFormat(path, format)
	b, err := os.ReadFile(path)
	if err != nil {
		return cfg, err
	}
	switch format {
	case "json":
		err = json.Unmarshal(b, &cfg)
	case "toml":
		err = parseSupervisorTOML(string(b), &cfg)
	default:
		err = fmt.Errorf("unsupported config format %q", format)
	}
	if err != nil {
		return cfg, err
	}
	applyDefaults(&cfg)
	return cfg, nil
}

func LoadAgent(path, format string) (AgentConfig, error) {
	cfg := DefaultAgentConfig()
	if path == "" {
		return cfg, nil
	}
	format = detectFormat(path, format)
	b, err := os.ReadFile(path)
	if err != nil {
		return cfg, err
	}
	switch format {
	case "json":
		err = json.Unmarshal(b, &cfg)
	case "toml":
		err = parseAgentTOML(string(b), &cfg)
	default:
		err = fmt.Errorf("unsupported config format %q", format)
	}
	if err != nil {
		return cfg, err
	}
	if cfg.PollIntervalSeconds <= 0 {
		cfg.PollIntervalSeconds = 30
	}
	if cfg.ProbeConcurrency <= 0 {
		cfg.ProbeConcurrency = 20
	}
	if cfg.AgentName == "" {
		cfg.AgentName = DefaultAgentConfig().AgentName
	}
	if cfg.SupervisorURL == "" {
		cfg.SupervisorURL = DefaultAgentConfig().SupervisorURL
	}
	def := DefaultAgentConfig()
	if cfg.PublicIPv4URL == "" && cfg.PublicIPv6URL == "" {
		cfg.PublicIPv4URL = def.PublicIPv4URL
		cfg.PublicIPv6URL = def.PublicIPv6URL
	}
	return cfg, nil
}

func detectFormat(path, format string) string {
	if format != "" {
		return strings.ToLower(format)
	}
	ext := strings.ToLower(filepath.Ext(path))
	switch ext {
	case ".json":
		return "json"
	case ".toml":
		return "toml"
	default:
		return "json"
	}
}

func applyDefaults(cfg *Config) {
	def := DefaultConfig()
	if cfg.Listen == "" {
		cfg.Listen = def.Listen
	}
	if cfg.SQLitePath == "" {
		cfg.SQLitePath = def.SQLitePath
	}
	if cfg.DashboardUser == "" {
		cfg.DashboardUser = def.DashboardUser
	}
	if cfg.DashboardPassword == "" {
		cfg.DashboardPassword = def.DashboardPassword
	}
	if len(cfg.DashboardRanges) == 0 {
		cfg.DashboardRanges = def.DashboardRanges
	}
	if cfg.DefaultRange == "" {
		cfg.DefaultRange = def.DefaultRange
	}
	if cfg.RetentionDays <= 0 {
		cfg.RetentionDays = def.RetentionDays
	}
	if cfg.RawRetentionDays <= 0 {
		cfg.RawRetentionDays = def.RawRetentionDays
	}
	if cfg.RawRetentionDays > cfg.RetentionDays {
		cfg.RawRetentionDays = cfg.RetentionDays
	}
	if cfg.RollupIntervalMins <= 0 {
		cfg.RollupIntervalMins = def.RollupIntervalMins
	}
	if cfg.FailureThreshold <= 0 {
		cfg.FailureThreshold = def.FailureThreshold
	}
	if cfg.TaskIntervalSeconds <= 0 {
		cfg.TaskIntervalSeconds = def.TaskIntervalSeconds
	}
	if cfg.Params.Count <= 0 {
		cfg.Params.Count = def.Params.Count
	}
	if cfg.Params.IntervalMillis <= 0 {
		cfg.Params.IntervalMillis = def.Params.IntervalMillis
	}
	if cfg.Params.TimeoutMillis <= 0 {
		cfg.Params.TimeoutMillis = def.Params.TimeoutMillis
	}
	if cfg.Params.ScheduleSeconds <= 0 {
		cfg.Params.ScheduleSeconds = cfg.TaskIntervalSeconds
	}
}

func parseSupervisorTOML(input string, cfg *Config) error {
	section := ""
	var currentTarget *model.PingTarget
	scanner := bufio.NewScanner(strings.NewReader(input))
	lineNumber := 0
	for scanner.Scan() {
		lineNumber++
		line := stripComment(scanner.Text())
		if line == "" {
			continue
		}
		if line == "[params]" {
			section = "params"
			currentTarget = nil
			continue
		}
		if line == "[[targets]]" {
			section = "targets"
			cfg.Targets = append(cfg.Targets, model.PingTarget{})
			currentTarget = &cfg.Targets[len(cfg.Targets)-1]
			continue
		}
		key, value, ok := strings.Cut(line, "=")
		if !ok {
			return fmt.Errorf("line %d: invalid TOML: %s", lineNumber, line)
		}
		key = strings.TrimSpace(key)
		value = strings.TrimSpace(value)
		switch section {
		case "params":
			if err := setParamsValue(&cfg.Params, key, value); err != nil {
				return fmt.Errorf("line %d: %w", lineNumber, err)
			}
		case "targets":
			if currentTarget == nil {
				return fmt.Errorf("line %d: %w", lineNumber, errors.New("target key found before [[targets]]"))
			}
			if err := setTargetValue(currentTarget, key, value); err != nil {
				return fmt.Errorf("line %d: %w", lineNumber, err)
			}
		default:
			if err := setConfigValue(cfg, key, value); err != nil {
				return fmt.Errorf("line %d: %w", lineNumber, err)
			}
		}
	}
	return scanner.Err()
}

func parseAgentTOML(input string, cfg *AgentConfig) error {
	scanner := bufio.NewScanner(strings.NewReader(input))
	lineNumber := 0
	for scanner.Scan() {
		lineNumber++
		line := stripComment(scanner.Text())
		if line == "" || strings.HasPrefix(line, "[") {
			continue
		}
		key, value, ok := strings.Cut(line, "=")
		if !ok {
			return fmt.Errorf("line %d: invalid TOML: %s", lineNumber, line)
		}
		key = strings.TrimSpace(key)
		value = strings.TrimSpace(value)
		switch key {
		case "supervisor_url":
			cfg.SupervisorURL = parseString(value)
		case "agent_name":
			cfg.AgentName = parseString(value)
		case "poll_interval_seconds":
			n, err := parseInt(key, value)
			if err != nil {
				return fmt.Errorf("line %d: %w", lineNumber, err)
			}
			cfg.PollIntervalSeconds = n
		case "probe_concurrency":
			n, err := parseInt(key, value)
			if err != nil {
				return fmt.Errorf("line %d: %w", lineNumber, err)
			}
			cfg.ProbeConcurrency = n
		case "public_ipv4_url":
			cfg.PublicIPv4URL = parseString(value)
		case "public_ipv6_url":
			cfg.PublicIPv6URL = parseString(value)
		}
	}
	return scanner.Err()
}

func setConfigValue(cfg *Config, key, value string) error {
	setInt := func(dst *int) error {
		n, err := parseInt(key, value)
		if err != nil {
			return err
		}
		*dst = n
		return nil
	}
	switch key {
	case "listen":
		cfg.Listen = parseString(value)
	case "sqlite_path":
		cfg.SQLitePath = parseString(value)
	case "dashboard_user":
		cfg.DashboardUser = parseString(value)
	case "dashboard_password":
		cfg.DashboardPassword = parseString(value)
	case "dashboard_ranges":
		cfg.DashboardRanges = parseStringArray(value)
	case "default_range":
		cfg.DefaultRange = parseString(value)
	case "retention_days":
		return setInt(&cfg.RetentionDays)
	case "raw_retention_days":
		return setInt(&cfg.RawRetentionDays)
	case "rollup_interval_minutes":
		return setInt(&cfg.RollupIntervalMins)
	case "failure_threshold":
		return setInt(&cfg.FailureThreshold)
	case "task_interval_seconds":
		return setInt(&cfg.TaskIntervalSeconds)
	default:
		return fmt.Errorf("unknown config key %q", key)
	}
	return nil
}

func setParamsValue(params *model.PingParams, key, value string) error {
	setInt := func(dst *int) error {
		n, err := parseInt(key, value)
		if err != nil {
			return err
		}
		*dst = n
		return nil
	}
	switch key {
	case "count":
		return setInt(&params.Count)
	case "interval_millis":
		return setInt(&params.IntervalMillis)
	case "timeout_millis":
		return setInt(&params.TimeoutMillis)
	case "enable_ipv6":
		params.EnableIPv6 = parseBool(value)
	case "schedule_seconds":
		return setInt(&params.ScheduleSeconds)
	default:
		return fmt.Errorf("unknown params key %q", key)
	}
	return nil
}

func setTargetValue(target *model.PingTarget, key, value string) error {
	switch key {
	case "name":
		target.Name = parseString(value)
	case "address":
		target.Address = parseString(value)
	case "port":
		n, err := parseInt(key, value)
		if err != nil {
			return err
		}
		target.Port = n
	case "labels":
		target.Labels = parseStringArray(value)
	default:
		return fmt.Errorf("unknown target key %q", key)
	}
	return nil
}

func stripComment(line string) string {
	line = strings.TrimSpace(line)
	inString := false
	for i, r := range line {
		if r == '"' {
			inString = !inString
		}
		if r == '#' && !inString {
			return strings.TrimSpace(line[:i])
		}
	}
	return line
}

func parseString(value string) string {
	value = strings.TrimSpace(value)
	value = strings.Trim(value, `"`)
	return value
}

func parseStringArray(value string) []string {
	value = strings.TrimSpace(value)
	value = strings.TrimPrefix(value, "[")
	value = strings.TrimSuffix(value, "]")
	if strings.TrimSpace(value) == "" {
		return nil
	}
	parts := strings.Split(value, ",")
	out := make([]string, 0, len(parts))
	for _, part := range parts {
		out = append(out, parseString(strings.TrimSpace(part)))
	}
	return out
}

func parseBool(value string) bool {
	return strings.EqualFold(strings.TrimSpace(value), "true")
}

func parseInt(key, value string) (int, error) {
	n, err := strconv.Atoi(strings.TrimSpace(value))
	if err != nil {
		return 0, fmt.Errorf("invalid integer for %s: %q", key, strings.TrimSpace(value))
	}
	return n, nil
}
