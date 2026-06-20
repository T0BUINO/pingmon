package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"pingmon/internal/config"
	"pingmon/internal/model"
)

func main() {
	supervisor := flag.String("supervisor", "", "optional supervisor base URL override, for example http://127.0.0.1:8080")
	configPath := flag.String("config", "", "optional JSON or TOML agent config")
	format := flag.String("format", "", "config format: json or toml")
	once := flag.Bool("once", false, "run one fetch/probe/report cycle and exit")
	flag.Parse()
	cfg, err := config.LoadAgent(*configPath, *format)
	if err != nil {
		log.Fatalf("load agent config: %v", err)
	}
	if *supervisor != "" {
		cfg.SupervisorURL = *supervisor
	}
	if cfg.SupervisorURL == "" {
		log.Fatal("supervisor URL is required in -supervisor or agent config supervisor_url")
	}
	base, err := normalizeBaseURL(cfg.SupervisorURL)
	if err != nil {
		log.Fatal(err)
	}
	for {
		sleepSeconds, err := runCycle(base, cfg)
		if err != nil {
			log.Printf("cycle failed: %v", err)
			sleepSeconds = cfg.PollIntervalSeconds
		}
		if *once {
			return
		}
		time.Sleep(time.Duration(sleepSeconds) * time.Second)
	}
}

func runCycle(supervisor string, cfg config.AgentConfig) (int, error) {
	tasks, err := fetchTasks(supervisor)
	if err != nil {
		return cfg.PollIntervalSeconds, err
	}
	sleepSeconds := nextPollInterval(tasks, cfg.PollIntervalSeconds)
	agentIP := fetchAgentIP(cfg)
	var wg sync.WaitGroup
	results := make(chan model.Result, len(tasks))
	for _, task := range tasks {
		task := task
		wg.Add(1)
		go func() {
			defer wg.Done()
			results <- tcpPing(cfg.AgentName, task)
		}()
	}
	wg.Wait()
	close(results)
	batch := make([]model.Result, 0, len(tasks))
	for result := range results {
		result.AgentIP = agentIP
		batch = append(batch, result)
	}
	if len(batch) == 0 {
		return sleepSeconds, nil
	}
	return sleepSeconds, uploadResults(supervisor, batch)
}

func nextPollInterval(tasks []model.Task, fallback int) int {
	if fallback <= 0 {
		fallback = 30
	}
	for _, task := range tasks {
		if task.Params.ScheduleSeconds > 0 {
			return task.Params.ScheduleSeconds
		}
	}
	return fallback
}

func fetchTasks(supervisor string) ([]model.Task, error) {
	resp, err := http.Get(supervisor + "/api/tasks")
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("fetch tasks status: %s", resp.Status)
	}
	var tasks []model.Task
	if err := json.NewDecoder(resp.Body).Decode(&tasks); err != nil {
		return nil, err
	}
	return tasks, nil
}

func fetchAgentIP(cfg config.AgentConfig) string {
	if strings.TrimSpace(cfg.PublicIPURL) != "" {
		ip, err := fetchPublicIP(cfg.PublicIPURL)
		if err != nil {
			log.Printf("public ip lookup failed: %v", err)
			return ""
		}
		return ip
	}
	ipv4, _ := fetchPublicIP(cfg.PublicIPv4URL)
	ipv6, _ := fetchPublicIP(cfg.PublicIPv6URL)
	switch {
	case ipv4 != "" && ipv6 != "":
		return ipv4 + " / " + ipv6
	case ipv4 != "":
		return ipv4
	case ipv6 != "":
		return ipv6
	default:
		log.Printf("public ip lookup failed for both IPv4 and IPv6 endpoints")
		return ""
	}
}

func fetchPublicIP(endpoint string) (string, error) {
	endpoint = strings.TrimSpace(endpoint)
	if endpoint == "" {
		return "", nil
	}
	client := &http.Client{Timeout: 3 * time.Second}
	resp, err := client.Get(endpoint)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("%s returned %s", endpoint, resp.Status)
	}
	var buf bytes.Buffer
	if _, err := buf.ReadFrom(resp.Body); err != nil {
		return "", err
	}
	ip := strings.TrimSpace(buf.String())
	if parsed := net.ParseIP(ip); parsed == nil {
		return "", fmt.Errorf("%s returned invalid IP %q", endpoint, ip)
	}
	return ip, nil
}

func tcpPing(agent string, task model.Task) model.Result {
	params := task.Params
	if params.Count <= 0 {
		params.Count = 3
	}
	if params.IntervalMillis <= 0 {
		params.IntervalMillis = 1000
	}
	if params.TimeoutMillis <= 0 {
		params.TimeoutMillis = 2000
	}
	result := model.Result{
		Agent:      agent,
		TargetName: task.Target.Name,
		Address:    task.Target.Address,
		Port:       task.Target.Port,
		Labels:     task.Target.Labels,
		CheckedAt:  time.Now(),
	}
	address := net.JoinHostPort(task.Target.Address, fmt.Sprint(task.Target.Port))
	timeout := time.Duration(params.TimeoutMillis) * time.Millisecond
	interval := time.Duration(params.IntervalMillis) * time.Millisecond
	network := "tcp"
	if !params.EnableIPv6 {
		network = "tcp4"
	}
	var latencySum time.Duration
	for i := 0; i < params.Count; i++ {
		start := time.Now()
		conn, err := net.DialTimeout(network, address, timeout)
		if err != nil {
			result.FailureCount++
			result.Error = err.Error()
		} else {
			result.SuccessCount++
			latencySum += time.Since(start)
			_ = conn.Close()
		}
		if i < params.Count-1 {
			time.Sleep(interval)
		}
	}
	if result.SuccessCount > 0 {
		result.AverageLatencyMS = float64(latencySum.Microseconds()) / 1000.0 / float64(result.SuccessCount)
	}
	total := result.SuccessCount + result.FailureCount
	if total > 0 {
		result.SuccessRate = float64(result.SuccessCount) / float64(total)
	}
	return result
}

func uploadResults(supervisor string, results []model.Result) error {
	body, err := json.Marshal(results)
	if err != nil {
		return err
	}
	resp, err := http.Post(supervisor+"/api/report", "application/json", bytes.NewReader(body))
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("report status: %s", resp.Status)
	}
	return nil
}

func normalizeBaseURL(raw string) (string, error) {
	if !strings.HasPrefix(raw, "http://") && !strings.HasPrefix(raw, "https://") {
		raw = "http://" + raw
	}
	u, err := url.Parse(raw)
	if err != nil {
		return "", err
	}
	if u.Host == "" {
		return "", fmt.Errorf("invalid supervisor URL: %s", raw)
	}
	return strings.TrimRight(u.String(), "/"), nil
}
