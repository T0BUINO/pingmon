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

var supervisorHTTPClient = &http.Client{Timeout: 10 * time.Second}
var publicIPHTTPClient = &http.Client{Timeout: 3 * time.Second}

const agentIPCacheTTL = 15 * time.Minute

var agentIPCache struct {
	sync.Mutex
	ipv4URL   string
	ipv6URL   string
	value     string
	expiresAt time.Time
}

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
	agentIP := fetchAgentIP(cfg)
	tasks, err := fetchTasks(supervisor, cfg.AgentName, agentIP)
	if err != nil {
		return cfg.PollIntervalSeconds, err
	}
	sleepSeconds := nextPollInterval(tasks, cfg.PollIntervalSeconds)
	batch := probeTasks(cfg, tasks, agentIP, tcpPing)
	if len(batch) == 0 {
		return sleepSeconds, nil
	}
	return sleepSeconds, uploadResults(supervisor, batch)
}

func probeTasks(cfg config.AgentConfig, tasks []model.Task, agentIP string, probe func(string, model.Task) model.Result) []model.Result {
	concurrency := cfg.ProbeConcurrency
	if concurrency <= 0 {
		concurrency = 20
	}
	if concurrency > len(tasks) {
		concurrency = len(tasks)
	}
	if concurrency == 0 {
		return nil
	}
	jobs := make(chan model.Task)
	results := make(chan model.Result, len(tasks))
	var wg sync.WaitGroup
	for range concurrency {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for task := range jobs {
				result := probe(cfg.AgentName, task)
				result.AgentIP = agentIP
				results <- result
			}
		}()
	}
	go func() {
		for _, task := range tasks {
			jobs <- task
		}
		close(jobs)
		wg.Wait()
		close(results)
	}()
	batch := make([]model.Result, 0, len(tasks))
	for result := range results {
		batch = append(batch, result)
	}
	return batch
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

func fetchTasks(supervisor, agent, agentIP string) ([]model.Task, error) {
	taskURL, err := url.Parse(supervisor + "/api/tasks")
	if err != nil {
		return nil, err
	}
	query := taskURL.Query()
	if agent != "" {
		query.Set("agent", agent)
	}
	if agentIP != "" {
		query.Set("agent_ip", agentIP)
	}
	taskURL.RawQuery = query.Encode()
	resp, err := supervisorHTTPClient.Get(taskURL.String())
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
	now := time.Now()
	agentIPCache.Lock()
	if agentIPCache.ipv4URL == cfg.PublicIPv4URL && agentIPCache.ipv6URL == cfg.PublicIPv6URL &&
		agentIPCache.value != "" && now.Before(agentIPCache.expiresAt) {
		value := agentIPCache.value
		agentIPCache.Unlock()
		return value
	}
	stale := ""
	if agentIPCache.ipv4URL == cfg.PublicIPv4URL && agentIPCache.ipv6URL == cfg.PublicIPv6URL {
		stale = agentIPCache.value
	}
	agentIPCache.Unlock()

	type lookup struct {
		version int
		ip      string
	}
	results := make(chan lookup, 2)
	for version, endpoint := range map[int]string{4: cfg.PublicIPv4URL, 6: cfg.PublicIPv6URL} {
		go func() {
			ip, _ := fetchPublicIP(endpoint)
			results <- lookup{version: version, ip: ip}
		}()
	}
	var ipv4, ipv6 string
	for range 2 {
		result := <-results
		if result.version == 4 {
			ipv4 = result.ip
		} else {
			ipv6 = result.ip
		}
	}
	value := ""
	switch {
	case ipv4 != "" && ipv6 != "":
		value = ipv4 + " / " + ipv6
	case ipv4 != "":
		value = ipv4
	case ipv6 != "":
		value = ipv6
	default:
		log.Printf("public ip lookup failed for both IPv4 and IPv6 endpoints")
		if stale != "" {
			agentIPCache.Lock()
			agentIPCache.expiresAt = now.Add(agentIPCacheTTL)
			agentIPCache.Unlock()
		}
		return stale
	}
	agentIPCache.Lock()
	agentIPCache.ipv4URL = cfg.PublicIPv4URL
	agentIPCache.ipv6URL = cfg.PublicIPv6URL
	agentIPCache.value = value
	agentIPCache.expiresAt = now.Add(agentIPCacheTTL)
	agentIPCache.Unlock()
	return value
}

func fetchPublicIP(endpoint string) (string, error) {
	endpoint = strings.TrimSpace(endpoint)
	if endpoint == "" {
		return "", nil
	}
	resp, err := publicIPHTTPClient.Get(endpoint)
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
	resp, err := supervisorHTTPClient.Post(supervisor+"/api/report", "application/json", bytes.NewReader(body))
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
