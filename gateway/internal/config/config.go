package config

import (
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"gopkg.in/yaml.v3"
)

// Config 是网关运行配置，字段口径见 D16 与 D21。
type Config struct {
	Listen         string
	Upstreams      []string
	ProbeInterval  time.Duration
	ProbeThreshold int
	ProbeTimeout   time.Duration
	ProbePath      string
}

type fileConfig struct {
	Listen    string          `yaml:"listen"`
	Upstreams []string        `yaml:"upstreams"`
	Probe     fileProbeConfig `yaml:"probe"`
}

type fileProbeConfig struct {
	Interval  string `yaml:"interval"`
	Threshold *int   `yaml:"threshold"`
	Timeout   string `yaml:"timeout"`
	Path      string `yaml:"path"`
}

func defaults() Config {
	return Config{
		Listen:         ":9000",
		ProbeInterval:  time.Second,
		ProbeThreshold: 3,
		ProbeTimeout:   time.Second,
		ProbePath:      "/health",
	}
}

// Load 解析 YAML 配置文件并套用命令行覆盖，全程不做网络访问（D16）。
func Load(path string, overrides []string) (Config, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return Config{}, fmt.Errorf("read config: %w", err)
	}

	var fc fileConfig
	if err := yaml.Unmarshal(raw, &fc); err != nil {
		return Config{}, fmt.Errorf("parse config: %w", err)
	}

	cfg := defaults()

	if fc.Listen != "" {
		cfg.Listen = fc.Listen
	}
	if len(fc.Upstreams) > 0 {
		cfg.Upstreams = append([]string(nil), fc.Upstreams...)
	}
	if fc.Probe.Interval != "" {
		d, err := time.ParseDuration(fc.Probe.Interval)
		if err != nil {
			return Config{}, fmt.Errorf("parse probe.interval: %w", err)
		}
		cfg.ProbeInterval = d
	}
	if fc.Probe.Threshold != nil {
		cfg.ProbeThreshold = *fc.Probe.Threshold
	}
	if fc.Probe.Timeout != "" {
		d, err := time.ParseDuration(fc.Probe.Timeout)
		if err != nil {
			return Config{}, fmt.Errorf("parse probe.timeout: %w", err)
		}
		cfg.ProbeTimeout = d
	}
	if fc.Probe.Path != "" {
		cfg.ProbePath = fc.Probe.Path
	}

	for _, ov := range overrides {
		if err := applyOverride(&cfg, ov); err != nil {
			return Config{}, err
		}
	}

	if len(cfg.Upstreams) == 0 {
		return Config{}, errors.New("upstreams is required")
	}
	if cfg.ProbeThreshold <= 0 {
		return Config{}, errors.New("probe.threshold must be greater than 0")
	}

	return cfg, nil
}

func applyOverride(cfg *Config, raw string) error {
	key, value, ok := strings.Cut(raw, "=")
	if !ok {
		return fmt.Errorf("invalid override %q: want key=value", raw)
	}
	key = strings.TrimSpace(key)
	value = strings.TrimSpace(value)

	switch key {
	case "listen":
		cfg.Listen = value
	case "upstreams":
		parts := strings.Split(value, ",")
		nodes := make([]string, 0, len(parts))
		for _, part := range parts {
			part = strings.TrimSpace(part)
			if part != "" {
				nodes = append(nodes, part)
			}
		}
		cfg.Upstreams = nodes
	case "probe.interval":
		d, err := time.ParseDuration(value)
		if err != nil {
			return fmt.Errorf("parse probe.interval: %w", err)
		}
		cfg.ProbeInterval = d
	case "probe.threshold":
		n, err := strconv.Atoi(value)
		if err != nil {
			return fmt.Errorf("parse probe.threshold: %w", err)
		}
		cfg.ProbeThreshold = n
	case "probe.timeout":
		d, err := time.ParseDuration(value)
		if err != nil {
			return fmt.Errorf("parse probe.timeout: %w", err)
		}
		cfg.ProbeTimeout = d
	case "probe.path":
		cfg.ProbePath = value
	default:
		return fmt.Errorf("unknown override key %q", key)
	}

	return nil
}
