package config

import (
	"fmt"
	"os"
	"strings"

	"gopkg.in/yaml.v3"
)

// Config 是存储节点运行配置，字段口径见 D42。
type Config struct {
	NodeID              string
	DecisionEndpoint    string
	DataDir             string
	CapacityBytes       int64
	EngineEndpoint      string
	Listen              string
	ReadCacheBytes      int64
	NVMEWatermarkPct    int
	HeartbeatIntervalS  int
	DownAfterS          int
	SaveRateRatio       float64
	PinQuotaBytes       int64
	PinTTLS             int64
	ManifestShardBlocks int
	RemoteNodes         []string
	RPCMaxMessageMB     int
	AuditIntervalS      int
	AuditSamplePct      int
	AuditMaxMbps        int
}

type fileConfig struct {
	NodeID              string   `yaml:"node_id"`
	DecisionEndpoint    string   `yaml:"decision_endpoint"`
	DataDir             string   `yaml:"data_dir"`
	CapacityBytes       *int64   `yaml:"capacity_bytes"`
	EngineEndpoint      string   `yaml:"engine_endpoint"`
	Listen              string   `yaml:"listen"`
	ReadCacheBytes      *int64   `yaml:"read_cache_bytes"`
	NVMEWatermarkPct    *int     `yaml:"nvme_watermark_pct"`
	HeartbeatIntervalS  *int     `yaml:"heartbeat_interval_s"`
	DownAfterS          *int     `yaml:"down_after_s"`
	SaveRateRatio       *float64 `yaml:"save_rate_ratio"`
	PinQuotaBytes       *int64   `yaml:"pin_quota_bytes"`
	PinTTLS             *int64   `yaml:"pin_ttl_s"`
	ManifestShardBlocks *int     `yaml:"manifest_shard_blocks"`
	RemoteNodes         []string `yaml:"remote_nodes"`
	RPCMaxMessageMB     *int     `yaml:"rpc_max_message_mb"`
	AuditIntervalS      *int     `yaml:"audit_interval_s"`
	AuditSamplePct      *int     `yaml:"audit_sample_pct"`
	AuditMaxMbps        *int     `yaml:"audit_max_mbps"`
}

func defaults() Config {
	return Config{
		Listen:              ":9100",
		ReadCacheBytes:      64 << 30,
		NVMEWatermarkPct:    90,
		HeartbeatIntervalS:  30,
		DownAfterS:          30,
		SaveRateRatio:       0.5,
		PinQuotaBytes:       0,
		PinTTLS:             86400,
		ManifestShardBlocks: 4096,
		RemoteNodes:         []string{},
		RPCMaxMessageMB:     10,
		AuditIntervalS:      3600,
		AuditSamplePct:      1,
		AuditMaxMbps:        128,
	}
}

// Load 解析 YAML 配置文件，五项必填缺失时返回列出全部缺失键名的错误（D42）。
func Load(path string) (Config, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return Config{}, fmt.Errorf("read config: %w", err)
	}

	var fc fileConfig
	if err := yaml.Unmarshal(raw, &fc); err != nil {
		return Config{}, fmt.Errorf("parse config: %w", err)
	}

	cfg := defaults()

	if fc.NodeID != "" {
		cfg.NodeID = fc.NodeID
	}
	if fc.DecisionEndpoint != "" {
		cfg.DecisionEndpoint = fc.DecisionEndpoint
	}
	if fc.DataDir != "" {
		cfg.DataDir = fc.DataDir
	}
	if fc.CapacityBytes != nil {
		cfg.CapacityBytes = *fc.CapacityBytes
	}
	if fc.EngineEndpoint != "" {
		cfg.EngineEndpoint = fc.EngineEndpoint
	}
	if fc.Listen != "" {
		cfg.Listen = fc.Listen
	}
	if fc.ReadCacheBytes != nil {
		cfg.ReadCacheBytes = *fc.ReadCacheBytes
	}
	if fc.NVMEWatermarkPct != nil {
		cfg.NVMEWatermarkPct = *fc.NVMEWatermarkPct
	}
	if fc.HeartbeatIntervalS != nil {
		cfg.HeartbeatIntervalS = *fc.HeartbeatIntervalS
	}
	if fc.DownAfterS != nil {
		cfg.DownAfterS = *fc.DownAfterS
	}
	if fc.SaveRateRatio != nil {
		cfg.SaveRateRatio = *fc.SaveRateRatio
	}
	if fc.PinQuotaBytes != nil {
		cfg.PinQuotaBytes = *fc.PinQuotaBytes
	}
	if fc.PinTTLS != nil {
		cfg.PinTTLS = *fc.PinTTLS
	}
	if fc.ManifestShardBlocks != nil {
		cfg.ManifestShardBlocks = *fc.ManifestShardBlocks
	}
	if len(fc.RemoteNodes) > 0 {
		cfg.RemoteNodes = append([]string(nil), fc.RemoteNodes...)
	}
	if fc.RPCMaxMessageMB != nil {
		cfg.RPCMaxMessageMB = *fc.RPCMaxMessageMB
	}
	if fc.AuditIntervalS != nil {
		cfg.AuditIntervalS = *fc.AuditIntervalS
	}
	if fc.AuditSamplePct != nil {
		cfg.AuditSamplePct = *fc.AuditSamplePct
	}
	if fc.AuditMaxMbps != nil {
		cfg.AuditMaxMbps = *fc.AuditMaxMbps
	}

	var missing []string
	if cfg.NodeID == "" {
		missing = append(missing, "node_id")
	}
	if cfg.DecisionEndpoint == "" {
		missing = append(missing, "decision_endpoint")
	}
	if cfg.DataDir == "" {
		missing = append(missing, "data_dir")
	}
	if cfg.CapacityBytes <= 0 {
		missing = append(missing, "capacity_bytes")
	}
	if cfg.EngineEndpoint == "" {
		missing = append(missing, "engine_endpoint")
	}
	if len(missing) > 0 {
		return Config{}, fmt.Errorf("missing required config keys: %s", strings.Join(missing, ", "))
	}

	if cfg.SaveRateRatio < 0 || cfg.SaveRateRatio > 1 {
		return Config{}, fmt.Errorf("save_rate_ratio must be in [0, 1], got %v", cfg.SaveRateRatio)
	}

	return cfg, nil
}
