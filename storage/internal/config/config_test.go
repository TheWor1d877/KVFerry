package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func writeConfig(t *testing.T, content string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "storage.yaml")
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatalf("write config: %v", err)
	}
	return path
}

const fullRequired = `
node_id: node-1
decision_endpoint: "localhost:9200"
data_dir: /tmp/kvferry-data
capacity_bytes: 1073741824
engine_endpoint: "localhost:9300"
`

func TestDefaults(t *testing.T) {
	cfg, err := Load(writeConfig(t, fullRequired))
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if cfg.Listen != ":9100" {
		t.Errorf("listen = %q, want :9100", cfg.Listen)
	}
	if cfg.ReadCacheBytes != 64<<30 {
		t.Errorf("read_cache_bytes = %d, want %d", cfg.ReadCacheBytes, int64(64<<30))
	}
	if cfg.NVMEWatermarkPct != 90 {
		t.Errorf("nvme_watermark_pct = %d, want 90", cfg.NVMEWatermarkPct)
	}
	if cfg.HeartbeatIntervalS != 30 {
		t.Errorf("heartbeat_interval_s = %d, want 30", cfg.HeartbeatIntervalS)
	}
	if cfg.DownAfterS != 30 {
		t.Errorf("down_after_s = %d, want 30", cfg.DownAfterS)
	}
	if cfg.SaveRateRatio != 0.5 {
		t.Errorf("save_rate_ratio = %v, want 0.5", cfg.SaveRateRatio)
	}
	if cfg.PinQuotaBytes != 0 {
		t.Errorf("pin_quota_bytes = %d, want 0", cfg.PinQuotaBytes)
	}
	if cfg.PinTTLS != 86400 {
		t.Errorf("pin_ttl_s = %d, want 86400", cfg.PinTTLS)
	}
	if cfg.ManifestShardBlocks != 4096 {
		t.Errorf("manifest_shard_blocks = %d, want 4096", cfg.ManifestShardBlocks)
	}
	if len(cfg.RemoteNodes) != 0 {
		t.Errorf("remote_nodes = %v, want empty", cfg.RemoteNodes)
	}
	if cfg.RPCMaxMessageMB != 10 {
		t.Errorf("rpc_max_message_mb = %d, want 10", cfg.RPCMaxMessageMB)
	}
	if cfg.AuditIntervalS != 3600 {
		t.Errorf("audit_interval_s = %d, want 3600", cfg.AuditIntervalS)
	}
	if cfg.AuditSamplePct != 1 {
		t.Errorf("audit_sample_pct = %d, want 1", cfg.AuditSamplePct)
	}
	if cfg.AuditMaxMbps != 128 {
		t.Errorf("audit_max_mbps = %d, want 128", cfg.AuditMaxMbps)
	}
}

func TestMissingRequired(t *testing.T) {
	tests := []struct {
		name    string
		content string
		want    []string
	}{
		{
			name:    "all missing",
			content: "listen: :9100\n",
			want:    []string{"node_id", "decision_endpoint", "data_dir", "capacity_bytes", "engine_endpoint"},
		},
		{
			name:    "node_id missing",
			content: "decision_endpoint: d\ndata_dir: /tmp\ncapacity_bytes: 1\nengine_endpoint: e\n",
			want:    []string{"node_id"},
		},
		{
			name:    "capacity_bytes missing",
			content: "node_id: n\ndecision_endpoint: d\ndata_dir: /tmp\nengine_endpoint: e\n",
			want:    []string{"capacity_bytes"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := Load(writeConfig(t, tt.content))
			if err == nil {
				t.Fatal("load should fail")
			}
			for _, key := range tt.want {
				if !strings.Contains(err.Error(), key) {
					t.Errorf("error %q should contain missing key %q", err.Error(), key)
				}
			}
		})
	}
}

func TestSaveRateRatioOutOfRange(t *testing.T) {
	for _, ratio := range []string{"-0.1", "1.1"} {
		content := fullRequired + "\nsave_rate_ratio: " + ratio + "\n"
		_, err := Load(writeConfig(t, content))
		if err == nil {
			t.Errorf("save_rate_ratio %s should be rejected", ratio)
		} else if !strings.Contains(err.Error(), "save_rate_ratio") {
			t.Errorf("error %q should mention save_rate_ratio", err.Error())
		}
	}
}

func TestReadCacheBytesZero(t *testing.T) {
	content := fullRequired + "\nread_cache_bytes: 0\n"
	cfg, err := Load(writeConfig(t, content))
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if cfg.ReadCacheBytes != 0 {
		t.Errorf("read_cache_bytes = %d, want 0 (explicit zero disables cache)", cfg.ReadCacheBytes)
	}
}
