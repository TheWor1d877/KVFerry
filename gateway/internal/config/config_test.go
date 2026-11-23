package config

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

func writeConfig(t *testing.T, body string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "gateway.yaml")
	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}
	return path
}

const twoUpstreams = `
listen: 127.0.0.1:9000
upstreams:
  - 127.0.0.1:8001
  - 127.0.0.1:8002
`

func TestConfigDefaults(t *testing.T) {
	path := writeConfig(t, `
upstreams:
  - 127.0.0.1:8001
`)
	cfg, err := Load(path, nil)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if cfg.Listen != ":9000" {
		t.Errorf("Listen = %q, want :9000", cfg.Listen)
	}
	if cfg.ProbeInterval != time.Second {
		t.Errorf("ProbeInterval = %v, want 1s", cfg.ProbeInterval)
	}
	if cfg.ProbeThreshold != 3 {
		t.Errorf("ProbeThreshold = %d, want 3", cfg.ProbeThreshold)
	}
	if cfg.ProbeTimeout != time.Second {
		t.Errorf("ProbeTimeout = %v, want 1s", cfg.ProbeTimeout)
	}
	if cfg.ProbePath != "/health" {
		t.Errorf("ProbePath = %q, want /health", cfg.ProbePath)
	}
	if len(cfg.Upstreams) != 1 || cfg.Upstreams[0] != "127.0.0.1:8001" {
		t.Errorf("Upstreams = %v, want [127.0.0.1:8001]", cfg.Upstreams)
	}
}

func TestConfigOverrideSingle(t *testing.T) {
	path := writeConfig(t, twoUpstreams)
	cfg, err := Load(path, []string{
		"listen=127.0.0.1:19000",
		"probe.interval=250ms",
		"probe.threshold=5",
		"probe.timeout=2s",
		"probe.path=/ready",
	})
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if cfg.Listen != "127.0.0.1:19000" {
		t.Errorf("Listen = %q, want 127.0.0.1:19000", cfg.Listen)
	}
	if cfg.ProbeInterval != 250*time.Millisecond {
		t.Errorf("ProbeInterval = %v, want 250ms", cfg.ProbeInterval)
	}
	if cfg.ProbeThreshold != 5 {
		t.Errorf("ProbeThreshold = %d, want 5", cfg.ProbeThreshold)
	}
	if cfg.ProbeTimeout != 2*time.Second {
		t.Errorf("ProbeTimeout = %v, want 2s", cfg.ProbeTimeout)
	}
	if cfg.ProbePath != "/ready" {
		t.Errorf("ProbePath = %q, want /ready", cfg.ProbePath)
	}
	if len(cfg.Upstreams) != 2 {
		t.Errorf("Upstreams = %v, want 2 entries", cfg.Upstreams)
	}
}

func TestConfigOverrideUpstreams(t *testing.T) {
	path := writeConfig(t, twoUpstreams)
	cfg, err := Load(path, []string{"upstreams=10.0.0.1:8001, 10.0.0.2:8002 ,10.0.0.3:8003"})
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	want := []string{"10.0.0.1:8001", "10.0.0.2:8002", "10.0.0.3:8003"}
	if len(cfg.Upstreams) != len(want) {
		t.Fatalf("Upstreams = %v, want %v", cfg.Upstreams, want)
	}
	for i := range want {
		if cfg.Upstreams[i] != want[i] {
			t.Errorf("Upstreams[%d] = %q, want %q", i, cfg.Upstreams[i], want[i])
		}
	}
}

func TestConfigMissingUpstreams(t *testing.T) {
	path := writeConfig(t, "listen: 127.0.0.1:9000\n")
	if _, err := Load(path, nil); err == nil {
		t.Fatal("Load succeeded without upstreams, want error")
	}

	path = writeConfig(t, twoUpstreams)
	if _, err := Load(path, []string{"upstreams= , "}); err == nil {
		t.Fatal("Load succeeded with empty upstreams override, want error")
	}
}

func TestConfigInvalidYAML(t *testing.T) {
	path := writeConfig(t, "upstreams: [\n")
	if _, err := Load(path, nil); err == nil {
		t.Fatal("Load succeeded on invalid YAML, want error")
	}
}

func TestConfigMissingFile(t *testing.T) {
	missing := filepath.Join(t.TempDir(), "absent.yaml")
	if _, err := Load(missing, nil); err == nil {
		t.Fatal("Load succeeded on missing file, want error")
	}
}

func TestConfigUnknownKey(t *testing.T) {
	path := writeConfig(t, twoUpstreams)
	if _, err := Load(path, []string{"probe.retries=2"}); err == nil {
		t.Fatal("Load succeeded on unknown override key, want error")
	}
}

func TestConfigThresholdNotPositive(t *testing.T) {
	path := writeConfig(t, twoUpstreams)
	for _, ov := range []string{"probe.threshold=0", "probe.threshold=-2"} {
		if _, err := Load(path, []string{ov}); err == nil {
			t.Fatalf("Load succeeded on %q, want error", ov)
		}
	}

	path = writeConfig(t, twoUpstreams+"\nprobe:\n  threshold: -1\n")
	if _, err := Load(path, nil); err == nil {
		t.Fatal("Load succeeded on negative probe.threshold, want error")
	}
}

func TestConfigInvalidDuration(t *testing.T) {
	path := writeConfig(t, twoUpstreams+"\nprobe:\n  interval: soon\n")
	if _, err := Load(path, nil); err == nil {
		t.Fatal("Load succeeded on invalid probe.interval, want error")
	}

	path = writeConfig(t, twoUpstreams)
	if _, err := Load(path, []string{"probe.timeout=forever"}); err == nil {
		t.Fatal("Load succeeded on invalid probe.timeout override, want error")
	}
}
func TestConfigYAMLThresholdExplicit(t *testing.T) {
	path := writeConfig(t, twoUpstreams+"\nprobe:\n  threshold: 0\n")
	if _, err := Load(path, nil); err == nil {
		t.Fatal("Load succeeded on explicit probe.threshold: 0, want error")
	}

	path = writeConfig(t, twoUpstreams)
	cfg, err := Load(path, nil)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if cfg.ProbeThreshold != 3 {
		t.Errorf("ProbeThreshold = %d, want default 3 when unset", cfg.ProbeThreshold)
	}

	path = writeConfig(t, twoUpstreams+"\nprobe:\n  threshold: 5\n")
	cfg, err = Load(path, nil)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if cfg.ProbeThreshold != 5 {
		t.Errorf("ProbeThreshold = %d, want 5", cfg.ProbeThreshold)
	}
}
