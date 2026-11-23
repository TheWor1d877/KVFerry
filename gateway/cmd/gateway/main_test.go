package main

import (
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

const mainTestTwoUpstreams = `
listen: 127.0.0.1:9000
upstreams:
  - 127.0.0.1:8001
  - 127.0.0.1:8002
`

func writeMainConfig(t *testing.T, body string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "gateway.yaml")
	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}
	return path
}

func TestLoadConfigRequiresConfigFlag(t *testing.T) {
	for _, args := range [][]string{nil, {"--set", "listen=127.0.0.1:19000"}} {
		if _, err := loadConfig(args); err == nil || !strings.Contains(err.Error(), "--config is required") {
			t.Fatalf("loadConfig(%v) err = %v, want --config is required", args, err)
		}
	}
}

func TestLoadConfigRejectsBadYAML(t *testing.T) {
	path := writeMainConfig(t, "upstreams: [\n")
	if _, err := loadConfig([]string{"--config", path}); err == nil {
		t.Fatal("loadConfig succeeded on invalid YAML, want error")
	}
}

func TestLoadConfigRejectsMissingUpstreams(t *testing.T) {
	path := writeMainConfig(t, "listen: 127.0.0.1:9000\n")
	if _, err := loadConfig([]string{"--config", path}); err == nil {
		t.Fatal("loadConfig succeeded without upstreams, want error")
	}
}

func TestLoadConfigSetOverrides(t *testing.T) {
	path := writeMainConfig(t, mainTestTwoUpstreams)
	cfg, err := loadConfig([]string{
		"--config", path,
		"--set", "listen=127.0.0.1:18080",
		"--set", "probe.interval=250ms",
	})
	if err != nil {
		t.Fatalf("loadConfig: %v", err)
	}
	if cfg.Listen != "127.0.0.1:18080" {
		t.Errorf("Listen = %q, want 127.0.0.1:18080", cfg.Listen)
	}
	if cfg.ProbeInterval != 250*time.Millisecond {
		t.Errorf("ProbeInterval = %v, want 250ms", cfg.ProbeInterval)
	}
	if len(cfg.Upstreams) != 2 {
		t.Errorf("Upstreams = %v, want the two file entries", cfg.Upstreams)
	}
}

func TestLoadConfigSetUpstreamsCommaList(t *testing.T) {
	path := writeMainConfig(t, mainTestTwoUpstreams)
	cfg, err := loadConfig([]string{"--config", path, "--set", "upstreams=10.0.0.1:8001, 10.0.0.2:8002"})
	if err != nil {
		t.Fatalf("loadConfig: %v", err)
	}
	want := []string{"10.0.0.1:8001", "10.0.0.2:8002"}
	if len(cfg.Upstreams) != len(want) {
		t.Fatalf("Upstreams = %v, want %v", cfg.Upstreams, want)
	}
	for i := range want {
		if cfg.Upstreams[i] != want[i] {
			t.Errorf("Upstreams[%d] = %q, want %q", i, cfg.Upstreams[i], want[i])
		}
	}
}

func TestLoadConfigRejectsUnknownSetKey(t *testing.T) {
	path := writeMainConfig(t, mainTestTwoUpstreams)
	if _, err := loadConfig([]string{"--config", path, "--set", "probe.retries=2"}); err == nil {
		t.Fatal("loadConfig succeeded on unknown override key, want error")
	}
}

// captureRunStderr 把 os.Stderr 换成管道，返回读取函数；读取函数关闭写端后取回全部输出。
func captureRunStderr(t *testing.T) func() string {
	t.Helper()
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatalf("os.Pipe: %v", err)
	}
	previous := os.Stderr
	os.Stderr = w
	t.Cleanup(func() {
		os.Stderr = previous
		_ = w.Close()
		_ = r.Close()
	})

	var once sync.Once
	var output string
	return func() string {
		once.Do(func() {
			_ = w.Close()
			raw, _ := io.ReadAll(r)
			_ = r.Close()
			output = string(raw)
		})
		return output
	}
}

// TestRunConfigErrorsExitCode1 验证缺 --config 与配置加载失败都在进入监听之前以退出码 1 结束。
func TestRunConfigErrorsExitCode1(t *testing.T) {
	badYAML := writeMainConfig(t, "upstreams: [\n")
	missing := filepath.Join(t.TempDir(), "absent.yaml")

	cases := []struct {
		name string
		args []string
	}{
		{"missing config flag", nil},
		{"bad yaml", []string{"--config", badYAML}},
		{"missing config file", []string{"--config", missing}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			read := captureRunStderr(t)
			if code := run(tc.args); code != 1 {
				t.Fatalf("run(%v) = %d, want 1", tc.args, code)
			}
			if out := read(); strings.TrimSpace(out) == "" {
				t.Errorf("stderr empty, want diagnostic for %v", tc.args)
			}
		})
	}
}

// TestRunUnknownFlagExitCode2 验证非法 flag 沿用 flag 包语义：stderr 有错误与 usage，退出码 2。
func TestRunUnknownFlagExitCode2(t *testing.T) {
	read := captureRunStderr(t)
	if code := run([]string{"--bad"}); code != 2 {
		t.Fatalf("run(--bad) = %d, want 2", code)
	}
	out := read()
	if !strings.Contains(out, "flag provided but not defined") {
		t.Errorf("stderr = %q, want flag parse error", out)
	}
	if !strings.Contains(out, "Usage of gateway") {
		t.Errorf("stderr = %q, want usage text", out)
	}
}
