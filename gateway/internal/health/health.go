package health

import (
	"io"
	"net/http"
)

// Handler 服务 GET /health，进程存活即回 200 与纯文本 ok（D23）。
func Handler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		w.WriteHeader(http.StatusOK)
		_, _ = io.WriteString(w, "ok")
	})
}
