package drill

import (
	"encoding/json"
	"net/http"
)

// Handler 服务 GET /drill-events，拉取即清空（D22）。
func Handler(b *Buffer) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(b.Snapshot())
	})
}
