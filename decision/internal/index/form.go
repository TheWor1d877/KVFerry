package index

// Spec 是 KV 口径四项（D44）：块字节、KV 精度、每 token 字节数与对齐修正。
type Spec struct {
	BlockBytes    int64
	KVPrecision   string
	BytesPerToken int64
	AlignmentPad  int64
}

// Equal 比对两组口径逐项一致（D44）。
func (s Spec) Equal(other Spec) bool {
	return s.BlockBytes == other.BlockBytes &&
		s.KVPrecision == other.KVPrecision &&
		s.BytesPerToken == other.BytesPerToken &&
		s.AlignmentPad == other.AlignmentPad
}

// FormConfig 是引擎下发的形态配置（D53）：KV 形态、每 token 字节数、能否分块取回、
// 搬迁单位与 KV 口径。
type FormConfig struct {
	KVShape          string
	BytesPerToken    int64
	ChunkedRetrieval bool
	MigrationUnit    int64
	Spec             Spec
}

// Equal 比对两组形态配置逐项一致（D53）。
func (c FormConfig) Equal(other FormConfig) bool {
	return c.KVShape == other.KVShape &&
		c.BytesPerToken == other.BytesPerToken &&
		c.ChunkedRetrieval == other.ChunkedRetrieval &&
		c.MigrationUnit == other.MigrationUnit &&
		c.Spec.Equal(other.Spec)
}

// Form 是形态索引（D53）：随引擎配置下发，重启从配置重建，不进软状态。
type Form struct {
	cfg FormConfig
}

// BuildForm 从配置重建形态索引（D53）；同一份配置重建结果一致。
func BuildForm(cfg FormConfig) *Form {
	return &Form{cfg: cfg}
}

// Config 返回形态配置（D53）。
func (f *Form) Config() FormConfig {
	return f.cfg
}

// SameSpec 判定查询口径与形态口径一致；不一致时跨口径块按未命中（D53）。
func (f *Form) SameSpec(s Spec) bool {
	return f.cfg.Spec.Equal(s)
}
