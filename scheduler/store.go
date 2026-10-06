package scheduler

import (
	"encoding/json"
	"os"
)

// snapshot 是调度器全部可恢复状态。
type snapshot struct {
	Capacity Resources            `json:"capacity"`
	Quotas   map[string]Resources `json:"quotas"`
	Jobs     []*Job               `json:"jobs"`
	Seq      uint64               `json:"seq"`
	Round    int                  `json:"round"`
}

// saveSnapshot 原子写入快照（先写临时文件再 rename）。
func (s *Scheduler) saveSnapshot() error {
	if s.storePath == "" {
		return nil
	}
	snap := snapshot{
		Capacity: s.capacity,
		Quotas:   s.quotas,
		Jobs:     make([]*Job, 0, len(s.jobs)),
		Seq:      s.seq,
		Round:    s.round,
	}
	for _, j := range s.jobs {
		snap.Jobs = append(snap.Jobs, j)
	}
	data, err := json.MarshalIndent(snap, "", "  ")
	if err != nil {
		return err
	}
	tmp := s.storePath + ".tmp"
	if err := os.WriteFile(tmp, data, 0o644); err != nil {
		return err
	}
	return os.Rename(tmp, s.storePath)
}

// loadSnapshot 从磁盘恢复调度器状态。
func loadSnapshot(path string) (*snapshot, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var snap snapshot
	if err := json.Unmarshal(data, &snap); err != nil {
		return nil, err
	}
	return &snap, nil
}
