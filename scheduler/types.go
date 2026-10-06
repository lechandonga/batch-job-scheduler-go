package scheduler

import (
	"errors"
	"fmt"
	"time"
)

// Resources 描述一份可计量资源需求或容量。
type Resources struct {
	CPU   int `json:"cpu"`
	MemMB int `json:"mem_mb"`
}

func (r Resources) Negative() bool { return r.CPU < 0 || r.MemMB < 0 }
func (r Resources) Zero() bool     { return r.CPU == 0 && r.MemMB == 0 }

func (r Resources) Fits(used, cap Resources) bool {
	return used.CPU+r.CPU <= cap.CPU && used.MemMB+r.MemMB <= cap.MemMB
}

func (r Resources) Add(o Resources) Resources {
	return Resources{CPU: r.CPU + o.CPU, MemMB: r.MemMB + o.MemMB}
}

func (r Resources) Sub(o Resources) Resources {
	return Resources{CPU: r.CPU - o.CPU, MemMB: r.MemMB - o.MemMB}
}

// JobState 作业生命周期状态。
type JobState string

const (
	StateQueued    JobState = "QUEUED"
	StateRunning   JobState = "RUNNING"
	StateBackoff   JobState = "BACKOFF" // 等待重试退避
	StateSucceeded JobState = "SUCCEEDED"
	StateFailed    JobState = "FAILED" // 重试耗尽
)

// Job 一个批量作业。
type Job struct {
	ID         string    `json:"id"`
	Tenant     string    `json:"tenant"`
	Priority   int       `json:"priority"`
	Req        Resources `json:"req"`
	MaxRetries int       `json:"max_retries"`

	State      JobState  `json:"state"`
	Progress   int       `json:"progress"` // 已完成进度（百分比），抢占不丢失
	Retries    int       `json:"retries"`
	Preempted  int       `json:"preempted"`
	Seq        uint64    `json:"seq"`                  // 提交顺序，保证确定性
	NotBefore  time.Time `json:"not_before,omitempty"` // 退避可见时间
	CooldownRd int       `json:"cooldown_round"`       // 抢占保护到的调度轮次
}

// 可区分的提交拒绝原因。
var (
	ErrMissingField     = errors.New("missing required field")
	ErrNegativeResource = errors.New("negative resource request")
	ErrZeroResource     = errors.New("zero resource request")
	ErrUnknownTenant    = errors.New("unknown tenant / invalid quota")
	ErrDuplicateJob     = errors.New("duplicate job id")
)

// RejectError 携带可区分的拒绝原因。
type RejectError struct {
	Reason error
	Detail string
}

func (e *RejectError) Error() string { return fmt.Sprintf("rejected: %v: %s", e.Reason, e.Detail) }
func (e *RejectError) Unwrap() error { return e.Reason }
