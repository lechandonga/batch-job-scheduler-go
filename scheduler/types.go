package scheduler

import (
	"fmt"
	"time"
)

// Resource describes a vector of consumable cluster capacity.
type Resource struct {
	CPU   int64 `json:"cpu"`
	MemMB int64 `json:"mem_mb"`
}

func (r Resource) Add(o Resource) Resource {
	return Resource{CPU: r.CPU + o.CPU, MemMB: r.MemMB + o.MemMB}
}
func (r Resource) Sub(o Resource) Resource {
	return Resource{CPU: r.CPU - o.CPU, MemMB: r.MemMB - o.MemMB}
}

// Fits reports whether r can be satisfied by avail.
func (r Resource) Fits(avail Resource) bool { return r.CPU <= avail.CPU && r.MemMB <= avail.MemMB }

func (r Resource) negative() bool { return r.CPU < 0 || r.MemMB < 0 }
func (r Resource) zero() bool     { return r.CPU == 0 && r.MemMB == 0 }

// RejectReason distinguishes why a submission or configuration was refused.
type RejectReason string

const (
	ReasonMissingField      RejectReason = "MISSING_FIELD"
	ReasonNegativeResource  RejectReason = "NEGATIVE_RESOURCE"
	ReasonZeroResource      RejectReason = "ZERO_RESOURCE"
	ReasonInvalidQuota      RejectReason = "INVALID_QUOTA"
	ReasonQuotaExceeded     RejectReason = "QUOTA_EXCEEDED"
	ReasonOversizedForFleet RejectReason = "OVERSIZED_FOR_FLEET"
	ReasonDuplicateJob      RejectReason = "DUPLICATE_JOB"
)

// RejectError is returned for every refused submission so callers can
// programmatically distinguish the cause.
type RejectError struct {
	Reason RejectReason
	Detail string
}

func (e *RejectError) Error() string { return fmt.Sprintf("%s: %s", e.Reason, e.Detail) }

// JobState is the lifecycle state of a job.
type JobState string

const (
	StateQueued    JobState = "QUEUED"
	StateRunning   JobState = "RUNNING"
	StateBackoff   JobState = "BACKOFF" // waiting for retry backoff to elapse
	StateSucceeded JobState = "SUCCEEDED"
	StateFailed    JobState = "FAILED" // retries exhausted
)

// Job is a unit of schedulable work.
type Job struct {
	ID          string    `json:"id"`
	TenantID    string    `json:"tenant_id"`
	Priority    int       `json:"priority"` // higher wins
	Request     Resource  `json:"request"`
	State       JobState  `json:"state"`
	Progress    float64   `json:"progress"` // 0..1, preserved across preemption
	Attempts    int       `json:"attempts"`
	NodeID      string    `json:"node_id,omitempty"`
	Submitted   time.Time `json:"submitted"`
	NotBefore   time.Time `json:"not_before,omitempty"`   // backoff gate
	PreemptedAt time.Time `json:"preempted_at,omitempty"` // last preemption time (cooldown)
	Preemptions int       `json:"preemptions"`
}

// Node is a schedulable resource partition of the local fleet.
type Node struct {
	ID       string   `json:"id"`
	Capacity Resource `json:"capacity"`
}

// Event is an explainability record: every scheduling decision emits one.
type Event struct {
	At     time.Time `json:"at"`
	Kind   string    `json:"kind"` // SUBMIT/REJECT/PLACE/PREEMPT/REQUEUE/FAIL/RETRY/EXHAUSTED/COMPLETE/RECOVER
	JobID  string    `json:"job_id,omitempty"`
	Detail string    `json:"detail"`
}

// RetryPolicy controls failure retries.
type RetryPolicy struct {
	MaxRetries     int
	InitialBackoff time.Duration
	MaxBackoff     time.Duration
}

// backoffFor returns the backoff delay before retry number n (1-based).
func (p RetryPolicy) backoffFor(n int) time.Duration {
	d := p.InitialBackoff
	for i := 1; i < n; i++ {
		d *= 2
		if d >= p.MaxBackoff {
			return p.MaxBackoff
		}
	}
	if d > p.MaxBackoff {
		return p.MaxBackoff
	}
	return d
}
