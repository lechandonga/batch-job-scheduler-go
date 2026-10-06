package scheduler

import (
	"fmt"
	"sort"
	"sync"
	"time"
)

// Config wires a Scheduler instance.
type Config struct {
	Nodes           []Node
	Quotas          map[string]Resource // per-tenant admission quota
	Retry           RetryPolicy
	AgingInterval   time.Duration // queued wait per +1 effective priority
	MaxAgingBoost   int           // cap on aging boost
	PreemptCooldown time.Duration // a preempted job is immune for this long
	SnapshotPath    string        // optional JSON snapshot for restart recovery
	Now             func() time.Time
}

// Scheduler is a local batch scheduling engine over a resource ledger.
// All methods are safe for concurrent use; a single mutex serializes
// submissions and scheduling rounds so resources can never be oversold.
type Scheduler struct {
	mu     sync.Mutex
	cfg    Config
	jobs   map[string]*Job
	used   map[string]Resource // nodeID -> allocated
	admit  map[string]Resource // tenantID -> admitted (queued+running+backoff)
	events []Event
	store  *Store
}

// NewScheduler validates the fleet and quota configuration, then recovers
// any snapshot left by a previous incarnation.
func NewScheduler(cfg Config) (*Scheduler, error) {
	if cfg.Now == nil {
		cfg.Now = time.Now
	}
	if cfg.MaxAgingBoost == 0 {
		cfg.MaxAgingBoost = 5
	}
	if cfg.AgingInterval == 0 {
		cfg.AgingInterval = time.Minute
	}
	seen := map[string]bool{}
	for _, n := range cfg.Nodes {
		if n.ID == "" {
			return nil, &RejectError{ReasonMissingField, "node id is required"}
		}
		if n.Capacity.negative() || n.Capacity.zero() {
			return nil, &RejectError{ReasonInvalidQuota, fmt.Sprintf("node %s has non-positive capacity", n.ID)}
		}
		if seen[n.ID] {
			return nil, &RejectError{ReasonInvalidQuota, fmt.Sprintf("duplicate node id %s", n.ID)}
		}
		seen[n.ID] = true
	}
	for t, q := range cfg.Quotas {
		if t == "" {
			return nil, &RejectError{ReasonMissingField, "tenant id is required"}
		}
		if q.negative() || q.zero() {
			return nil, &RejectError{ReasonInvalidQuota, fmt.Sprintf("tenant %s has non-positive quota", t)}
		}
	}
	s := &Scheduler{
		cfg:   cfg,
		jobs:  map[string]*Job{},
		used:  map[string]Resource{},
		admit: map[string]Resource{},
	}
	if cfg.SnapshotPath != "" {
		s.store = NewStore(cfg.SnapshotPath)
		if err := s.recover(); err != nil {
			return nil, err
		}
	}
	return s, nil
}

// Submit validates and admits a job into the queue. Invalid submissions are
// refused with a *RejectError carrying a distinguishable RejectReason.
func (s *Scheduler) Submit(j Job) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.validate(j); err != nil {
		s.emit("REJECT", j.ID, err.Error())
		return err
	}
	j.State = StateQueued
	j.Submitted = s.cfg.Now()
	s.jobs[j.ID] = &j
	s.admit[j.TenantID] = s.admit[j.TenantID].Add(j.Request)
	s.emit("SUBMIT", j.ID, fmt.Sprintf("tenant=%s prio=%d req=%+v", j.TenantID, j.Priority, j.Request))
	return s.persistLocked()
}

func (s *Scheduler) validate(j Job) error {
	if j.ID == "" || j.TenantID == "" {
		return &RejectError{ReasonMissingField, "job id and tenant id are required"}
	}
	if j.Request.negative() {
		return &RejectError{ReasonNegativeResource, fmt.Sprintf("request must not be negative: %+v", j.Request)}
	}
	if j.Request.zero() {
		return &RejectError{ReasonZeroResource, "request must ask for some resource"}
	}
	quota, ok := s.cfg.Quotas[j.TenantID]
	if !ok {
		return &RejectError{ReasonInvalidQuota, fmt.Sprintf("tenant %s has no quota configured", j.TenantID)}
	}
	if _, dup := s.jobs[j.ID]; dup {
		return &RejectError{ReasonDuplicateJob, fmt.Sprintf("job %s already exists", j.ID)}
	}
	if !s.fitsFleet(j.Request) {
		return &RejectError{ReasonOversizedForFleet, fmt.Sprintf("request %+v exceeds every node", j.Request)}
	}
	if !s.admit[j.TenantID].Add(j.Request).Fits(quota) {
		return &RejectError{ReasonQuotaExceeded, fmt.Sprintf("tenant %s quota %+v would be exceeded by %+v", j.TenantID, quota, j.Request)}
	}
	return nil
}

func (s *Scheduler) fitsFleet(r Resource) bool {
	for _, n := range s.cfg.Nodes {
		if r.Fits(n.Capacity) {
			return true
		}
	}
	return false
}

// ReportProgress records execution progress (0..1); it survives preemption.
func (s *Scheduler) ReportProgress(id string, p float64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if j, ok := s.jobs[id]; ok && j.State == StateRunning {
		j.Progress = p
	}
}

// Complete marks a running job succeeded and releases its allocation.
func (s *Scheduler) Complete(id string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	j, ok := s.jobs[id]
	if !ok || j.State != StateRunning {
		return
	}
	s.releaseLocked(j)
	j.State = StateSucceeded
	j.Progress = 1
	s.admit[j.TenantID] = s.admit[j.TenantID].Sub(j.Request)
	s.emit("COMPLETE", id, "")
	_ = s.persistLocked()
}

// Fail releases the job's allocation and either schedules a bounded,
// exponentially-backed-off retry or marks the job permanently failed.
func (s *Scheduler) Fail(id string, cause string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	j, ok := s.jobs[id]
	if !ok || j.State != StateRunning {
		return
	}
	s.releaseLocked(j)
	s.emit("FAIL", id, cause)
	if j.Attempts > s.cfg.Retry.MaxRetries {
		j.State = StateFailed
		s.admit[j.TenantID] = s.admit[j.TenantID].Sub(j.Request)
		s.emit("EXHAUSTED", id, fmt.Sprintf("retries exhausted after %d attempts", j.Attempts))
	} else {
		j.State = StateBackoff
		j.NotBefore = s.cfg.Now().Add(s.cfg.Retry.backoffFor(j.Attempts))
		s.emit("REQUEUE", id, fmt.Sprintf("retry %d/%d after %s", j.Attempts, s.cfg.Retry.MaxRetries, s.cfg.Retry.backoffFor(j.Attempts)))
	}
	_ = s.persistLocked()
}

func (s *Scheduler) releaseLocked(j *Job) {
	if j.NodeID != "" {
		s.used[j.NodeID] = s.used[j.NodeID].Sub(j.Request)
		j.NodeID = ""
	}
}

// persistLocked snapshots state so a restart loses nothing.
func (s *Scheduler) persistLocked() error {
	if s.store == nil {
		return nil
	}
	jobs := make([]*Job, 0, len(s.jobs))
	for _, j := range s.jobs {
		jobs = append(jobs, j)
	}
	sort.Slice(jobs, func(a, b int) bool { return jobs[a].ID < jobs[b].ID })
	return s.store.Save(jobs)
}

// recover rebuilds the ledger from a snapshot: running jobs keep their
// node allocations (no double-booking, no leaks), backoff jobs whose delay
// expired during downtime are requeued on the next Schedule round.
func (s *Scheduler) recover() error {
	jobs, err := s.store.Load()
	if err != nil {
		return err
	}
	for _, j := range jobs {
		cp := *j
		s.jobs[cp.ID] = &cp
		switch cp.State {
		case StateRunning:
			s.used[cp.NodeID] = s.used[cp.NodeID].Add(cp.Request)
			s.admit[cp.TenantID] = s.admit[cp.TenantID].Add(cp.Request)
		case StateQueued, StateBackoff:
			s.admit[cp.TenantID] = s.admit[cp.TenantID].Add(cp.Request)
		}
		s.emit("RECOVER", cp.ID, fmt.Sprintf("state=%s node=%s", cp.State, cp.NodeID))
	}
	return nil
}

// effectivePriority applies aging: long-queued jobs gain priority so that
// low-priority work cannot starve.
func (s *Scheduler) effectivePriority(j *Job, now time.Time) int {
	boost := int(now.Sub(j.Submitted) / s.cfg.AgingInterval)
	if boost > s.cfg.MaxAgingBoost {
		boost = s.cfg.MaxAgingBoost
	}
	return j.Priority + boost
}

func (s *Scheduler) emit(kind, jobID, detail string) {
	s.events = append(s.events, Event{At: s.cfg.Now(), Kind: kind, JobID: jobID, Detail: detail})
}

// Events returns a copy of the explainability log.
func (s *Scheduler) Events() []Event {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]Event, len(s.events))
	copy(out, s.events)
	return out
}

// Schedule runs one deterministic scheduling round: it releases matured
// backoff jobs, orders the queue by (effective priority, submit time, id),
// places what fits, and preempts strictly-lower-effective-priority victims
// when a higher-priority job cannot otherwise be placed.
func (s *Scheduler) Schedule() {
	s.mu.Lock()
	defer s.mu.Unlock()
	now := s.cfg.Now()

	for _, j := range s.jobs {
		if j.State == StateBackoff && !now.Before(j.NotBefore) {
			j.State = StateQueued
			s.emit("RETRY", j.ID, fmt.Sprintf("backoff elapsed, attempt %d requeued", j.Attempts))
		}
	}

	queue := s.queuedLocked()
	for _, j := range queue {
		if node, ok := s.placeLocked(j); ok {
			s.emit("PLACE", j.ID, fmt.Sprintf("node=%s", node))
			continue
		}
		s.preemptLocked(j, now)
	}
	_ = s.persistLocked()
}

func (s *Scheduler) queuedLocked() []*Job {
	now := s.cfg.Now()
	var q []*Job
	for _, j := range s.jobs {
		if j.State == StateQueued {
			q = append(q, j)
		}
	}
	sort.Slice(q, func(a, b int) bool {
		ea, eb := s.effectivePriority(q[a], now), s.effectivePriority(q[b], now)
		if ea != eb {
			return ea > eb
		}
		if !q[a].Submitted.Equal(q[b].Submitted) {
			return q[a].Submitted.Before(q[b].Submitted)
		}
		return q[a].ID < q[b].ID
	})
	return q
}

// placeLocked first-fits the job onto the lowest-id node with room.
func (s *Scheduler) placeLocked(j *Job) (string, bool) {
	for _, n := range s.sortedNodes() {
		avail := n.Capacity.Sub(s.used[n.ID])
		if j.Request.Fits(avail) {
			s.used[n.ID] = s.used[n.ID].Add(j.Request)
			j.State = StateRunning
			j.NodeID = n.ID
			j.Attempts++
			return n.ID, true
		}
	}
	return "", false
}

func (s *Scheduler) sortedNodes() []Node {
	ns := make([]Node, len(s.cfg.Nodes))
	copy(ns, s.cfg.Nodes)
	sort.Slice(ns, func(a, b int) bool { return ns[a].ID < ns[b].ID })
	return ns
}

// preemptLocked evicts the minimal set of lowest-effective-priority victims
// (strictly lower than the candidate, outside cooldown) that lets the
// candidate fit on some node. Victims return to the queue with their
// progress preserved.
func (s *Scheduler) preemptLocked(cand *Job, now time.Time) {
	ceff := s.effectivePriority(cand, now)
	var victims []*Job
	for _, j := range s.jobs {
		if j.State != StateRunning {
			continue
		}
		if s.effectivePriority(j, now) >= ceff {
			continue // never preempt peers or betters
		}
		if now.Sub(j.PreemptedAt) < s.cfg.PreemptCooldown {
			continue // immune: prevents preempt thrash
		}
		victims = append(victims, j)
	}
	sort.Slice(victims, func(a, b int) bool {
		ea, eb := s.effectivePriority(victims[a], now), s.effectivePriority(victims[b], now)
		if ea != eb {
			return ea < eb
		}
		return victims[a].ID > victims[b].ID
	})
	var chosen []*Job
	freed := map[string]Resource{}
	for _, v := range victims {
		chosen = append(chosen, v)
		freed[v.NodeID] = freed[v.NodeID].Add(v.Request)
		if s.fitsWithFreed(cand.Request, freed) {
			break
		}
	}
	if !s.fitsWithFreed(cand.Request, freed) {
		return // even full preemption cannot place it; leave victims alone
	}
	for _, v := range chosen {
		s.used[v.NodeID] = s.used[v.NodeID].Sub(v.Request)
		v.State = StateQueued
		v.PreemptedAt = now
		v.Preemptions++
		s.emit("PREEMPT", v.ID, fmt.Sprintf("evicted by %s (eff prio %d > %d), progress %.2f kept", cand.ID, ceff, s.effectivePriority(v, now), v.Progress))
		v.NodeID = ""
	}
	if node, ok := s.placeLocked(cand); ok {
		s.emit("PLACE", cand.ID, fmt.Sprintf("node=%s after preempting %d job(s)", node, len(chosen)))
	}
}

func (s *Scheduler) fitsWithFreed(r Resource, freed map[string]Resource) bool {
	for _, n := range s.sortedNodes() {
		avail := n.Capacity.Sub(s.used[n.ID]).Add(freed[n.ID])
		if r.Fits(avail) {
			return true
		}
	}
	return false
}

// Job returns a snapshot of a job by id.
func (s *Scheduler) Job(id string) (Job, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	j, ok := s.jobs[id]
	if !ok {
		return Job{}, false
	}
	return *j, true
}

// NodeUsage returns allocated resources per node id.
func (s *Scheduler) NodeUsage() map[string]Resource {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := map[string]Resource{}
	for k, v := range s.used {
		out[k] = v
	}
	return out
}
