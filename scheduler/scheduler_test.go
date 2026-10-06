package scheduler

import (
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"
)

// fakeClock lets tests control scheduling time deterministically.
type fakeClock struct{ t time.Time }

func (c *fakeClock) now() time.Time          { return c.t }
func (c *fakeClock) advance(d time.Duration) { c.t = c.t.Add(d) }

func newTestScheduler(t *testing.T, clk *fakeClock, snapshot string) *Scheduler {
	t.Helper()
	s, err := NewScheduler(Config{
		Nodes: []Node{
			{ID: "node-a", Capacity: Resource{CPU: 4, MemMB: 4096}},
			{ID: "node-b", Capacity: Resource{CPU: 4, MemMB: 4096}},
		},
		Quotas: map[string]Resource{
			"tenant-1": {CPU: 6, MemMB: 6144},
			"tenant-2": {CPU: 8, MemMB: 8192},
		},
		Retry:           RetryPolicy{MaxRetries: 2, InitialBackoff: time.Second, MaxBackoff: 10 * time.Second},
		AgingInterval:   time.Minute,
		MaxAgingBoost:   5,
		PreemptCooldown: 30 * time.Second,
		SnapshotPath:    snapshot,
		Now:             clk.now,
	})
	if err != nil {
		t.Fatalf("NewScheduler: %v", err)
	}
	return s
}

func mustReject(t *testing.T, err error, want RejectReason) {
	t.Helper()
	var re *RejectError
	if !errors.As(err, &re) {
		t.Fatalf("expected RejectError, got %v", err)
	}
	if re.Reason != want {
		t.Fatalf("reason = %s, want %s (err=%v)", re.Reason, want, err)
	}
	t.Logf("rejected as expected: %v", err)
}

// --- Scenario: submission validation & quota semantics ---------------------

func TestSubmitValidationAndQuota(t *testing.T) {
	clk := &fakeClock{t: time.Now()}
	s := newTestScheduler(t, clk, "")

	t.Run("缺字段被拒绝", func(t *testing.T) {
		mustReject(t, s.Submit(Job{TenantID: "tenant-1", Request: Resource{CPU: 1}}), ReasonMissingField)
		mustReject(t, s.Submit(Job{ID: "j1", Request: Resource{CPU: 1}}), ReasonMissingField)
	})
	t.Run("负资源被拒绝", func(t *testing.T) {
		mustReject(t, s.Submit(Job{ID: "j2", TenantID: "tenant-1", Request: Resource{CPU: -1, MemMB: 1}}), ReasonNegativeResource)
	})
	t.Run("零资源被拒绝", func(t *testing.T) {
		mustReject(t, s.Submit(Job{ID: "j3", TenantID: "tenant-1"}), ReasonZeroResource)
	})
	t.Run("无配额租户被拒绝", func(t *testing.T) {
		mustReject(t, s.Submit(Job{ID: "j4", TenantID: "ghost", Request: Resource{CPU: 1}}), ReasonInvalidQuota)
	})
	t.Run("超出单节点容量被拒绝", func(t *testing.T) {
		mustReject(t, s.Submit(Job{ID: "j5", TenantID: "tenant-1", Request: Resource{CPU: 99}}), ReasonOversizedForFleet)
	})
	t.Run("超出租户配额被拒绝", func(t *testing.T) {
		if err := s.Submit(Job{ID: "big-1", TenantID: "tenant-1", Priority: 1, Request: Resource{CPU: 4, MemMB: 4096}}); err != nil {
			t.Fatal(err)
		}
		mustReject(t, s.Submit(Job{ID: "big-2", TenantID: "tenant-1", Request: Resource{CPU: 4, MemMB: 4096}}), ReasonQuotaExceeded)
	})
	t.Run("重复作业ID被拒绝", func(t *testing.T) {
		mustReject(t, s.Submit(Job{ID: "big-1", TenantID: "tenant-1", Request: Resource{CPU: 1}}), ReasonDuplicateJob)
	})
	t.Run("非法配额配置被拒绝", func(t *testing.T) {
		_, err := NewScheduler(Config{
			Nodes:  []Node{{ID: "n", Capacity: Resource{CPU: 1}}},
			Quotas: map[string]Resource{"t": {CPU: -1}},
		})
		mustReject(t, err, ReasonInvalidQuota)
	})
}

func TestDeterministicPlacement(t *testing.T) {
	run := func() map[string]string {
		clk := &fakeClock{t: time.Now()}
		s := newTestScheduler(t, clk, "")
		for i := 0; i < 4; i++ {
			_ = s.Submit(Job{ID: fmt.Sprintf("j%d", i), TenantID: "tenant-1", Priority: 1, Request: Resource{CPU: 1, MemMB: 512}})
		}
		s.Schedule()
		out := map[string]string{}
		for i := 0; i < 4; i++ {
			j, _ := s.Job(fmt.Sprintf("j%d", i))
			out[j.ID] = j.NodeID
		}
		return out
	}
	first := run()
	for i := 0; i < 5; i++ {
		got := run()
		for id, node := range first {
			if got[id] != node {
				t.Fatalf("non-deterministic placement for %s: %s vs %s", id, node, got[id])
			}
		}
	}
	t.Logf("deterministic placement across runs: %v", first)
}

// --- Scenario: priority preemption & fairness ------------------------------

func TestPreemption(t *testing.T) {
	clk := &fakeClock{t: time.Now()}
	s := newTestScheduler(t, clk, "")

	// Fill the fleet with low-priority work (tenant-2 quota: 4 CPU).
	for i := 0; i < 4; i++ {
		id := fmt.Sprintf("low-%d", i)
		if err := s.Submit(Job{ID: id, TenantID: "tenant-2", Priority: 1, Request: Resource{CPU: 2, MemMB: 1024}}); err != nil {
			t.Fatal(err)
		}
	}
	s.Schedule()
	for i := 0; i < 4; i++ {
		j, _ := s.Job(fmt.Sprintf("low-%d", i))
		if j.State != StateRunning {
			t.Fatalf("low-%d not running: %s", i, j.State)
		}
	}
	// Give every potential victim meaningful progress.
	for i := 0; i < 4; i++ {
		s.ReportProgress(fmt.Sprintf("low-%d", i), 0.7)
	}

	// High-priority job arrives; needs 2 CPU, no room -> preempt one victim.
	if err := s.Submit(Job{ID: "hi", TenantID: "tenant-1", Priority: 10, Request: Resource{CPU: 2, MemMB: 1024}}); err != nil {
		t.Fatal(err)
	}
	s.Schedule()

	hi, _ := s.Job("hi")
	if hi.State != StateRunning {
		t.Fatalf("hi not running after preemption: %s", hi.State)
	}
	var back []Job
	for i := 0; i < 4; i++ {
		j, _ := s.Job(fmt.Sprintf("low-%d", i))
		if j.State == StateQueued {
			back = append(back, j)
		}
	}
	if len(back) != 1 {
		t.Fatalf("expected exactly 1 preempted job, got %d", len(back))
	}
	if back[0].Progress != 0.7 {
		t.Fatalf("preempted job lost progress: %v", back[0].Progress)
	}
	t.Logf("preempted %s with progress %.2f preserved", back[0].ID, back[0].Progress)

	// Capacity recovery: complete hi, preempted job returns to running.
	s.Complete("hi")
	s.Schedule()
	j, _ := s.Job(back[0].ID)
	if j.State != StateRunning {
		t.Fatalf("preempted job did not recover after capacity freed: %s", j.State)
	}
	t.Logf("job %s resumed on node %s after capacity recovery", j.ID, j.NodeID)
}

func TestNoPreemptThrashAndNoStarvation(t *testing.T) {
	clk := &fakeClock{t: time.Now()}
	s := newTestScheduler(t, clk, "")

	// Two low-priority jobs hold the whole fleet.
	_ = s.Submit(Job{ID: "low-1", TenantID: "tenant-2", Priority: 1, Request: Resource{CPU: 4, MemMB: 4096}})
	_ = s.Submit(Job{ID: "low-2", TenantID: "tenant-2", Priority: 1, Request: Resource{CPU: 4, MemMB: 4096}})
	s.Schedule()

	submit := func(id string) {
		if err := s.Submit(Job{ID: id, TenantID: "tenant-1", Priority: 5, Request: Resource{CPU: 4, MemMB: 4096}}); err != nil {
			t.Fatal(err)
		}
	}
	// First high-priority job preempts exactly one victim.
	submit("hi-1")
	s.Schedule()
	l1, _ := s.Job("low-1")
	l2, _ := s.Job("low-2")
	if l1.Preemptions+l2.Preemptions != 1 {
		t.Fatalf("expected exactly 1 preemption, got %d+%d", l1.Preemptions, l2.Preemptions)
	}
	// Capacity returns, victim resumes; second preemption takes the other job.
	s.Complete("hi-1")
	s.Schedule()
	submit("hi-2")
	clk.advance(5 * time.Second) // inside 30s cooldown of the first victim
	s.Schedule()
	s.Complete("hi-2")
	s.Schedule()
	// Third high-priority job: both victims are still inside their cooldown,
	// so it must wait instead of bouncing them again.
	submit("hi-3")
	clk.advance(5 * time.Second)
	s.Schedule()
	l1, _ = s.Job("low-1")
	l2, _ = s.Job("low-2")
	h3, _ := s.Job("hi-3")
	if l1.Preemptions != 1 || l2.Preemptions != 1 {
		t.Fatalf("thrash: preemptions low-1=%d low-2=%d", l1.Preemptions, l2.Preemptions)
	}
	if h3.State != StateQueued {
		t.Fatalf("hi-3 should wait out the victim cooldown, got %s", h3.State)
	}
	t.Logf("cooldown prevented thrash: low-1=%d low-2=%d preemptions, hi-3 queued", l1.Preemptions, l2.Preemptions)

	// Starvation: a queued low-priority job ages until it outranks newcomers.
	clk2 := &fakeClock{t: time.Now()}
	s2, err := NewScheduler(Config{
		Nodes: []Node{
			{ID: "node-a", Capacity: Resource{CPU: 4, MemMB: 4096}},
			{ID: "node-b", Capacity: Resource{CPU: 4, MemMB: 4096}},
		},
		Quotas:        map[string]Resource{"tenant-1": {CPU: 8, MemMB: 8192}, "tenant-2": {CPU: 8, MemMB: 8192}},
		AgingInterval: time.Minute,
		Now:           clk2.now,
	})
	if err != nil {
		t.Fatal(err)
	}
	_ = s2.Submit(Job{ID: "old-low", TenantID: "tenant-2", Priority: 1, Request: Resource{CPU: 4, MemMB: 4096}})
	_ = s2.Submit(Job{ID: "filler", TenantID: "tenant-1", Priority: 3, Request: Resource{CPU: 4, MemMB: 4096}})
	s2.Schedule() // both nodes full
	_ = s2.Submit(Job{ID: "waiting-low", TenantID: "tenant-2", Priority: 1, Request: Resource{CPU: 4, MemMB: 4096}})
	s2.Schedule() // queued: no room
	if j, _ := s2.Job("waiting-low"); j.State != StateQueued {
		t.Fatalf("waiting-low should be queued: %s", j.State)
	}
	// Age it past a newer, higher-base-priority competitor.
	clk2.advance(2 * time.Minute) // eff prio = 1 + 2 = 3
	_ = s2.Submit(Job{ID: "newcomer", TenantID: "tenant-1", Priority: 2, Request: Resource{CPU: 4, MemMB: 4096}})
	s2.Complete("old-low")
	s2.Schedule()
	w, _ := s2.Job("waiting-low")
	n, _ := s2.Job("newcomer")
	if w.State != StateRunning || n.State != StateQueued {
		t.Fatalf("aging failed: waiting-low=%s newcomer=%s", w.State, n.State)
	}
	t.Logf("starvation avoided: aged waiting-low running, newcomer queued")
}

// --- Scenario: failure retry with bounded backoff --------------------------

func TestFailureRetryConverges(t *testing.T) {
	clk := &fakeClock{t: time.Now()}
	s := newTestScheduler(t, clk, "")

	if err := s.Submit(Job{ID: "flaky", TenantID: "tenant-1", Priority: 1, Request: Resource{CPU: 2, MemMB: 1024}}); err != nil {
		t.Fatal(err)
	}
	s.Schedule()
	j, _ := s.Job("flaky")
	if j.State != StateRunning {
		t.Fatalf("flaky not running: %s", j.State)
	}

	// Failure releases the allocation immediately: no leak, no double-booking.
	node := j.NodeID
	s.Fail("flaky", "simulated crash")
	if used := s.NodeUsage()[node]; used.CPU != 0 {
		t.Fatalf("%s still holds %+v after failure", node, used)
	}
	j, _ = s.Job("flaky")
	if j.State != StateBackoff {
		t.Fatalf("expected BACKOFF, got %s", j.State)
	}

	// Backoff gates the retry; after it elapses the job runs again.
	s.Schedule()
	if j, _ = s.Job("flaky"); j.State != StateBackoff {
		t.Fatalf("retry fired before backoff elapsed: %s", j.State)
	}
	clk.advance(time.Second)
	s.Schedule()
	if j, _ = s.Job("flaky"); j.State != StateRunning {
		t.Fatalf("retry did not run after backoff: %s", j.State)
	}

	// Exhaust the remaining retries; the job converges to FAILED and its
	// quota is released.
	s.Fail("flaky", "crash 2")
	clk.advance(2 * time.Second)
	s.Schedule()
	s.Fail("flaky", "crash 3")
	j, _ = s.Job("flaky")
	if j.State != StateFailed {
		t.Fatalf("expected FAILED after retries exhausted, got %s", j.State)
	}
	for _, used := range s.NodeUsage() {
		if used.CPU != 0 || used.MemMB != 0 {
			t.Fatalf("resource leak after exhaustion: %+v", used)
		}
	}
	t.Logf("retry converged to FAILED after %d attempts, ledger clean", j.Attempts)
}

// --- Scenario: concurrent submissions never oversell -----------------------

func TestConcurrentSubmitNoOversell(t *testing.T) {
	clk := &fakeClock{t: time.Now()}
	s := newTestScheduler(t, clk, "")

	const workers = 32
	var wg sync.WaitGroup
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := 0; i < 4; i++ {
				_ = s.Submit(Job{
					ID:       fmt.Sprintf("w%d-j%d", w, i),
					TenantID: fmt.Sprintf("tenant-%d", w%2+1),
					Priority: w % 5,
					Request:  Resource{CPU: 1, MemMB: 512},
				})
				s.Schedule()
			}
		}(w)
	}
	wg.Wait()

	// Ledger invariant: per-node usage never exceeds capacity.
	usage := s.NodeUsage()
	for _, n := range []string{"node-a", "node-b"} {
		u := usage[n]
		if u.CPU > 4 || u.MemMB > 4096 {
			t.Fatalf("oversold %s: %+v", n, u)
		}
		t.Logf("node %s usage after concurrent storm: %+v", n, u)
	}
	// Running jobs exactly account for the ledger.
	var sum Resource
	for w := 0; w < workers; w++ {
		for i := 0; i < 4; i++ {
			j, ok := s.Job(fmt.Sprintf("w%d-j%d", w, i))
			if ok && j.State == StateRunning {
				sum = sum.Add(j.Request)
			}
		}
	}
	total := usage["node-a"].Add(usage["node-b"])
	if sum != total {
		t.Fatalf("ledger mismatch: running=%+v ledger=%+v", sum, total)
	}
	t.Logf("ledger consistent under concurrency: %+v", total)
}

// --- Scenario: restart recovery --------------------------------------------

func TestRestartRecovery(t *testing.T) {
	clk := &fakeClock{t: time.Now()}
	path := t.TempDir() + "/snapshot.json"

	s := newTestScheduler(t, clk, path)
	_ = s.Submit(Job{ID: "run-1", TenantID: "tenant-1", Priority: 5, Request: Resource{CPU: 2, MemMB: 1024}})
	_ = s.Submit(Job{ID: "queued-1", TenantID: "tenant-1", Priority: 1, Request: Resource{CPU: 2, MemMB: 1024}})
	_ = s.Submit(Job{ID: "flaky", TenantID: "tenant-2", Priority: 1, Request: Resource{CPU: 2, MemMB: 1024}})
	s.Schedule()
	s.ReportProgress("run-1", 0.4)
	s.Fail("flaky", "crash before restart")

	// Simulate a crash+restart: a fresh scheduler over the same snapshot.
	s2 := newTestScheduler(t, clk, path)

	j, ok := s2.Job("run-1")
	if !ok || j.State != StateRunning || j.NodeID == "" {
		t.Fatalf("running job lost across restart: %+v", j)
	}
	if j.Progress != 0.4 {
		t.Fatalf("progress lost across restart: %v", j.Progress)
	}
	if _, ok := s2.Job("queued-1"); !ok {
		t.Fatal("queued job lost across restart")
	}
	if j, _ := s2.Job("flaky"); j.State != StateBackoff {
		t.Fatalf("backoff state lost across restart: %s", j.State)
	}
	// No double-booking: recovered allocation is on the ledger exactly once.
	if u := s2.NodeUsage()[j.NodeID]; u.CPU != 2 {
		t.Fatalf("recovered ledger wrong (double-book or leak): %+v", u)
	}
	// Recovered scheduler keeps making progress without duplicating work.
	clk.advance(time.Second)
	s2.Schedule()
	j2, _ := s2.Job("flaky")
	if j2.State != StateRunning || j2.Attempts != 2 {
		t.Fatalf("retry after restart wrong: state=%s attempts=%d", j2.State, j2.Attempts)
	}
	t.Logf("restart recovered: run-1 running (progress %.1f), flaky retry attempt %d", j.Progress, j2.Attempts)
}
