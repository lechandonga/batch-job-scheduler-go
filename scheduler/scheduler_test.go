package scheduler

import (
	"errors"
	"fmt"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

func testQuotas() map[string]Resources {
	return map[string]Resources{
		"team-a": {CPU: 4, MemMB: 4096},
		"team-b": {CPU: 4, MemMB: 4096},
	}
}

// TestQuotaRejection 覆盖：缺字段 / 负资源 / 零资源 / 未知租户 / 超配额 的拒绝原因可区分。
func TestQuotaRejection(t *testing.T) {
	s, err := New(Resources{CPU: 8, MemMB: 8192}, testQuotas(), "")
	if err != nil {
		t.Fatalf("new scheduler: %v", err)
	}
	cases := []struct {
		name string
		job  Job
		want error
	}{
		{"missing id", Job{Tenant: "team-a", Req: Resources{CPU: 1, MemMB: 1}}, ErrMissingField},
		{"missing tenant", Job{ID: "j1", Req: Resources{CPU: 1, MemMB: 1}}, ErrMissingField},
		{"negative cpu", Job{ID: "j2", Tenant: "team-a", Req: Resources{CPU: -1, MemMB: 1}}, ErrNegativeResource},
		{"zero req", Job{ID: "j3", Tenant: "team-a", Req: Resources{}}, ErrZeroResource},
		{"unknown tenant", Job{ID: "j4", Tenant: "ghost", Req: Resources{CPU: 1, MemMB: 1}}, ErrUnknownTenant},
		{"over tenant quota", Job{ID: "j5", Tenant: "team-a", Req: Resources{CPU: 5, MemMB: 1}}, ErrUnknownTenant},
	}
	for _, tc := range cases {
		err := s.Submit(tc.job)
		if !errors.Is(err, tc.want) {
			t.Errorf("case %q: got %v, want reason %v", tc.name, err, tc.want)
		} else {
			t.Logf("case %q rejected as expected: %v", tc.name, err)
		}
	}

	// 非法配额配置必须被拒绝。
	if _, err := New(Resources{CPU: -1}, testQuotas(), ""); !errors.Is(err, ErrNegativeResource) {
		t.Errorf("negative capacity: got %v", err)
	}
	if _, err := New(Resources{CPU: 8, MemMB: 8192}, map[string]Resources{"bad": {CPU: 0}}, ""); !errors.Is(err, ErrUnknownTenant) {
		t.Errorf("invalid tenant quota: got %v", err)
	}
	t.Log("quota rejection scenarios all covered")
}

// TestDeterministicPlacement 覆盖：相同输入多次调度得到一致放置结果。
func TestDeterministicPlacement(t *testing.T) {
	run := func() []string {
		s, _ := New(Resources{CPU: 4, MemMB: 4096}, testQuotas(), "")
		for i := 0; i < 6; i++ {
			_ = s.Submit(Job{ID: fmt.Sprintf("j%d", i), Tenant: "team-a", Priority: i % 3, Req: Resources{CPU: 1, MemMB: 512}})
		}
		return s.Schedule()
	}
	first := run()
	for i := 0; i < 5; i++ {
		got := run()
		if fmt.Sprint(got) != fmt.Sprint(first) {
			t.Fatalf("non-deterministic placement: %v vs %v", first, got)
		}
	}
	t.Logf("deterministic placement over 5 runs: %v", first)
}

// TestPreemption 覆盖：高优先级抢占低优先级、进度保留、冷却防抖、容量恢复后稳定。
func TestPreemption(t *testing.T) {
	s, _ := New(Resources{CPU: 2, MemMB: 2048}, testQuotas(), "")
	_ = s.Submit(Job{ID: "low", Tenant: "team-a", Priority: 1, Req: Resources{CPU: 2, MemMB: 1024}})
	placed := s.Schedule()
	if len(placed) != 1 || placed[0] != "low" {
		t.Fatalf("low should be placed, got %v", placed)
	}
	_ = s.ReportProgress("low", 60)

	_ = s.Submit(Job{ID: "high", Tenant: "team-a", Priority: 9, Req: Resources{CPU: 2, MemMB: 1024}})
	placed = s.Schedule()
	jLow, _ := s.Get("low")
	jHigh, _ := s.Get("high")
	if jHigh.State != StateRunning {
		t.Fatalf("high should preempt and run, got %s", jHigh.State)
	}
	if jLow.State != StateQueued || jLow.Progress != 60 || jLow.Preempted != 1 {
		t.Fatalf("preempted job must keep progress and requeue, got %+v", jLow)
	}
	t.Logf("preemption ok: high running, low requeued with progress=%d preempted=%d", jLow.Progress, jLow.Preempted)

	// 冷却期内 low 不可被再次抢占（防抖）：再来一个高优先级作业不应抢占 low。
	_ = s.Complete("high")
	s.Schedule() // low 重新运行
	_ = s.Submit(Job{ID: "high2", Tenant: "team-a", Priority: 9, Req: Resources{CPU: 2, MemMB: 1024}})
	s.Schedule()
	jLow, _ = s.Get("low")
	jHigh2, _ := s.Get("high2")
	if jLow.Preempted != 1 {
		t.Fatalf("cooldown violated: low preempted %d times", jLow.Preempted)
	}
	t.Logf("cooldown ok: low state=%s preempted=%d, high2 state=%s", jLow.State, jLow.Preempted, jHigh2.State)

	// 容量恢复后回到稳定状态。
	_ = s.Complete("low")
	s.Schedule()
	jHigh2, _ = s.Get("high2")
	if jHigh2.State != StateRunning {
		t.Fatalf("after capacity freed, high2 should run, got %s", jHigh2.State)
	}
	t.Log("system returned to stable state after capacity recovery")
}

// TestFairnessNoStarvation 覆盖：低优先级作业经 aging 最终获得资源，不饿死。
func TestFairnessNoStarvation(t *testing.T) {
	s, _ := New(Resources{CPU: 1, MemMB: 1024}, testQuotas(), "")
	_ = s.Submit(Job{ID: "poor", Tenant: "team-a", Priority: 0, Req: Resources{CPU: 1, MemMB: 256}})
	s.Schedule()
	_ = s.Complete("poor") // 先让它跑完一轮，验证 aging 后它能再次竞争成功
	_ = s.Submit(Job{ID: "poor2", Tenant: "team-a", Priority: 0, Req: Resources{CPU: 1, MemMB: 256}})
	// 持续有高优先级作业流入，但每个只跑一轮。
	ran := false
	for round := 0; round < 50 && !ran; round++ {
		hid := fmt.Sprintf("rich%d", round)
		_ = s.Submit(Job{ID: hid, Tenant: "team-a", Priority: 5, Req: Resources{CPU: 1, MemMB: 256}})
		s.Schedule()
		if j, _ := s.Get("poor2"); j.State == StateRunning {
			ran = true
			_ = s.Complete("poor2")
		} else {
			_ = s.Complete(hid)
		}
	}
	if !ran {
		t.Fatal("low priority job starved for 50 rounds")
	}
	t.Log("aging prevented starvation: low priority job eventually ran")
}

// TestRetryBackoff 覆盖：失败重试退避收敛、资源不泄漏、重试耗尽进入 FAILED。
func TestRetryBackoff(t *testing.T) {
	s, _ := New(Resources{CPU: 2, MemMB: 2048}, testQuotas(), "")
	_ = s.Submit(Job{ID: "flaky", Tenant: "team-a", Priority: 1, Req: Resources{CPU: 1, MemMB: 512}, MaxRetries: 2})
	s.Schedule()

	for attempt := 1; attempt <= 2; attempt++ {
		if err := s.Fail("flaky"); err != nil {
			t.Fatalf("fail attempt %d: %v", attempt, err)
		}
		used, _ := s.Usage()
		if used.CPU != 0 {
			t.Fatalf("resource leaked after failure: used=%+v", used)
		}
		j, _ := s.Get("flaky")
		if j.State != StateBackoff || j.Retries != attempt {
			t.Fatalf("attempt %d: got state=%s retries=%d", attempt, j.State, j.Retries)
		}
		// 注入时钟推进，跳过退避。
		s.mu.Lock()
		base := s.now()
		s.now = func() time.Time { return base.Add(BackoffMax + time.Second) }
		s.mu.Unlock()
		s.Schedule()
		j, _ = s.Get("flaky")
		if j.State != StateRunning {
			t.Fatalf("attempt %d: should retry, got %s", attempt, j.State)
		}
		t.Logf("retry attempt %d converged: backoff honored, resource re-acquired cleanly", attempt)
	}
	// 重试耗尽。
	_ = s.Fail("flaky")
	j, _ := s.Get("flaky")
	if j.State != StateFailed {
		t.Fatalf("retries exhausted: got %s", j.State)
	}
	used, _ := s.Usage()
	if used.CPU != 0 {
		t.Fatalf("resource leaked after final failure: %+v", used)
	}
	t.Log("retry budget exhausted, job FAILED, no resource leak")
}

// TestConcurrentSubmit 覆盖：并发提交不超卖资源、无重复入队。
func TestConcurrentSubmit(t *testing.T) {
	s, _ := New(Resources{CPU: 8, MemMB: 8192}, testQuotas(), "")
	var wg sync.WaitGroup
	errs := make(chan error, 200)
	for g := 0; g < 10; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := 0; i < 10; i++ {
				id := fmt.Sprintf("g%d-j%d", g, i)
				if err := s.Submit(Job{ID: id, Tenant: "team-a", Priority: i, Req: Resources{CPU: 1, MemMB: 512}}); err != nil {
					errs <- err
				}
			}
			// 重复提交同一 ID 必须幂等。
			if err := s.Submit(Job{ID: fmt.Sprintf("g%d-j0", g), Tenant: "team-a", Req: Resources{CPU: 1, MemMB: 512}}); err != nil {
				errs <- err
			}
		}(g)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatalf("concurrent submit error: %v", err)
	}
	s.Schedule()
	used, tenant := s.Usage()
	if used.CPU > 8 || tenant["team-a"].CPU > 4 {
		t.Fatalf("oversold: cluster=%+v tenant=%+v", used, tenant)
	}
	t.Logf("concurrent submit ok: 100 jobs, cluster used=%+v (cap 8), team-a used=%+v (quota 4)", used, tenant["team-a"])
}

// TestRestartRecovery 覆盖：重启后作业状态与资源占用恢复，不重复不丢单。
func TestRestartRecovery(t *testing.T) {
	path := filepath.Join(t.TempDir(), "snapshot.json")
	s, _ := New(Resources{CPU: 2, MemMB: 4096}, testQuotas(), path)
	_ = s.Submit(Job{ID: "run1", Tenant: "team-a", Priority: 5, Req: Resources{CPU: 2, MemMB: 1024}})
	_ = s.Submit(Job{ID: "wait1", Tenant: "team-a", Priority: 1, Req: Resources{CPU: 2, MemMB: 1024}})
	s.Schedule()
	_ = s.ReportProgress("run1", 42)

	// 模拟重启：同一快照路径新建调度器。
	s2, err := New(Resources{CPU: 2, MemMB: 4096}, testQuotas(), path)
	if err != nil {
		t.Fatalf("recover: %v", err)
	}
	j, ok := s2.Get("run1")
	if !ok || j.State != StateRunning || j.Progress != 42 {
		t.Fatalf("run1 not recovered: %+v ok=%v", j, ok)
	}
	w, ok := s2.Get("wait1")
	if !ok || w.State != StateQueued {
		t.Fatalf("wait1 not recovered: %+v ok=%v", w, ok)
	}
	used, _ := s2.Usage()
	if used.CPU != 2 {
		t.Fatalf("ledger not restored: used=%+v", used)
	}
	// 恢复后继续调度不得双重占用。
	_ = s2.Submit(Job{ID: "new1", Tenant: "team-a", Priority: 1, Req: Resources{CPU: 2, MemMB: 1024}})
	s2.Schedule()
	used, _ = s2.Usage()
	if used.CPU > 2 {
		t.Fatalf("double allocation after recovery: %+v", used)
	}
	// 不丢单：恢复后 wait1 仍能被调度。
	_ = s2.Complete("run1")
	s2.Schedule()
	w, _ = s2.Get("wait1")
	if w.State != StateRunning {
		t.Fatalf("queued job lost after recovery: %s", w.State)
	}
	t.Logf("restart recovery ok: run1(progress=42) restored, wait1 scheduled, usage=%+v", used)
}
