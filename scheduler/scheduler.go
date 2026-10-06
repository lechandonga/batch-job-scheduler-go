package scheduler

import (
	"fmt"
	"os"
	"sort"
	"sync"
	"time"
)

// 调度与抢占规则常量。
const (
	// PreemptCooldownRounds 被抢占的作业在此轮数内不可再次被抢占，防止来回抖动。
	PreemptCooldownRounds = 2
	// AgingPerRound 排队作业每等待一轮有效优先级 +1，防止低优先级饿死。
	AgingPerRound = 1
	// BackoffBase 重试退避基数，退避 = BackoffBase << retries，封顶 BackoffMax。
	BackoffBase = 100 * time.Millisecond
	BackoffMax  = 2 * time.Second
)

// Scheduler 本地批量作业调度引擎，使用资源账本模拟集群算力。
// 所有方法并发安全；每次状态变更后持久化快照，重启可恢复。
type Scheduler struct {
	mu        sync.Mutex
	capacity  Resources            // 集群总容量
	quotas    map[string]Resources // 租户配额
	jobs      map[string]*Job
	seq       uint64
	round     int
	storePath string
	now       func() time.Time // 可注入时钟，便于测试
}

// New 创建调度器；storePath 非空时启用快照持久化并尝试恢复。
func New(capacity Resources, quotas map[string]Resources, storePath string) (*Scheduler, error) {
	if capacity.Negative() || capacity.Zero() {
		return nil, fmt.Errorf("%w: cluster capacity must be positive", ErrNegativeResource)
	}
	for t, q := range quotas {
		if q.Negative() || q.Zero() {
			return nil, fmt.Errorf("%w: tenant %q quota must be positive", ErrUnknownTenant, t)
		}
	}
	s := &Scheduler{
		capacity:  capacity,
		quotas:    quotas,
		jobs:      map[string]*Job{},
		storePath: storePath,
		now:       time.Now,
	}
	if storePath != "" {
		if _, err := os.Stat(storePath); err == nil {
			snap, err := loadSnapshot(storePath)
			if err != nil {
				return nil, fmt.Errorf("load snapshot: %w", err)
			}
			s.capacity = snap.Capacity
			s.quotas = snap.Quotas
			s.seq = snap.Seq
			s.round = snap.Round
			for _, j := range snap.Jobs {
				s.jobs[j.ID] = j
			}
		}
	}
	return s, nil
}

// Submit 校验并接收作业。相同 ID 重复提交幂等返回 nil（不重复入队）。
func (s *Scheduler) Submit(j Job) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if j.ID == "" || j.Tenant == "" {
		return &RejectError{ErrMissingField, "id and tenant are required"}
	}
	if j.Req.Negative() {
		return &RejectError{ErrNegativeResource, fmt.Sprintf("req=%+v", j.Req)}
	}
	if j.Req.Zero() {
		return &RejectError{ErrZeroResource, "request must be positive"}
	}
	quota, ok := s.quotas[j.Tenant]
	if !ok {
		return &RejectError{ErrUnknownTenant, fmt.Sprintf("tenant %q has no quota", j.Tenant)}
	}
	if !j.Req.Fits(Resources{}, quota) {
		return &RejectError{ErrUnknownTenant, fmt.Sprintf("req=%+v exceeds tenant quota %+v", j.Req, quota)}
	}
	if !j.Req.Fits(Resources{}, s.capacity) {
		return &RejectError{ErrNegativeResource, fmt.Sprintf("req=%+v exceeds cluster capacity %+v", j.Req, s.capacity)}
	}
	if _, dup := s.jobs[j.ID]; dup {
		return nil // 幂等：相同 ID 不重复入队
	}
	s.seq++
	j.Seq = s.seq
	j.State = StateQueued
	s.jobs[j.ID] = &j
	return s.saveSnapshot()
}

// usedLocked 计算当前账本占用。
func (s *Scheduler) usedLocked() (cluster Resources, perTenant map[string]Resources) {
	perTenant = map[string]Resources{}
	for _, j := range s.jobs {
		if j.State == StateRunning {
			cluster = cluster.Add(j.Req)
			perTenant[j.Tenant] = perTenant[j.Tenant].Add(j.Req)
		}
	}
	return
}

// effectivePriority 排队优先级 = 基础优先级 + 等待轮数（aging 防饿死）。
func (s *Scheduler) effectivePriority(j *Job) int {
	waited := s.round - int(j.Seq)
	if waited < 0 {
		waited = 0
	}
	return j.Priority + waited*AgingPerRound
}

// Schedule 执行一轮调度，返回本轮放置的作业 ID（确定性顺序）。
// 规则：按（有效优先级降序，提交序号升序）处理队列；资源不足时允许抢占
// 严格更低优先级且不在冷却期内的运行中作业；被抢占作业保留进度回到队列。
func (s *Scheduler) Schedule() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.round++
	now := s.now()

	var queue []*Job
	for _, j := range s.jobs {
		switch j.State {
		case StateQueued:
			queue = append(queue, j)
		case StateBackoff:
			if !now.Before(j.NotBefore) {
				j.State = StateQueued
				queue = append(queue, j)
			}
		}
	}
	sort.Slice(queue, func(a, b int) bool {
		pa, pb := s.effectivePriority(queue[a]), s.effectivePriority(queue[b])
		if pa != pb {
			return pa > pb
		}
		return queue[a].Seq < queue[b].Seq
	})

	placed := []string{}
	for _, j := range queue {
		if s.tryPlace(j) {
			placed = append(placed, j.ID)
			continue
		}
		if s.preemptFor(j) && s.tryPlace(j) {
			placed = append(placed, j.ID)
		}
	}
	_ = s.saveSnapshot()
	return placed
}

// tryPlace 尝试在账本中放置作业（不超租户配额与集群容量）。
func (s *Scheduler) tryPlace(j *Job) bool {
	clusterUsed, tenantUsed := s.usedLocked()
	if !j.Req.Fits(clusterUsed, s.capacity) {
		return false
	}
	if !j.Req.Fits(tenantUsed[j.Tenant], s.quotas[j.Tenant]) {
		return false
	}
	j.State = StateRunning
	return true
}

// preemptFor 为作业 j 抢占资源。仅抢占：严格更低优先级、非冷却期、
// 且抢占后足以放置 j 的运行中作业。返回是否已腾出足够空间。
func (s *Scheduler) preemptFor(j *Job) bool {
	var victims []*Job
	for _, o := range s.jobs {
		if o.State == StateRunning && o.ID != j.ID &&
			o.Priority < j.Priority && o.CooldownRd < s.round {
			victims = append(victims, o)
		}
	}
	// 确定性：按优先级升序、序号降序选择牺牲者（先抢最低优先级）。
	sort.Slice(victims, func(a, b int) bool {
		if victims[a].Priority != victims[b].Priority {
			return victims[a].Priority < victims[b].Priority
		}
		return victims[a].Seq > victims[b].Seq
	})
	// 模拟：只抢占必要的最小集合。
	clusterUsed, tenantUsed := s.usedLocked()
	needCluster := !j.Req.Fits(clusterUsed, s.capacity)
	needTenant := !j.Req.Fits(tenantUsed[j.Tenant], s.quotas[j.Tenant])
	if !needCluster && !needTenant {
		return true
	}
	var chosen []*Job
	simCluster, simTenant := clusterUsed, tenantUsed[j.Tenant]
	for _, v := range victims {
		clusterOK := j.Req.Fits(simCluster, s.capacity)
		tenantOK := j.Req.Fits(simTenant, s.quotas[j.Tenant])
		if clusterOK && tenantOK {
			break
		}
		chosen = append(chosen, v)
		simCluster = simCluster.Sub(v.Req)
		if v.Tenant == j.Tenant {
			simTenant = simTenant.Sub(v.Req)
		}
	}
	if !j.Req.Fits(simCluster, s.capacity) || !j.Req.Fits(simTenant, s.quotas[j.Tenant]) {
		return false // 抢占也无法满足，不动账本
	}
	for _, v := range chosen {
		v.State = StateQueued // 进度保留在 v.Progress
		v.Preempted++
		v.CooldownRd = s.round + PreemptCooldownRounds
	}
	return true
}

// Complete 标记作业完成并释放资源。
func (s *Scheduler) Complete(id string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	j, ok := s.jobs[id]
	if !ok {
		return fmt.Errorf("job %q not found", id)
	}
	if j.State != StateRunning {
		return fmt.Errorf("job %q not running (state=%s)", id, j.State)
	}
	j.Progress = 100
	j.State = StateSucceeded
	return s.saveSnapshot()
}

// Fail 标记运行中作业失败：释放资源并进入退避重试；重试耗尽则 FAILED。
func (s *Scheduler) Fail(id string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	j, ok := s.jobs[id]
	if !ok {
		return fmt.Errorf("job %q not found", id)
	}
	if j.State != StateRunning {
		return fmt.Errorf("job %q not running (state=%s)", id, j.State)
	}
	if j.Retries >= j.MaxRetries {
		j.State = StateFailed
		return s.saveSnapshot()
	}
	j.Retries++
	backoff := BackoffBase << (j.Retries - 1)
	if backoff > BackoffMax {
		backoff = BackoffMax
	}
	j.NotBefore = s.now().Add(backoff)
	j.State = StateBackoff
	return s.saveSnapshot()
}

// ReportProgress 上报运行中作业进度（抢占/重试后不丢失）。
func (s *Scheduler) ReportProgress(id string, pct int) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	j, ok := s.jobs[id]
	if !ok {
		return fmt.Errorf("job %q not found", id)
	}
	if pct < 0 || pct > 100 {
		return fmt.Errorf("invalid progress %d", pct)
	}
	j.Progress = pct
	return s.saveSnapshot()
}

// Get 返回作业快照。
func (s *Scheduler) Get(id string) (Job, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	j, ok := s.jobs[id]
	if !ok {
		return Job{}, false
	}
	return *j, true
}

// Usage 返回当前账本占用（集群与租户）。
func (s *Scheduler) Usage() (Resources, map[string]Resources) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.usedLocked()
}
