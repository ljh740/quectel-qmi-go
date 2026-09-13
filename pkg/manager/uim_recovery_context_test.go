package manager

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/ljh740/quectel-qmi-go/pkg/qmi"
)

func newUIMContextTestManager() *Manager {
	m := newRecoveryTestManager()
	m.coreReady = true
	m.state = StateDisconnected
	return m
}

// 惰性分配前的锁等待受 ctx 期限约束：恢复锁被占用时在期限内返回 ctx 错误，不会一直等。
func TestEnsureUIMServiceContextGivesUpLockWaitAtDeadline(t *testing.T) {
	m := newUIMContextTestManager()
	m.client = &qmi.Client{}
	m.uimRecoveryMu.Lock()
	defer m.uimRecoveryMu.Unlock()

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Millisecond)
	defer cancel()
	start := time.Now()
	_, err := m.ensureUIMServiceContext(ctx)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("ensureUIMServiceContext with the lock held = %v, want DeadlineExceeded", err)
	}
	if waited := time.Since(start); waited > time.Second {
		t.Fatalf("ensureUIMServiceContext waited %v, want to give up at the 60ms deadline", waited)
	}
}

// 操作失败后重绑前先看期限：期限已到不再重绑、不再重试，返回原始错误。
func TestWithUIMRecoveryContextSkipsRebindWhenDeadlineExpired(t *testing.T) {
	m := newUIMContextTestManager()
	uim := &qmi.UIMService{}
	m.ensureUIMServiceHook = func() (*qmi.UIMService, error) { return uim, nil }
	var rebinds int
	m.rebindUIMServiceHook = func(reason string) (*qmi.UIMService, error) {
		rebinds++
		return uim, nil
	}
	ctx, cancel := context.WithCancel(context.Background())
	opErr := recoverableQMIError(qmi.ServiceUIM, qmi.UIMPowerOnSIM)
	calls := 0
	err := m.withUIMRecoveryContext(ctx, "UIMPowerOnSIM", func(*qmi.UIMService) error {
		calls++
		cancel() // 操作本身耗尽了调用方的期限
		return opErr
	})
	if !errors.Is(err, opErr) {
		t.Fatalf("withUIMRecoveryContext = %v, want the original operation error", err)
	}
	if calls != 1 || rebinds != 0 {
		t.Fatalf("calls=%d rebinds=%d, want a single attempt without rebind after the deadline", calls, rebinds)
	}
	select {
	case evt := <-m.eventCh:
		t.Fatalf("caller cancellation scheduled %v, want no recovery", evt)
	default:
	}
}

// 重绑前的恢复锁等待同样受期限约束：锁被占用时在期限内放弃，不调用重绑。
func TestWithUIMRecoveryContextGivesUpRebindLockAtDeadline(t *testing.T) {
	m := newUIMContextTestManager()
	uim := &qmi.UIMService{}
	m.ensureUIMServiceHook = func() (*qmi.UIMService, error) { return uim, nil }
	var rebinds int
	m.rebindUIMServiceHook = func(reason string) (*qmi.UIMService, error) {
		rebinds++
		return uim, nil
	}
	m.uimRecoveryMu.Lock()
	defer m.uimRecoveryMu.Unlock()

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Millisecond)
	defer cancel()
	opErr := recoverableQMIError(qmi.ServiceUIM, qmi.UIMPowerOnSIM)
	err := m.withUIMRecoveryContext(ctx, "UIMPowerOnSIM", func(*qmi.UIMService) error { return opErr })
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("withUIMRecoveryContext with the recovery lock held = %v, want DeadlineExceeded", err)
	}
	if rebinds != 0 {
		t.Fatalf("rebinds = %d, want 0 when the lock wait times out", rebinds)
	}
}

// 期限充足时行为不变：失败→重绑→重试成功。
func TestWithUIMRecoveryContextStillRebindsWithinDeadline(t *testing.T) {
	m := newUIMContextTestManager()
	first := &qmi.UIMService{}
	rebound := &qmi.UIMService{}
	m.ensureUIMServiceHook = func() (*qmi.UIMService, error) { return first, nil }
	m.rebindUIMServiceHook = func(reason string) (*qmi.UIMService, error) { return rebound, nil }
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	var seen []*qmi.UIMService
	err := m.withUIMRecoveryContext(ctx, "UIMPowerOnSIM", func(u *qmi.UIMService) error {
		seen = append(seen, u)
		if u == first {
			return recoverableQMIError(qmi.ServiceUIM, qmi.UIMPowerOnSIM)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("withUIMRecoveryContext = %v, want success after rebind", err)
	}
	if len(seen) != 2 || seen[0] != first || seen[1] != rebound {
		t.Fatalf("attempts = %v, want first then rebound", seen)
	}
}

// lockContext：无期限 ctx 直接阻塞获取；有期限 ctx 在锁释放后立即获得锁。
func TestLockContextAcquiresWhenLockIsReleased(t *testing.T) {
	var mu sync.Mutex
	mu.Lock()
	go func() {
		time.Sleep(30 * time.Millisecond)
		mu.Unlock()
	}()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := lockContext(ctx, &mu); err != nil {
		t.Fatalf("lockContext = %v, want acquired after release", err)
	}
	mu.Unlock()
	if err := lockContext(context.Background(), &mu); err != nil {
		t.Fatalf("lockContext(Background) = %v", err)
	}
	mu.Unlock()
}

// 惰性分配的锁等待超时且已达服务超时阈值：错误上报走独立冷却锁，不再等待被占用的重绑锁，
// 调用及时返回并调度一次后台恢复。
func TestEnsureUIMServiceContextEscalatesWithoutWaitingForRecoveryLock(t *testing.T) {
	m := newUIMContextTestManager()
	m.client = &qmi.Client{}
	m.cfg.RecoveryPolicy.ServiceTimeoutThreshold = 1
	m.uimRecoveryMu.Lock()
	defer m.uimRecoveryMu.Unlock()

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Millisecond)
	defer cancel()
	start := time.Now()
	_, err := withUIMRecoveryValueContext(m, ctx, "UIMPowerOnSIM", func(*qmi.UIMService) (struct{}, error) {
		t.Fatal("operation must not run without a service")
		return struct{}{}, nil
	})
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("withUIMRecoveryValueContext = %v, want DeadlineExceeded", err)
	}
	if waited := time.Since(start); waited > time.Second {
		t.Fatalf("escalation took %v while the recovery lock was held, want prompt return", waited)
	}
	if evt := waitInternalRecoveryEvent(t, m.eventCh, time.Second); evt != eventModemReset {
		t.Fatalf("scheduled %v, want eventModemReset", evt)
	}
}

// 操作本身真实到期（DeadlineExceeded）且达到阈值：不重绑、不重试，但仍调度一次后台恢复。
func TestWithUIMRecoveryContextSchedulesRecoveryOnRepeatedDeadline(t *testing.T) {
	m := newUIMContextTestManager()
	m.cfg.RecoveryPolicy.ServiceTimeoutThreshold = 1
	uim := &qmi.UIMService{}
	m.ensureUIMServiceHook = func() (*qmi.UIMService, error) { return uim, nil }
	var rebinds int
	m.rebindUIMServiceHook = func(reason string) (*qmi.UIMService, error) {
		rebinds++
		return uim, nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	err := m.withUIMRecoveryContext(ctx, "UIMPowerOnSIM", func(*qmi.UIMService) error {
		<-ctx.Done()
		return ctx.Err()
	})
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("withUIMRecoveryContext = %v, want the operation's DeadlineExceeded", err)
	}
	if rebinds != 0 {
		t.Fatalf("rebinds = %d, want 0 after the deadline", rebinds)
	}
	if evt := waitInternalRecoveryEvent(t, m.eventCh, time.Second); evt != eventModemReset {
		t.Fatalf("scheduled %v, want eventModemReset", evt)
	}
}

// 重绑后的指示注册重放继承调用方剩余预算：其上下文期限不晚于调用方期限。
func TestReplayUIMIndicationsAfterRebindInheritsCallerDeadline(t *testing.T) {
	m := newUIMContextTestManager()
	m.cfg.Timeouts.IndicationRegister = 5 * time.Second
	var seen time.Time
	var ok bool
	m.registerUIMIndications = func(ctx context.Context) (uint32, error) {
		seen, ok = ctx.Deadline()
		return 0, nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()
	callerDeadline, _ := ctx.Deadline()
	m.replayUIMIndicationsAfterRebind(ctx, &qmi.UIMService{}, "test")
	if !ok || seen.After(callerDeadline) {
		t.Fatalf("replay deadline = %v (set=%v), want no later than the caller deadline %v", seen, ok, callerDeadline)
	}

	// 调用方预算充足时仍受 IndicationRegister 上限约束。
	long, cancelLong := context.WithTimeout(context.Background(), time.Minute)
	defer cancelLong()
	m.replayUIMIndicationsAfterRebind(long, &qmi.UIMService{}, "test")
	if !ok || time.Until(seen) > 5*time.Second+100*time.Millisecond {
		t.Fatalf("replay deadline with a generous caller = %v, want capped by IndicationRegister (5s)", time.Until(seen))
	}
}

// lockContext：已取消的 ctx 即使锁空闲也不获取。
func TestLockContextRejectsCancelledContext(t *testing.T) {
	var mu sync.Mutex
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := lockContext(ctx, &mu); !errors.Is(err, context.Canceled) {
		t.Fatalf("lockContext(cancelled) = %v, want Canceled", err)
	}
	if !mu.TryLock() {
		t.Fatal("lockContext must not have acquired the mutex for a cancelled context")
	}
	mu.Unlock()
}

// 惰性分配阶段被调用方主动取消：返回 Canceled，不上报、不入队 core recovery。
func TestEnsureUIMServiceContextCancelDoesNotScheduleRecovery(t *testing.T) {
	m := newUIMContextTestManager()
	m.client = &qmi.Client{}
	m.cfg.RecoveryPolicy.ServiceTimeoutThreshold = 1
	m.uimRecoveryMu.Lock()
	defer m.uimRecoveryMu.Unlock()

	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(20 * time.Millisecond)
		cancel()
	}()
	_, err := withUIMRecoveryValueContext(m, ctx, "UIMPowerOnSIM", func(*qmi.UIMService) (struct{}, error) {
		t.Fatal("operation must not run without a service")
		return struct{}{}, nil
	})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("withUIMRecoveryValueContext = %v, want Canceled", err)
	}
	select {
	case evt := <-m.eventCh:
		t.Fatalf("caller cancellation during lazy allocation scheduled %v, want nothing", evt)
	case <-time.After(100 * time.Millisecond):
	}
}

// 重绑阶段被调用方主动取消：重绑错误原样返回，不入队 core recovery。
func TestWithUIMRecoveryContextRebindCancelDoesNotScheduleRecovery(t *testing.T) {
	m := newUIMContextTestManager()
	m.cfg.RecoveryPolicy.ServiceTimeoutThreshold = 1
	uim := &qmi.UIMService{}
	m.ensureUIMServiceHook = func() (*qmi.UIMService, error) { return uim, nil }
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	rebindErr := errors.New("allocate client ID aborted: context canceled")
	m.rebindUIMServiceHook = func(reason string) (*qmi.UIMService, error) {
		cancel()
		return nil, rebindErr
	}
	err := m.withUIMRecoveryContext(ctx, "UIMPowerOnSIM", func(*qmi.UIMService) error {
		return recoverableQMIError(qmi.ServiceUIM, qmi.UIMPowerOnSIM)
	})
	if !errors.Is(err, rebindErr) {
		t.Fatalf("withUIMRecoveryContext = %v, want the rebind error", err)
	}
	select {
	case evt := <-m.eventCh:
		t.Fatalf("caller cancellation during rebind scheduled %v, want nothing", evt)
	case <-time.After(100 * time.Millisecond):
	}
}

// 惰性分配因期限到期中止：错误仍带分配失败标识，无论默认阈值还是禁用普通超时恢复，都立即调度恢复；
// 同样文案但由调用方主动取消时不调度。
func TestEnsureUIMServiceAllocationDeadlineKeepsImmediateRecoveryPolicy(t *testing.T) {
	for _, disableTimeoutRecovery := range []bool{false, true} {
		t.Run(fmt.Sprintf("disable_timeout_recovery=%v", disableTimeoutRecovery), func(t *testing.T) {
			m := newUIMContextTestManager()
			m.cfg.RecoveryPolicy.DisableServiceTimeoutRecovery = disableTimeoutRecovery
			allocErr := fmt.Errorf("allocate UIM client failed: %s (aborted by caller after 1 attempt(s), last error: write failed): %w",
				qmi.AllocateClientIDFailedText, context.DeadlineExceeded)
			m.ensureUIMServiceHook = func() (*qmi.UIMService, error) { return nil, allocErr }
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			if _, err := withUIMRecoveryValueContext(m, ctx, "UIMPowerOnSIM", func(*qmi.UIMService) (struct{}, error) {
				t.Fatal("operation must not run")
				return struct{}{}, nil
			}); !errors.Is(err, allocErr) {
				t.Fatalf("withUIMRecoveryValueContext = %v, want the allocation error", err)
			}
			if evt := waitInternalRecoveryEvent(t, m.eventCh, time.Second); evt != eventModemReset {
				t.Fatalf("scheduled %v, want immediate eventModemReset for an allocation failure", evt)
			}
		})
	}

	m := newUIMContextTestManager()
	ctx, cancel := context.WithCancel(context.Background())
	cancelledErr := fmt.Errorf("allocate UIM client failed: %s (aborted by caller after 1 attempt(s), last error: write failed): %w",
		qmi.AllocateClientIDFailedText, context.Canceled)
	m.ensureUIMServiceHook = func() (*qmi.UIMService, error) {
		cancel()
		return nil, cancelledErr
	}
	if _, err := withUIMRecoveryValueContext(m, ctx, "UIMPowerOnSIM", func(*qmi.UIMService) (struct{}, error) {
		return struct{}{}, nil
	}); !errors.Is(err, cancelledErr) {
		t.Fatalf("withUIMRecoveryValueContext = %v, want the cancelled allocation error", err)
	}
	select {
	case evt := <-m.eventCh:
		t.Fatalf("caller cancellation with an allocation-failure text scheduled %v, want nothing", evt)
	case <-time.After(100 * time.Millisecond):
	}
}

// ensure 钩子在调用方取消后返回可恢复的 QMI 错误：取消守卫必须拦住上报，不入队恢复。
func TestEnsureUIMServiceCancelledWithRecoverableErrorDoesNotScheduleRecovery(t *testing.T) {
	m := newUIMContextTestManager()
	ctx, cancel := context.WithCancel(context.Background())
	recoverable := recoverableQMIError(qmi.ServiceControl, qmi.CTLGetClientID)
	m.ensureUIMServiceHook = func() (*qmi.UIMService, error) {
		cancel()
		return nil, recoverable
	}
	if _, err := withUIMRecoveryValueContext(m, ctx, "UIMPowerOnSIM", func(*qmi.UIMService) (struct{}, error) {
		return struct{}{}, nil
	}); !errors.Is(err, recoverable) {
		t.Fatalf("withUIMRecoveryValueContext = %v, want the recoverable error", err)
	}
	select {
	case evt := <-m.eventCh:
		t.Fatalf("cancelled caller with a recoverable ensure error scheduled %v, want nothing", evt)
	case <-time.After(100 * time.Millisecond):
	}
}
