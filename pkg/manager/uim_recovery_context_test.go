package manager

import (
	"context"
	"errors"
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
