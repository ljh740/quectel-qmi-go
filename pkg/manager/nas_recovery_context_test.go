package manager

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/ljh740/quectel-qmi-go/pkg/qmi"
)

func newNASContextTestManager() *Manager {
	m := newRecoveryTestManager()
	m.coreReady = true
	m.state = StateDisconnected
	return m
}

// 惰性分配前的锁等待受 ctx 期限约束：恢复锁被占用时在期限内返回 ctx 错误，不会一直等。
func TestEnsureNASServiceContextGivesUpLockWaitAtDeadline(t *testing.T) {
	m := newNASContextTestManager()
	m.client = &qmi.Client{}
	m.nasRecoveryMu.Lock()
	defer m.nasRecoveryMu.Unlock()

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Millisecond)
	defer cancel()
	start := time.Now()
	_, err := m.ensureNASServiceContext(ctx)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("ensureNASServiceContext with the lock held = %v, want DeadlineExceeded", err)
	}
	if waited := time.Since(start); waited > time.Second {
		t.Fatalf("ensureNASServiceContext waited %v, want to give up at the 60ms deadline", waited)
	}
}

// 操作失败后重绑前先看期限：期限已到不再重绑、不再重试，返回原始错误，且主动取消不调度恢复。
func TestWithNASRecoveryContextSkipsRebindWhenDeadlineExpired(t *testing.T) {
	m := newNASContextTestManager()
	nas := &qmi.NASService{}
	m.ensureNASServiceHook = func() (*qmi.NASService, error) { return nas, nil }
	var rebinds int
	m.rebindNASServiceHook = func(reason string) (*qmi.NASService, error) {
		rebinds++
		return nas, nil
	}
	ctx, cancel := context.WithCancel(context.Background())
	opErr := recoverableQMIError(qmi.ServiceNAS, qmi.NASInitiateNetworkRegister)
	calls := 0
	err := m.withNASRecoveryContext(ctx, "NASInitiateNetworkRegister", func(*qmi.NASService) error {
		calls++
		cancel() // 操作本身耗尽了调用方的期限
		return opErr
	})
	if !errors.Is(err, opErr) {
		t.Fatalf("withNASRecoveryContext = %v, want the original operation error", err)
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
func TestWithNASRecoveryContextGivesUpRebindLockAtDeadline(t *testing.T) {
	m := newNASContextTestManager()
	nas := &qmi.NASService{}
	m.ensureNASServiceHook = func() (*qmi.NASService, error) { return nas, nil }
	var rebinds int
	m.rebindNASServiceHook = func(reason string) (*qmi.NASService, error) {
		rebinds++
		return nas, nil
	}
	m.nasRecoveryMu.Lock()
	defer m.nasRecoveryMu.Unlock()

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Millisecond)
	defer cancel()
	opErr := recoverableQMIError(qmi.ServiceNAS, qmi.NASInitiateNetworkRegister)
	start := time.Now()
	err := m.withNASRecoveryContext(ctx, "NASInitiateNetworkRegister", func(*qmi.NASService) error { return opErr })
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("withNASRecoveryContext with the recovery lock held = %v, want DeadlineExceeded", err)
	}
	if waited := time.Since(start); waited > time.Second {
		t.Fatalf("waited %v for the recovery lock, want to give up at the deadline", waited)
	}
	if rebinds != 0 {
		t.Fatalf("rebinds = %d, want 0 when the lock wait times out", rebinds)
	}
}

// 期限充足时行为不变：失败→重绑→重试成功。
func TestWithNASRecoveryContextStillRebindsWithinDeadline(t *testing.T) {
	m := newNASContextTestManager()
	first := &qmi.NASService{}
	rebound := &qmi.NASService{}
	m.ensureNASServiceHook = func() (*qmi.NASService, error) { return first, nil }
	m.rebindNASServiceHook = func(reason string) (*qmi.NASService, error) { return rebound, nil }
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	var seen []*qmi.NASService
	err := m.withNASRecoveryContext(ctx, "NASInitiateNetworkRegister", func(n *qmi.NASService) error {
		seen = append(seen, n)
		if n == first {
			return recoverableQMIError(qmi.ServiceNAS, qmi.NASInitiateNetworkRegister)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("withNASRecoveryContext = %v, want success after rebind", err)
	}
	if len(seen) != 2 || seen[0] != first || seen[1] != rebound {
		t.Fatalf("attempts = %v, want first then rebound", seen)
	}
}

// 真实到期（非主动取消）且达到超时阈值：不重绑、不重试，但调度一次 core recovery，避免 NAS 持续无响应得不到恢复。
func TestWithNASRecoveryContextSchedulesRecoveryOnRepeatedDeadline(t *testing.T) {
	m := newNASContextTestManager()
	m.cfg.RecoveryPolicy.ServiceTimeoutThreshold = 1
	nas := &qmi.NASService{}
	m.ensureNASServiceHook = func() (*qmi.NASService, error) { return nas, nil }
	var rebinds int
	m.rebindNASServiceHook = func(reason string) (*qmi.NASService, error) {
		rebinds++
		return nas, nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	err := m.withNASRecoveryContext(ctx, "NASInitiateNetworkRegister", func(*qmi.NASService) error {
		<-ctx.Done()
		return ctx.Err()
	})
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("withNASRecoveryContext = %v, want the operation's DeadlineExceeded", err)
	}
	if rebinds != 0 {
		t.Fatalf("rebinds = %d, want 0 after the deadline", rebinds)
	}
	if evt := waitInternalRecoveryEvent(t, m.eventCh, time.Second); evt != eventModemReset {
		t.Fatalf("scheduled %v, want eventModemReset", evt)
	}
}
