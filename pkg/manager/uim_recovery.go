package manager

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/ljh740/quectel-qmi-go/pkg/qmi"
)

func (m *Manager) withUIMRecovery(op string, fn func(uim *qmi.UIMService) error) error {
	return m.withUIMRecoveryContext(context.Background(), op, fn)
}

// withUIMRecoveryContext 与 withUIMRecovery 相同，但服务惰性分配、重绑及其锁等待都受 ctx 期限约束：
// 调用方给出的期限覆盖整个操作，而不只覆盖最终的 QMI 请求。
// withUIMRecoveryContext bounds lazy allocation, rebind and their lock waits by ctx as well as the request itself.
func (m *Manager) withUIMRecoveryContext(ctx context.Context, op string, fn func(uim *qmi.UIMService) error) error {
	_, err := withUIMRecoveryValueContext(m, ctx, op, func(uim *qmi.UIMService) (struct{}, error) {
		return struct{}{}, fn(uim)
	})
	return err
}

func withUIMRecoveryValue[T any](m *Manager, op string, fn func(uim *qmi.UIMService) (T, error)) (T, error) {
	return withUIMRecoveryValueContext(m, context.Background(), op, fn)
}

func withUIMRecoveryValueContext[T any](m *Manager, ctx context.Context, op string, fn func(uim *qmi.UIMService) (T, error)) (T, error) {
	var zero T
	if ctx == nil {
		ctx = context.Background()
	}

	uim, err := m.ensureUIMServiceContext(ctx)
	if err != nil {
		if m.shouldRecoverUIMError(op, err) {
			m.triggerCoreRecoveryFromService("UIM", op, "initial", err)
		}
		return zero, err
	}

	result, err := fn(uim)
	if err == nil {
		m.noteServiceOperationSuccess("UIM", op)
		return result, nil
	}
	if !m.shouldRecoverUIMError(op, err) {
		return result, err
	}
	// 期限已到就不再开始重绑：重绑与重试都超出了调用方允许的时间。真实到期（DeadlineExceeded）且已达
	// 恢复阈值时仍调度一次后台 core recovery，避免 UIM 持续无响应却永远得不到恢复；主动取消不触发。
	if ctxErr := ctx.Err(); ctxErr != nil {
		if errors.Is(ctxErr, context.DeadlineExceeded) {
			m.logServiceRecovery("UIM", op, "deadline", err, "UIM operation failed and the caller deadline expired; scheduling core recovery instead of rebinding")
			m.triggerCoreRecoveryFromService("UIM", op, "deadline", err)
		} else {
			m.logServiceRecovery("UIM", op, "initial", err, "UIM operation failed; skipping rebind because the caller cancelled")
		}
		return result, err
	}

	m.logServiceRecovery("UIM", op, "initial", err, "UIM operation failed; rebinding UIM service")

	if lockErr := lockContext(ctx, &m.uimRecoveryMu); lockErr != nil {
		m.logServiceRecovery("UIM", op, "rebind", lockErr, "UIM rebind skipped: deadline expired while waiting for the recovery lock")
		return zero, fmt.Errorf("%s: UIM rebind skipped: %w (initial=%v)", op, lockErr, err)
	}
	uim, rebindErr := m.rebindUIMServiceContext(ctx, "recover:"+op)
	m.uimRecoveryMu.Unlock()
	if rebindErr != nil {
		m.logServiceRecovery("UIM", op, "rebind", rebindErr, "UIM service rebind failed")
		m.triggerCoreRecoveryFromService("UIM", op, "rebind", rebindErr)
		return zero, fmt.Errorf("%s: UIM rebind failed: %w (initial=%v)", op, rebindErr, err)
	}

	retryResult, retryErr := fn(uim)
	if retryErr == nil {
		m.noteServiceOperationSuccess("UIM", op)
		m.log.WithField("service_name", "UIM").WithField("op", op).WithField("phase", "retry").Info("UIM operation recovered after rebind")
		return retryResult, nil
	}
	if m.shouldRecoverUIMError(op, retryErr) {
		m.logServiceRecovery("UIM", op, "retry", retryErr, "UIM operation still failing after rebind")
		m.triggerCoreRecoveryFromService("UIM", op, "retry", retryErr)
	}
	return retryResult, retryErr
}

// lockContext 在 ctx 期限内获取 mu；没有期限/取消信号的 ctx 直接阻塞等待。
// lockContext acquires mu, giving up when ctx expires; contexts without Done block like Lock.
func lockContext(ctx context.Context, mu *sync.Mutex) error {
	if ctx.Done() == nil {
		mu.Lock()
		return nil
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if mu.TryLock() {
		return nil
	}
	ticker := time.NewTicker(5 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			if mu.TryLock() {
				return nil
			}
		}
	}
}

func (m *Manager) ensureUIMService() (*qmi.UIMService, error) {
	return m.ensureUIMServiceContext(context.Background())
}

// ensureUIMServiceContext 返回已分配的 UIM 服务，必要时在 ctx 期限内惰性分配（含锁等待与 CTL 分配请求）。
func (m *Manager) ensureUIMServiceContext(ctx context.Context) (*qmi.UIMService, error) {
	if m == nil {
		return nil, ErrServiceNotReady("UIM")
	}
	if m.ensureUIMServiceHook != nil {
		return m.ensureUIMServiceHook()
	}
	if ctx == nil {
		ctx = context.Background()
	}

	m.mu.RLock()
	uim := m.uim
	client := m.client
	m.mu.RUnlock()
	if uim != nil {
		return uim, nil
	}
	if client == nil {
		return nil, ErrServiceNotReady("UIM")
	}

	if err := lockContext(ctx, &m.uimRecoveryMu); err != nil {
		return nil, fmt.Errorf("allocate UIM client: %w", err)
	}
	defer m.uimRecoveryMu.Unlock()

	m.mu.RLock()
	uim = m.uim
	client = m.client
	m.mu.RUnlock()
	if uim != nil {
		return uim, nil
	}
	if client == nil {
		return nil, ErrServiceNotReady("UIM")
	}

	allocated, err := qmi.NewUIMServiceWithContext(ctx, client)
	if err != nil {
		return nil, fmt.Errorf("allocate UIM client failed: %w", err)
	}

	m.mu.Lock()
	if m.client != client {
		m.mu.Unlock()
		m.discardUIMServiceContext(ctx, allocated, "lazy_allocate_client_replaced")
		return nil, ErrServiceNotReady("UIM")
	}
	m.uim = allocated
	m.mu.Unlock()
	m.log.Info("UIM service lazily allocated")
	return allocated, nil
}

// discardUIMServiceContext 在 m.mu 外、用调用方剩余预算释放分配期间已作废的 UIM 客户端；释放失败只记录。
func (m *Manager) discardUIMServiceContext(ctx context.Context, svc *qmi.UIMService, reason string) {
	if svc == nil {
		return
	}
	if err := svc.CloseWithContext(ctx); err != nil {
		m.log.WithError(err).WithField("reason", reason).Warn("Releasing a superseded UIM client failed")
	}
}

func (m *Manager) rebindUIMService(reason string) (*qmi.UIMService, error) {
	return m.rebindUIMServiceContext(context.Background(), reason)
}

// rebindUIMServiceContext 在 ctx 期限内释放旧 UIM 客户端并重新分配；调用方须持有 uimRecoveryMu。
func (m *Manager) rebindUIMServiceContext(ctx context.Context, reason string) (*qmi.UIMService, error) {
	if m == nil {
		return nil, ErrServiceNotReady("UIM")
	}
	if m.rebindUIMServiceHook != nil {
		return m.rebindUIMServiceHook(reason)
	}

	m.mu.Lock()
	prev := m.uim
	client := m.client
	m.uim = nil
	m.mu.Unlock()

	if prev != nil {
		if err := prev.CloseWithContext(ctx); err != nil {
			m.log.WithError(err).WithField("reason", reason).Warn("Closing previous UIM client failed during rebind")
		}
	}
	if client == nil {
		return nil, ErrServiceNotReady("UIM")
	}
	if err := ctx.Err(); err != nil {
		return nil, fmt.Errorf("allocate UIM client: %w", err)
	}

	allocated, err := qmi.NewUIMServiceWithContext(ctx, client)
	if err != nil {
		return nil, fmt.Errorf("allocate UIM client failed: %w", err)
	}

	m.mu.Lock()
	if m.client != client {
		m.mu.Unlock()
		m.discardUIMServiceContext(ctx, allocated, "rebind_client_replaced")
		return nil, ErrServiceNotReady("UIM")
	}
	m.uim = allocated
	m.mu.Unlock()

	m.replayUIMIndicationsAfterRebind(ctx, allocated, reason)

	m.log.WithField("reason", reason).Info("UIM service rebound")
	return allocated, nil
}

// replayUIMIndicationsAfterRebind 在重绑后重放指示注册：继承调用方剩余预算，并以 IndicationRegister 超时为上限，
// 不额外延长总期限；失败只记录。
func (m *Manager) replayUIMIndicationsAfterRebind(ctx context.Context, uim *qmi.UIMService, reason string) {
	registerCtx, cancel := contextWithMaxTimeout(ctx, m.cfg.Timeouts.IndicationRegister)
	defer cancel()
	acceptedMask, registerErr := m.registerUIMIndicationsWithContext(registerCtx, uim)
	if registerErr != nil {
		m.log.WithField("reason", reason).WithError(registerErr).Warn("Failed to replay UIM indication registration after rebind")
		return
	}
	m.log.WithField("reason", reason).WithField("requested_mask", m.uimIndicationRegistrationMask()).WithField("accepted_mask", acceptedMask).Info("Replayed UIM indication registration after rebind")
}

func (m *Manager) shouldRecoverUIMError(op string, err error) bool {
	return m.shouldRecoverServiceOperationError("UIM", op, err, "uim service not available")
}

func (m *Manager) triggerCoreRecoveryFromUIM(op string, phase string, cause error) {
	m.triggerCoreRecoveryFromService("UIM", op, phase, cause)
}

func (m *Manager) enqueueModemResetEvent(source string) {
	m.enqueueModemResetEventOpts(source, false)
}

// enqueueModemResetEventOpts 投递一次 modem reset 恢复事件。
// guaranteed 为 true（显式恢复请求）时不受去抖窗口影响：已有恢复在途则合并为待处理（在途恢复结束后再跑一次），
// 已有事件排队则由该事件的尝试代为服务，否则一定入队；因此显式请求总能对应到一次在其之后启动的恢复尝试。
// Explicit (guaranteed) requests bypass the debounce window so that every issued recovery ticket is
// eventually served by an attempt that starts after it.
func (m *Manager) enqueueModemResetEventOpts(source string, guaranteed bool) {
	if m == nil {
		return
	}
	m.resetEvents.Add(1)
	if !m.claimModemResetQueueSlot(source, guaranteed) {
		return
	}
	m.dispatchQueuedModemReset(source)
}

// claimModemResetQueueSlot 在锁内决定事件是被合并还是占用唯一的排队位；返回 true 表示 modemResetQueued 已置位，
// 调用方必须投递该事件。恢复在途时转为待处理；已有事件排队或落入去抖窗口（guaranteed 除外）时合并。
// claimModemResetQueueSlot decides under the lock whether the event coalesces or takes the single queued slot.
func (m *Manager) claimModemResetQueueSlot(source string, guaranteed bool) bool {
	now := time.Now()
	m.modemResetMu.Lock()
	coalesced := ""
	switch {
	case m.modemResetRecovering:
		m.modemResetPending = true
		coalesced = "Coalesced modem-reset event while recovery is running"
	case m.modemResetQueued:
		coalesced = "Coalesced modem-reset event: another reset event is already queued"
	case !guaranteed && !m.modemResetEnqueuedAt.IsZero() && now.Sub(m.modemResetEnqueuedAt) < m.modemResetDedupWindow:
		coalesced = "Deduplicated modem-reset event inside debounce window"
	default:
		m.modemResetEnqueuedAt = now
		m.modemResetQueued = true
	}
	if coalesced != "" {
		m.resetCoalesced.Add(1)
	}
	m.modemResetMu.Unlock()
	if coalesced != "" {
		m.log.WithField("source", source).Debug(coalesced)
		return false
	}
	return true
}

// dispatchQueuedModemReset 投递已占位的事件。事件队列满时延后重试，重试期间排队位保持，IsCoreRecovering 持续为 true；
// 重试时若恢复已在途，占位在同一临界区内转为待处理，由恢复结束后原子转回排队。
// dispatchQueuedModemReset delivers a claimed event, retrying later while keeping the queued slot when the queue is full.
func (m *Manager) dispatchQueuedModemReset(source string) {
	m.dispatchQueuedModemResetWithRetry(source, 200*time.Millisecond)
}

func (m *Manager) dispatchQueuedModemResetWithRetry(source string, retryDelay time.Duration) {
	select {
	case m.eventCh <- eventModemReset:
		return
	default:
	}
	m.log.WithField("source", source).Warn("Internal event queue is full; scheduling deferred modem-reset event")
	m.scheduleAfter(retryDelay, func() {
		m.modemResetMu.Lock()
		if m.modemResetRecovering {
			m.modemResetQueued = false
			m.modemResetPending = true
			m.resetCoalesced.Add(1)
			m.modemResetMu.Unlock()
			m.log.WithField("source", source).Debug("Deferred modem-reset event folded into the running recovery")
			return
		}
		m.modemResetMu.Unlock()
		m.dispatchQueuedModemResetWithRetry(source+"_deferred_retry", 500*time.Millisecond)
	})
}

func (m *Manager) logUIMRecovery(op string, phase string, err error, message string) {
	m.logServiceRecovery("UIM", op, phase, err, message)
}
