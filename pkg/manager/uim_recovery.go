package manager

import (
	"fmt"
	"time"

	"github.com/ljh740/quectel-qmi-go/pkg/qmi"
)

func (m *Manager) withUIMRecovery(op string, fn func(uim *qmi.UIMService) error) error {
	_, err := withUIMRecoveryValue(m, op, func(uim *qmi.UIMService) (struct{}, error) {
		return struct{}{}, fn(uim)
	})
	return err
}

func withUIMRecoveryValue[T any](m *Manager, op string, fn func(uim *qmi.UIMService) (T, error)) (T, error) {
	var zero T

	uim, err := m.ensureUIMService()
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

	m.logServiceRecovery("UIM", op, "initial", err, "UIM operation failed; rebinding UIM service")

	m.uimRecoveryMu.Lock()
	uim, rebindErr := m.rebindUIMService("recover:" + op)
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

func (m *Manager) ensureUIMService() (*qmi.UIMService, error) {
	if m == nil {
		return nil, ErrServiceNotReady("UIM")
	}
	if m.ensureUIMServiceHook != nil {
		return m.ensureUIMServiceHook()
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

	m.uimRecoveryMu.Lock()
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

	allocated, err := qmi.NewUIMService(client)
	if err != nil {
		return nil, fmt.Errorf("allocate UIM client failed: %w", err)
	}

	m.mu.Lock()
	defer m.mu.Unlock()
	if m.client != client {
		_ = allocated.Close()
		return nil, ErrServiceNotReady("UIM")
	}
	m.uim = allocated
	m.log.Info("UIM service lazily allocated")
	return allocated, nil
}

func (m *Manager) rebindUIMService(reason string) (*qmi.UIMService, error) {
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
		if err := prev.Close(); err != nil {
			m.log.WithError(err).WithField("reason", reason).Warn("Closing previous UIM client failed during rebind")
		}
	}
	if client == nil {
		return nil, ErrServiceNotReady("UIM")
	}

	allocated, err := qmi.NewUIMService(client)
	if err != nil {
		return nil, fmt.Errorf("allocate UIM client failed: %w", err)
	}

	m.mu.Lock()
	if m.client != client {
		m.mu.Unlock()
		_ = allocated.Close()
		return nil, ErrServiceNotReady("UIM")
	}
	m.uim = allocated
	m.mu.Unlock()

	ctx, cancel := m.opContext(m.cfg.Timeouts.IndicationRegister)
	acceptedMask, registerErr := m.registerUIMIndicationsWithContext(ctx, allocated)
	cancel()
	if registerErr != nil {
		m.log.WithField("reason", reason).WithError(registerErr).Warn("Failed to replay UIM indication registration after rebind")
	} else {
		m.log.WithField("reason", reason).WithField("requested_mask", m.uimIndicationRegistrationMask()).WithField("accepted_mask", acceptedMask).Info("Replayed UIM indication registration after rebind")
	}

	m.log.WithField("reason", reason).Info("UIM service rebound")
	return allocated, nil
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
