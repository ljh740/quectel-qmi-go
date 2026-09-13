package manager

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"
)

// ErrCoreRecoveryExhausted 表示 core recovery 已耗尽重试、不会再有新的恢复尝试。
// ErrCoreRecoveryExhausted reports that core recovery gave up and no further attempt will run.
var ErrCoreRecoveryExhausted = errors.New("qmi core recovery exhausted")

// ErrCoreRecoveryStopped 表示 Manager 正在停止，恢复不会再完成。
// ErrCoreRecoveryStopped reports that the manager is stopping and recovery will not complete.
var ErrCoreRecoveryStopped = errors.New("qmi manager stopping")

// coreRecoveryTicketState 把显式恢复请求与恢复尝试对应起来：
//   - issue 为每个请求发放递增票号；
//   - 每次恢复尝试启动时记录 serving = 当时已发放的最大票号（该尝试服务此前的全部请求）；
//   - 尝试成功时 completed = serving；
//   - 重试耗尽时置 exhausted，下一次尝试启动时清除。
//
// coreRecoveryTicketState maps explicit recovery requests to recovery attempts: every request gets an
// increasing ticket, an attempt serves all tickets issued before it started, and a successful attempt
// completes the tickets it served.
type coreRecoveryTicketState struct {
	mu        sync.Mutex
	issued    uint64
	serving   uint64
	completed uint64
	exhausted bool
}

func (s *coreRecoveryTicketState) issue() uint64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.issued++
	return s.issued
}

func (s *coreRecoveryTicketState) servingTicket() uint64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.serving
}

func (s *coreRecoveryTicketState) beginAttempt() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.serving = s.issued
	s.exhausted = false
}

func (s *coreRecoveryTicketState) completeAttempt() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.serving > s.completed {
		s.completed = s.serving
	}
}

func (s *coreRecoveryTicketState) markExhausted() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.exhausted = true
}

func (s *coreRecoveryTicketState) snapshot() (issued, serving, completed uint64, exhausted bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.issued, s.serving, s.completed, s.exhausted
}

// RequestCoreRecoveryTicket 请求一次 core recovery 并返回可等待的票号。
// scheduled 为 true 时本次请求已入队（或合并进排队/在途恢复之后的待处理恢复），票号由此后启动的恢复尝试服务；
// core 未就绪（已有恢复在途或等待重试）时不再入队，票号沿用当前尝试服务的票号，由它及其重试代为完成，scheduled 为 false；
// Manager 正在停止或已停止时返回 (0, false)。票号 0 表示没有对应的恢复尝试，WaitCoreRecoveryTicket(0) 只等待空闲且就绪。
//
// RequestCoreRecoveryTicket requests a core recovery and returns a ticket that WaitCoreRecoveryTicket can
// wait on. Explicit requests bypass the debounce window, so a scheduled ticket is always served by an
// attempt that starts after the request.
func (m *Manager) RequestCoreRecoveryTicket(reason string) (ticket uint64, scheduled bool) {
	if m == nil {
		return 0, false
	}
	reason = strings.TrimSpace(reason)
	if reason == "" {
		reason = "external_request"
	}

	m.mu.RLock()
	coreReady := m.coreReady
	stopped := m.coreStoppedLocked()
	m.mu.RUnlock()
	if stopped {
		return 0, false
	}
	cause := fmt.Errorf("%s", reason)
	if !coreReady {
		ticket = m.coreRecoveryTickets.servingTicket()
		m.logServiceRecovery("POST_SWITCH", reason, "recover-core", cause, "Core recovery already in progress; request joins the in-flight recovery")
		return ticket, false
	}

	ticket = m.coreRecoveryTickets.issue()
	m.logServiceRecovery("POST_SWITCH", reason, "recover-core", cause, "Scheduling core recovery due to explicit request")
	m.enqueueModemResetEventOpts("post_switch_recovery", true)
	return ticket, true
}

// IsCoreRecovering 报告是否有 core recovery 在途、待处理或已排队。
// IsCoreRecovering reports whether a core recovery attempt is running, pending, or queued.
func (m *Manager) IsCoreRecovering() bool {
	if m == nil {
		return false
	}
	m.modemResetMu.Lock()
	defer m.modemResetMu.Unlock()
	return m.modemResetRecovering || m.modemResetPending || m.modemResetQueued
}

// CoreRecoveryTicketStatus 返回票号状态：已发放、当前尝试服务、已完成的最大票号，以及是否已耗尽。
// CoreRecoveryTicketStatus exposes the ticket bookkeeping for observability and tests.
func (m *Manager) CoreRecoveryTicketStatus() (issued, serving, completed uint64, exhausted bool) {
	if m == nil {
		return 0, 0, 0, false
	}
	return m.coreRecoveryTickets.snapshot()
}

// coreRecoveryWaitState 是等待判定用的一致快照。
// coreRecoveryWaitState is the consistent snapshot WaitCoreRecoveryTicket decides on.
type coreRecoveryWaitState struct {
	serving    uint64
	completed  uint64
	exhausted  bool
	ready      bool
	stopped    bool
	recovering bool
	stage      string
	lastErr    string
}

// coreRecoveryWaitSnapshot 在 modemResetMu 内一次读取恢复状态、就绪标志与票号：恢复尝试的开始与结束都在
// modemResetMu 内切换 recovering，因此 recovering 为 false 时读到的 ready/completed 不会来自一次进行到一半的尝试。
// 锁序：modemResetMu → mu → coreRecoveryTickets.mu；其他路径不会在持有 mu 或票号锁时再取 modemResetMu。
// coreRecoveryWaitSnapshot reads recovery flags, readiness and tickets under modemResetMu so they describe one instant.
func (m *Manager) coreRecoveryWaitSnapshot() coreRecoveryWaitState {
	m.modemResetMu.Lock()
	defer m.modemResetMu.Unlock()
	st := coreRecoveryWaitState{recovering: m.modemResetRecovering || m.modemResetPending || m.modemResetQueued}
	m.mu.RLock()
	st.ready = m.coreReady
	st.stopped = m.coreStoppedLocked()
	st.stage = m.coreReadyStage
	st.lastErr = m.coreReadyLastErr
	m.mu.RUnlock()
	_, st.serving, st.completed, st.exhausted = m.coreRecoveryTickets.snapshot()
	return st
}

// WaitCoreRecoveryTicket 等待票号对应的恢复完成：已完成的票号不小于 ticket、没有恢复在途/待处理/排队，且 core 就绪。
// 恢复耗尽返回 ErrCoreRecoveryExhausted，Manager 正在停止或已停止返回 ErrCoreRecoveryStopped，ctx 到期返回带状态的错误。
//
// WaitCoreRecoveryTicket blocks until the attempt serving the ticket succeeded, no further recovery is
// running, pending or queued, and the core is ready.
func (m *Manager) WaitCoreRecoveryTicket(ctx context.Context, ticket uint64) error {
	if m == nil {
		return errors.New("qmi manager is nil")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	start := time.Now()
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	for {
		st := m.coreRecoveryWaitSnapshot()
		if st.stopped {
			return fmt.Errorf("等待 QMI core recovery 完成失败 (ticket=%d): %w", ticket, ErrCoreRecoveryStopped)
		}
		if !st.recovering {
			if st.completed >= ticket && st.ready {
				return nil
			}
			if st.exhausted {
				return fmt.Errorf("等待 QMI core recovery 完成失败 (ticket=%d completed=%d last_err=%s): %w",
					ticket, st.completed, strings.TrimSpace(st.lastErr), ErrCoreRecoveryExhausted)
			}
		}
		select {
		case <-ctx.Done():
			stage := st.stage
			if stage == "" {
				stage = "unknown"
			}
			return fmt.Errorf(
				"等待 QMI core recovery 完成超时 (ticket=%d serving=%d completed=%d recovering=%v ready=%v stage=%s waited=%s last_err=%s): %w",
				ticket, st.serving, st.completed, st.recovering, st.ready, stage,
				time.Since(start).Round(time.Millisecond), strings.TrimSpace(st.lastErr), ctx.Err(),
			)
		case <-ticker.C:
		}
	}
}
