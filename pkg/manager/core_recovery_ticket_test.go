package manager

import (
	"context"
	"errors"
	"testing"
	"time"
)

func newTicketTestManager(t *testing.T) *Manager {
	t.Helper()
	m := newRecoveryTestManager()
	m.afterFunc = func(_ time.Duration, fn func()) *time.Timer { return time.NewTimer(time.Hour) }
	m.openClientAndAllocateServicesHook = func(context.Context) error { return nil }
	m.checkSIMHook = func() error { return nil }
	m.modemResetQuietWindow = 5 * time.Millisecond
	m.getICCIDStrictHook = func(ctx context.Context) (string, error) { return "iccid", nil }
	m.mu.Lock()
	m.markCoreReadyLocked("test")
	m.mu.Unlock()
	return m
}

func drainModemResetEvent(t *testing.T, m *Manager) {
	t.Helper()
	select {
	case evt := <-m.eventCh:
		if evt != eventModemReset {
			t.Fatalf("queued event = %v, want eventModemReset", evt)
		}
	case <-time.After(time.Second):
		t.Fatal("no modem reset event was queued")
	}
}

func startWait(m *Manager, ticket uint64) <-chan error {
	done := make(chan error, 1)
	go func() { done <- m.WaitCoreRecoveryTicket(context.Background(), ticket) }()
	return done
}

func assertWaitBlocked(t *testing.T, done <-chan error, msg string) {
	t.Helper()
	select {
	case err := <-done:
		t.Fatalf("%s (returned %v)", msg, err)
	case <-time.After(150 * time.Millisecond):
	}
}

func awaitWait(t *testing.T, done <-chan error) error {
	t.Helper()
	select {
	case err := <-done:
		return err
	case <-time.After(5 * time.Second):
		t.Fatal("WaitCoreRecoveryTicket did not return")
		return nil
	}
}

// 显式请求发放票号并入队；随后启动的尝试服务该票号，成功后等待返回。
func TestRequestCoreRecoveryTicketIsServedByNextAttempt(t *testing.T) {
	m := newTicketTestManager(t)

	ticket, scheduled := m.RequestCoreRecoveryTicket("post_switch")
	if !scheduled || ticket != 1 {
		t.Fatalf("RequestCoreRecoveryTicket = (%d, %v), want (1, true)", ticket, scheduled)
	}
	if !m.IsCoreRecovering() {
		t.Fatal("a queued reset event should count as recovering")
	}
	done := startWait(m, ticket)
	assertWaitBlocked(t, done, "wait returned before the attempt ran")

	drainModemResetEvent(t, m)
	m.handleModemResetEvent()

	if err := awaitWait(t, done); err != nil {
		t.Fatalf("WaitCoreRecoveryTicket = %v, want nil", err)
	}
	if issued, serving, completed, exhausted := m.CoreRecoveryTicketStatus(); issued != 1 || serving != 1 || completed != 1 || exhausted {
		t.Fatalf("ticket status = (%d, %d, %d, %v), want (1, 1, 1, false)", issued, serving, completed, exhausted)
	}
	if m.IsCoreRecovering() {
		t.Fatal("nothing should be recovering after the attempt completed")
	}
}

// 等待期间尝试仍在进行（core 未就绪）不得返回；尝试成功后返回。
func TestWaitCoreRecoveryTicketBlocksWhileAttemptRuns(t *testing.T) {
	m := newTicketTestManager(t)
	release := make(chan struct{})
	entered := make(chan struct{}, 1)
	m.openClientAndAllocateServicesHook = func(context.Context) error {
		entered <- struct{}{}
		<-release
		return nil
	}
	ticket, _ := m.RequestCoreRecoveryTicket("post_switch")
	drainModemResetEvent(t, m)
	attemptDone := make(chan struct{})
	go func() { m.handleModemResetEvent(); close(attemptDone) }()
	<-entered

	done := startWait(m, ticket)
	assertWaitBlocked(t, done, "wait returned while the attempt was still running")
	close(release)
	<-attemptDone
	if err := awaitWait(t, done); err != nil {
		t.Fatalf("WaitCoreRecoveryTicket = %v, want nil", err)
	}
}

// core 未就绪时的请求不入队：票号沿用在途尝试服务的票号，由它完成。
func TestRequestCoreRecoveryTicketJoinsInFlightAttempt(t *testing.T) {
	m := newTicketTestManager(t)
	release := make(chan struct{})
	entered := make(chan struct{}, 1)
	m.openClientAndAllocateServicesHook = func(context.Context) error {
		entered <- struct{}{}
		<-release
		return nil
	}
	first, _ := m.RequestCoreRecoveryTicket("first")
	drainModemResetEvent(t, m)
	attemptDone := make(chan struct{})
	go func() { m.handleModemResetEvent(); close(attemptDone) }()
	<-entered // core 已标为未就绪

	joined, scheduled := m.RequestCoreRecoveryTicket("second")
	if scheduled || joined != first {
		t.Fatalf("RequestCoreRecoveryTicket while recovering = (%d, %v), want (%d, false)", joined, scheduled, first)
	}
	select {
	case evt := <-m.eventCh:
		t.Fatalf("unexpected event %v queued for a joined request", evt)
	default:
	}
	done := startWait(m, joined)
	assertWaitBlocked(t, done, "wait returned before the in-flight attempt completed")
	close(release)
	<-attemptDone
	if err := awaitWait(t, done); err != nil {
		t.Fatalf("WaitCoreRecoveryTicket = %v, want nil", err)
	}
}

// 恢复在途但 core 仍显示就绪（启动窗口）时的请求被合并为待处理：在途尝试的成功不算，待处理尝试完成后才返回。
func TestRequestCoreRecoveryTicketCoalescedIntoPendingAttempt(t *testing.T) {
	m := newTicketTestManager(t)
	first, _ := m.RequestCoreRecoveryTicket("first")
	drainModemResetEvent(t, m)
	// 模拟事件循环取走事件、第一次尝试刚启动、尚未把 core 标为未就绪。
	m.coreRecoveryTickets.beginAttempt()
	m.modemResetMu.Lock()
	m.modemResetQueued = false
	m.modemResetRecovering = true
	m.modemResetMu.Unlock()

	second, scheduled := m.RequestCoreRecoveryTicket("second")
	if !scheduled || second != first+1 {
		t.Fatalf("RequestCoreRecoveryTicket during start window = (%d, %v), want (%d, true)", second, scheduled, first+1)
	}
	m.modemResetMu.Lock()
	pending := m.modemResetPending
	m.modemResetMu.Unlock()
	if !pending {
		t.Fatal("request during a running attempt should become a pending recovery")
	}

	done := startWait(m, second)
	// 第一次尝试成功：只完成 first，待处理恢复尚未运行。
	m.coreRecoveryTickets.completeAttempt()
	m.modemResetMu.Lock()
	m.modemResetRecovering = false
	m.modemResetMu.Unlock()
	assertWaitBlocked(t, done, "wait returned on the in-flight attempt's success while a pending recovery was due")
	// first 已完成，但仍有待处理恢复：同样不得放行。
	firstCtx, firstCancel := context.WithTimeout(context.Background(), 150*time.Millisecond)
	defer firstCancel()
	if err := m.WaitCoreRecoveryTicket(firstCtx, first); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("WaitCoreRecoveryTicket(first) with a pending recovery = %v, want deadline exceeded", err)
	}

	// 待处理恢复运行（依赖在尝试结束后会把 pending 重新入队）。
	m.modemResetMu.Lock()
	m.modemResetPending = false
	m.modemResetMu.Unlock()
	m.enqueueModemResetEventOpts("pending_after_recovery", true)
	drainModemResetEvent(t, m)
	m.handleModemResetEvent()
	if err := awaitWait(t, done); err != nil {
		t.Fatalf("WaitCoreRecoveryTicket = %v, want nil after the pending recovery completed", err)
	}
}

// 已完成的票号在仍有恢复在途/待处理时不放行。
func TestWaitCoreRecoveryTicketWaitsWhileAnotherRecoveryIsPending(t *testing.T) {
	m := newTicketTestManager(t)
	ticket, _ := m.RequestCoreRecoveryTicket("first")
	drainModemResetEvent(t, m)
	m.handleModemResetEvent()
	if err := m.WaitCoreRecoveryTicket(context.Background(), ticket); err != nil {
		t.Fatalf("WaitCoreRecoveryTicket = %v, want nil", err)
	}

	m.modemResetMu.Lock()
	m.modemResetPending = true
	m.modemResetMu.Unlock()
	ctx, cancel := context.WithTimeout(context.Background(), 150*time.Millisecond)
	defer cancel()
	if err := m.WaitCoreRecoveryTicket(ctx, ticket); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("WaitCoreRecoveryTicket with a pending recovery = %v, want deadline exceeded", err)
	}
}

// 恢复耗尽（控制设备消失）时等待立即失败。
func TestWaitCoreRecoveryTicketFailsWhenRecoveryExhausted(t *testing.T) {
	m := newTicketTestManager(t)
	m.cfg.Device.ControlPath = "/dev/this-node-does-not-exist-xyz"
	m.openClientAndAllocateServicesHook = func(context.Context) error { return errors.New("no such device") }
	ticket, _ := m.RequestCoreRecoveryTicket("post_switch")
	drainModemResetEvent(t, m)
	m.handleModemResetEvent()

	err := m.WaitCoreRecoveryTicket(context.Background(), ticket)
	if !errors.Is(err, ErrCoreRecoveryExhausted) {
		t.Fatalf("WaitCoreRecoveryTicket = %v, want ErrCoreRecoveryExhausted", err)
	}
}

// 重试耗尽同样置耗尽标志；新的尝试启动后清除。
func TestScheduleRecoverRetryExhaustedMarksTickets(t *testing.T) {
	m := &Manager{
		log:    NewNopLogger(),
		events: NewEventEmitter(),
		cfg:    Config{RecoveryPolicy: RecoveryPolicy{MaxRecoverAttempts: 1}},
	}
	m.afterFunc = func(_ time.Duration, fn func()) *time.Timer { return time.NewTimer(time.Hour) }
	m.scheduleRecoverRetry("test")
	m.scheduleRecoverRetry("test") // recoverCount=2 > 1 → 耗尽
	if _, _, _, exhausted := m.CoreRecoveryTicketStatus(); !exhausted {
		t.Fatal("exhausted flag not set after recovery gave up")
	}
	m.coreRecoveryTickets.beginAttempt()
	if _, _, _, exhausted := m.CoreRecoveryTicketStatus(); exhausted {
		t.Fatal("exhausted flag should clear when a new attempt starts")
	}
}

// Manager 停止时等待失败。
func TestWaitCoreRecoveryTicketFailsWhenStopping(t *testing.T) {
	m := newTicketTestManager(t)
	m.mu.Lock()
	m.state = StateStopping
	m.mu.Unlock()
	if err := m.WaitCoreRecoveryTicket(context.Background(), 5); !errors.Is(err, ErrCoreRecoveryStopped) {
		t.Fatalf("WaitCoreRecoveryTicket = %v, want ErrCoreRecoveryStopped", err)
	}
	if ticket, scheduled := m.RequestCoreRecoveryTicket("post_switch"); ticket != 0 || scheduled {
		t.Fatalf("RequestCoreRecoveryTicket while stopping = (%d, %v), want (0, false)", ticket, scheduled)
	}
}

// 显式请求不受去抖窗口影响；普通指示仍被去抖；已排队时两者都合并。
func TestEnqueueModemResetEventGuaranteedBypassesDebounce(t *testing.T) {
	m := newTicketTestManager(t)
	m.modemResetMu.Lock()
	m.modemResetEnqueuedAt = time.Now()
	m.modemResetMu.Unlock()

	m.enqueueModemResetEventOpts("indication", false)
	select {
	case evt := <-m.eventCh:
		t.Fatalf("debounced indication was queued as %v", evt)
	default:
	}
	before := m.Stats().ResetCoalesced

	m.enqueueModemResetEventOpts("explicit", true)
	drainModemResetEvent(t, m)
	if got := m.Stats().ResetCoalesced; got != before {
		t.Fatalf("guaranteed enqueue was coalesced (coalesced %d -> %d)", before, got)
	}
	if !m.IsCoreRecovering() {
		t.Fatal("queued reset event should count as recovering")
	}
}

func TestEnqueueModemResetEventCoalescesWhileQueued(t *testing.T) {
	m := newTicketTestManager(t)
	m.enqueueModemResetEventOpts("first", true)
	before := m.Stats().ResetCoalesced
	m.enqueueModemResetEventOpts("second", true)
	if got := m.Stats().ResetCoalesced; got != before+1 {
		t.Fatalf("second enqueue while queued: coalesced %d -> %d, want +1", before, got)
	}
	drainModemResetEvent(t, m)
	select {
	case evt := <-m.eventCh:
		t.Fatalf("a second event %v was queued", evt)
	default:
	}
	m.handleModemResetEvent()
	if m.IsCoreRecovering() {
		t.Fatal("queued flag should clear once the event is picked up")
	}
}
