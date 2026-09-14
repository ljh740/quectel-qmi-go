package manager

import (
	"context"
	"errors"
	"fmt"

	"github.com/ljh740/quectel-qmi-go/pkg/qmi"
)

func (m *Manager) withNASRecovery(op string, fn func(nas *qmi.NASService) error) error {
	return m.withNASRecoveryContext(context.Background(), op, fn)
}

// withNASRecoveryContext 与 withNASRecovery 相同，但服务惰性分配、重绑及其锁等待都受 ctx 期限约束：
// 调用方给出的期限覆盖整个操作，而不只覆盖最终的 QMI 请求（与 withUIMRecoveryContext 同一语义）。
func (m *Manager) withNASRecoveryContext(ctx context.Context, op string, fn func(nas *qmi.NASService) error) error {
	_, err := withNASRecoveryValueContext(m, ctx, op, func(nas *qmi.NASService) (struct{}, error) {
		return struct{}{}, fn(nas)
	})
	return err
}

func withNASRecoveryValue[T any](m *Manager, op string, fn func(nas *qmi.NASService) (T, error)) (T, error) {
	return withNASRecoveryValueContext(m, context.Background(), op, fn)
}

func withNASRecoveryValueContext[T any](m *Manager, ctx context.Context, op string, fn func(nas *qmi.NASService) (T, error)) (T, error) {
	var zero T
	if ctx == nil {
		ctx = context.Background()
	}

	nas, err := m.ensureNASServiceContext(ctx)
	if err != nil {
		// 调用方主动取消不是设备故障：不上报、不触发整机恢复；真实超时/服务错误照常上报。
		if !callerCancelled(ctx) && m.shouldRecoverNASError(op, err) {
			m.triggerCoreRecoveryFromService("NAS", op, "initial", err)
		}
		return zero, err
	}

	result, err := fn(nas)
	if err == nil {
		m.noteServiceOperationSuccess("NAS", op)
		return result, nil
	}
	if !m.shouldRecoverNASError(op, err) {
		return result, err
	}
	// 期限已到就不再开始重绑：重绑与重试都超出了调用方允许的时间。真实到期且已达恢复阈值时仍调度一次
	// 后台 core recovery，避免 NAS 持续无响应却永远得不到恢复；主动取消不触发。
	if ctxErr := ctx.Err(); ctxErr != nil {
		if errors.Is(ctxErr, context.DeadlineExceeded) {
			m.logServiceRecovery("NAS", op, "deadline", err, "NAS operation failed and the caller deadline expired; scheduling core recovery instead of rebinding")
			m.triggerCoreRecoveryFromService("NAS", op, "deadline", err)
		} else {
			m.logServiceRecovery("NAS", op, "initial", err, "NAS operation failed; skipping rebind because the caller cancelled")
		}
		return result, err
	}

	m.logServiceRecovery("NAS", op, "initial", err, "NAS operation failed; rebinding NAS service")

	if lockErr := lockContext(ctx, &m.nasRecoveryMu); lockErr != nil {
		m.logServiceRecovery("NAS", op, "rebind", lockErr, "NAS rebind skipped: deadline expired while waiting for the recovery lock")
		return zero, fmt.Errorf("%s: NAS rebind skipped: %w (initial=%v)", op, lockErr, err)
	}
	nas, rebindErr := m.rebindNASServiceContext(ctx, "recover:"+op)
	m.nasRecoveryMu.Unlock()
	if rebindErr != nil {
		m.logServiceRecovery("NAS", op, "rebind", rebindErr, "NAS service rebind failed")
		if !callerCancelled(ctx) {
			m.triggerCoreRecoveryFromService("NAS", op, "rebind", rebindErr)
		}
		return zero, fmt.Errorf("%s: NAS rebind failed: %w (initial=%v)", op, rebindErr, err)
	}

	retryResult, retryErr := fn(nas)
	if retryErr == nil {
		m.noteServiceOperationSuccess("NAS", op)
		m.log.WithField("service_name", "NAS").WithField("op", op).WithField("phase", "retry").Info("NAS operation recovered after rebind")
		return retryResult, nil
	}
	if !callerCancelled(ctx) && m.shouldRecoverNASError(op, retryErr) {
		m.logServiceRecovery("NAS", op, "retry", retryErr, "NAS operation still failing after rebind")
		m.triggerCoreRecoveryFromService("NAS", op, "retry", retryErr)
	}
	return retryResult, retryErr
}

func (m *Manager) ensureNASService() (*qmi.NASService, error) {
	return m.ensureNASServiceContext(context.Background())
}

// ensureNASServiceContext 返回已分配的 NAS 服务，必要时在 ctx 期限内惰性分配（含锁等待与 CTL 分配请求）。
func (m *Manager) ensureNASServiceContext(ctx context.Context) (*qmi.NASService, error) {
	if m == nil {
		return nil, ErrServiceNotReady("NAS")
	}
	if m.ensureNASServiceHook != nil {
		return m.ensureNASServiceHook()
	}
	if ctx == nil {
		ctx = context.Background()
	}

	m.mu.RLock()
	nas := m.nas
	client := m.client
	m.mu.RUnlock()
	if nas != nil {
		return nas, nil
	}
	if client == nil {
		return nil, ErrServiceNotReady("NAS")
	}

	if err := lockContext(ctx, &m.nasRecoveryMu); err != nil {
		return nil, fmt.Errorf("allocate NAS client: %w", err)
	}
	defer m.nasRecoveryMu.Unlock()

	m.mu.RLock()
	nas = m.nas
	client = m.client
	m.mu.RUnlock()
	if nas != nil {
		return nas, nil
	}
	if client == nil {
		return nil, ErrServiceNotReady("NAS")
	}

	allocated, err := qmi.NewNASServiceWithContext(ctx, client)
	if err != nil {
		return nil, fmt.Errorf("allocate NAS client failed: %w", err)
	}

	m.mu.Lock()
	if m.client != client {
		m.mu.Unlock()
		m.discardNASServiceContext(ctx, allocated, "lazy_allocate_client_replaced")
		return nil, ErrServiceNotReady("NAS")
	}
	m.nas = allocated
	m.mu.Unlock()
	m.log.Info("NAS service lazily allocated")
	return allocated, nil
}

// discardNASServiceContext 在 m.mu 外、用调用方剩余预算释放分配期间已作废的 NAS 客户端；释放失败只记录。
func (m *Manager) discardNASServiceContext(ctx context.Context, svc *qmi.NASService, reason string) {
	if svc == nil {
		return
	}
	if err := svc.CloseWithContext(ctx); err != nil {
		m.log.WithError(err).WithField("reason", reason).Warn("Releasing a superseded NAS client failed")
	}
}

func (m *Manager) rebindNASService(reason string) (*qmi.NASService, error) {
	return m.rebindNASServiceContext(context.Background(), reason)
}

// rebindNASServiceContext 在 ctx 期限内释放旧 NAS 客户端并重新分配；调用方须持有 nasRecoveryMu。
func (m *Manager) rebindNASServiceContext(ctx context.Context, reason string) (*qmi.NASService, error) {
	if m == nil {
		return nil, ErrServiceNotReady("NAS")
	}
	if m.rebindNASServiceHook != nil {
		return m.rebindNASServiceHook(reason)
	}

	m.mu.Lock()
	prev := m.nas
	client := m.client
	m.nas = nil
	m.mu.Unlock()

	if prev != nil {
		if err := prev.CloseWithContext(ctx); err != nil {
			m.log.WithError(err).WithField("reason", reason).Warn("Closing previous NAS client failed during rebind")
		}
	}
	if client == nil {
		return nil, ErrServiceNotReady("NAS")
	}
	if err := ctx.Err(); err != nil {
		return nil, fmt.Errorf("allocate NAS client: %w", err)
	}

	allocated, err := qmi.NewNASServiceWithContext(ctx, client)
	if err != nil {
		return nil, fmt.Errorf("allocate NAS client failed: %w", err)
	}

	m.mu.Lock()
	if m.client != client {
		m.mu.Unlock()
		m.discardNASServiceContext(ctx, allocated, "rebind_client_replaced")
		return nil, ErrServiceNotReady("NAS")
	}
	m.nas = allocated
	m.mu.Unlock()
	m.log.WithField("reason", reason).Info("NAS service rebound")
	return allocated, nil
}
