package qmi

import (
	"context"
	"errors"
	"testing"
	"time"
)

// failFirstWrite 让首次写请求失败，返回后续是否收到新请求的观察通道。
func failFirstWrite(t *testing.T, c *Client) <-chan struct{} {
	t.Helper()
	more := make(chan struct{}, 1)
	go func() {
		wr := <-c.writeCh
		wr.result <- errors.New("write failed")
		select {
		case <-c.writeCh:
			more <- struct{}{}
		case <-time.After(time.Second):
		}
	}()
	return more
}

// 首次写失败后在 500ms 重试等待中被取消：返回的错误可判定为 Canceled，并且不再发出第二个请求。
func TestAllocateClientIDWithContextReportsCancelDuringRetryWait(t *testing.T) {
	c := newUIMUnitTestClient()
	more := failFirstWrite(t, c)
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(50 * time.Millisecond)
		cancel()
	}()
	start := time.Now()
	_, err := c.AllocateClientIDWithContext(ctx, ServiceUIM)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("AllocateClientIDWithContext() error = %v, want Canceled", err)
	}
	if elapsed := time.Since(start); elapsed >= 500*time.Millisecond {
		t.Fatalf("AllocateClientIDWithContext() took %v, want to abort the retry wait promptly", elapsed)
	}
	select {
	case <-more:
		t.Fatal("a second allocation request was sent after cancellation")
	case <-time.After(150 * time.Millisecond):
	}
}

// 首次写失败后在重试等待中到期：返回的错误可判定为 DeadlineExceeded，并且不再发出第二个请求。
func TestAllocateClientIDWithContextReportsDeadlineDuringRetryWait(t *testing.T) {
	c := newUIMUnitTestClient()
	more := failFirstWrite(t, c)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Millisecond)
	defer cancel()
	_, err := c.AllocateClientIDWithContext(ctx, ServiceUIM)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("AllocateClientIDWithContext() error = %v, want DeadlineExceeded", err)
	}
	select {
	case <-more:
		t.Fatal("a second allocation request was sent after the deadline")
	case <-time.After(150 * time.Millisecond):
	}
}
