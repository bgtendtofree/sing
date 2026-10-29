package udpnat

import (
	"context"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sagernet/sing/common/buf"
	M "github.com/sagernet/sing/common/metadata"
	N "github.com/sagernet/sing/common/network"
)

type contextHandlerEx func(context.Context, N.PacketConn, N.CloseHandlerFunc)

func (h contextHandlerEx) NewPacketConnectionEx(ctx context.Context, c N.PacketConn, _, _ M.Socksaddr, onClose N.CloseHandlerFunc) {
	h(ctx, c, onClose)
}

type contextHandler func(context.Context, N.PacketConn) error

func (h contextHandler) NewPacketConnection(ctx context.Context, c N.PacketConn, _ M.Metadata) error {
	return h(ctx, c)
}

func (contextHandler) NewError(context.Context, error) {}

// Hide the underlying cancel context's private values so context uses our
// AfterFunc implementation, allowing registrations and their cleanup to be
// checked without relying on goroutine counts or context internals.
type trackedContext struct {
	context.Context
	callbacks atomic.Int32
}

type handlerValueKey struct{}

func (c *trackedContext) Value(key any) any {
	if _, ok := key.(handlerValueKey); ok {
		return c.Context.Value(key)
	}
	return nil
}

func (c *trackedContext) AfterFunc(f func()) func() bool {
	c.callbacks.Add(1)
	var once sync.Once
	release := func() { once.Do(func() { c.callbacks.Add(-1) }) }
	stop := context.AfterFunc(c.Context, func() {
		release()
		f()
	})
	return func() bool {
		stopped := stop()
		if stopped {
			release()
		}
		return stopped
	}
}

type contextSession struct {
	ctx     context.Context
	conn    *conn
	onClose N.CloseHandlerFunc
	exited  chan struct{}
}

func waitContextTest(t *testing.T, done <-chan struct{}) {
	t.Helper()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for handler cancellation or cleanup")
	}
}

func waitContextSession(t *testing.T, ready <-chan *contextSession) *contextSession {
	t.Helper()
	select {
	case session := <-ready:
		return session
	case <-time.After(2 * time.Second):
		t.Fatal("handler did not start")
		return nil
	}
}

func sendContextPacket(service *Service[int], ctx context.Context, key int, initCtx context.Context) {
	service.NewContextPacketEx(ctx, key, buf.NewSize(1), M.ParseSocksaddr("127.0.0.1:1"), M.ParseSocksaddr("127.0.0.1:9"), func(N.PacketConn) (context.Context, N.PacketWriter) {
		return initCtx, testWriter{}
	})
}

func drainContextSession(session *contextSession) {
	(&gatedSession{conn: session.conn}).drain()
}

func waitContextCallbacks(t *testing.T, tracked *trackedContext, want int32) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for tracked.callbacks.Load() != want {
		if time.Now().After(deadline) {
			t.Fatalf("init context registrations = %d, want %d", tracked.callbacks.Load(), want)
		}
		time.Sleep(time.Millisecond)
	}
}

func TestHandlerContextCanceledOnEviction(t *testing.T) {
	parent, cancel := context.WithCancel(context.Background())
	tracked := &trackedContext{Context: context.WithValue(parent, handlerValueKey{}, "initialized")}
	originalCtx := context.WithValue(context.Background(), handlerValueKey{}, "original")
	ready := make(chan *contextSession, 2)
	handler := contextHandlerEx(func(ctx context.Context, c N.PacketConn, onClose N.CloseHandlerFunc) {
		session := &contextSession{ctx: ctx, conn: c.(*conn), exited: make(chan struct{})}
		ready <- session
		// Model DNS work waiting only on the handler context, not ReadPacket.
		<-ctx.Done()
		onClose(nil)
		close(session.exited)
	})
	service := NewExWithCapacity[int](60, 1, true, handler)
	var sessions []*contextSession
	t.Cleanup(func() {
		service.nat.Clear()
		cancel() // Also releases handlers if cancellation propagation regresses.
		for _, session := range sessions {
			waitContextTest(t, session.exited)
			drainContextSession(session)
		}
	})

	sendContextPacket(service, originalCtx, 1, tracked)
	first := waitContextSession(t, ready)
	sessions = append(sessions, first)
	if got := first.ctx.Value(handlerValueKey{}); got != "initialized" {
		t.Fatalf("handler context value = %v, want init's replacement context value", got)
	}
	if got := tracked.callbacks.Load(); got != 1 {
		t.Fatalf("active init context registrations = %d, want 1", got)
	}
	sendContextPacket(service, originalCtx, 2, tracked)
	second := waitContextSession(t, ready)
	sessions = append(sessions, second)
	waitContextTest(t, first.exited)
	if cause := context.Cause(first.ctx); cause != net.ErrClosed {
		t.Fatalf("evicted handler cancellation cause = %v", cause)
	}
	// Cancellation closes Done before unlinking the parent's registration.
	// The AfterFunc goroutine may still be completing that unlink.
	waitContextCallbacks(t, tracked, 1)
	select {
	case <-second.ctx.Done():
		t.Fatal("eviction canceled the replacement handler")
	default:
	}
	service.nat.Clear()
	waitContextTest(t, second.exited)
	waitContextCallbacks(t, tracked, 0)
}

func TestHandlerContextNormalCleanup(t *testing.T) {
	for _, legacy := range []bool{false, true} {
		name := "onClose"
		if legacy {
			name = "legacyReturn"
		}
		t.Run(name, func(t *testing.T) {
			parent, cancel := context.WithCancel(context.Background())
			tracked := &trackedContext{Context: parent}
			ready := make(chan *contextSession, 1)
			release := make(chan struct{})
			var releaseOnce sync.Once
			unblock := func() { releaseOnce.Do(func() { close(release) }) }
			var service *Service[int]
			if legacy {
				service = NewWithCapacity[int](60, 1, true, contextHandler(func(ctx context.Context, c N.PacketConn) error {
					ready <- &contextSession{ctx: ctx, conn: c.(*conn)}
					<-release
					return nil
				}))
			} else {
				service = NewExWithCapacity[int](60, 1, true, contextHandlerEx(func(ctx context.Context, c N.PacketConn, onClose N.CloseHandlerFunc) {
					ready <- &contextSession{ctx: ctx, conn: c.(*conn), onClose: onClose}
					// The Ex handler may return before its asynchronous onClose.
				}))
			}
			var session *contextSession
			t.Cleanup(func() {
				unblock()
				service.nat.Clear()
				cancel()
				if session != nil {
					drainContextSession(session)
				}
			})
			sendContextPacket(service, context.Background(), 1, tracked)
			session = waitContextSession(t, ready)
			select {
			case <-session.ctx.Done():
				t.Fatal("handler context canceled before normal close")
			default:
			}
			if got := tracked.callbacks.Load(); got != 1 {
				t.Fatalf("active init context registrations = %d, want 1", got)
			}
			if legacy {
				unblock()
			} else {
				session.onClose(nil)
			}
			waitContextTest(t, session.ctx.Done())
			waitContextTest(t, session.conn.ctx.Done())
			deadline := time.Now().Add(2 * time.Second)
			for service.nat.Exist(1) || tracked.callbacks.Load() != 0 {
				if time.Now().After(deadline) {
					t.Fatal("normal close leaked cache entry or init context callback")
				}
				time.Sleep(time.Millisecond)
			}
		})
	}
}

func TestHandlerContextPreservesParentCancellation(t *testing.T) {
	for _, cancelInit := range []bool{false, true} {
		name := "originalParent"
		if cancelInit {
			name = "initParent"
		}
		t.Run(name, func(t *testing.T) {
			original, cancelOriginal := context.WithCancel(context.Background())
			initialized, cancelInitialized := context.WithCancel(context.Background())
			ready := make(chan *contextSession, 1)
			service := NewExWithCapacity[int](60, 1, true, contextHandlerEx(func(ctx context.Context, c N.PacketConn, onClose N.CloseHandlerFunc) {
				ready <- &contextSession{ctx: ctx, conn: c.(*conn), onClose: onClose}
			}))
			defer cancelOriginal()
			defer cancelInitialized()
			defer service.nat.Clear()
			sendContextPacket(service, original, 1, initialized)
			session := waitContextSession(t, ready)
			defer drainContextSession(session)
			if cancelInit {
				cancelInitialized()
			} else {
				cancelOriginal()
			}
			waitContextTest(t, session.ctx.Done())
			session.onClose(nil)
		})
	}
}
