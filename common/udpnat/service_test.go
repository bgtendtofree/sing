package udpnat

import (
	"context"
	"net/netip"
	"sync"
	"testing"
	"time"

	"github.com/sagernet/sing/common/buf"
	M "github.com/sagernet/sing/common/metadata"
	N "github.com/sagernet/sing/common/network"
)

type testHandler struct {
	returned chan struct{}
}

func (h testHandler) NewPacketConnection(_ context.Context, conn N.PacketConn, _ M.Metadata) error {
	for {
		packet := buf.NewSize(1)
		_, err := conn.ReadPacket(packet)
		packet.Release()
		if err != nil {
			if h.returned != nil {
				h.returned <- struct{}{}
			}
			return err
		}
	}
}

func (h testHandler) NewError(context.Context, error) {}

type testWriter struct{}

func (testWriter) WritePacket(buffer *buf.Buffer, _ M.Socksaddr) error {
	buffer.Release()
	return nil
}

type gatedSession struct {
	conn        *conn
	release     chan struct{}
	releaseOnce sync.Once
	exited      chan struct{}
}

func (s *gatedSession) unblock() {
	s.releaseOnce.Do(func() { close(s.release) })
}

func (s *gatedSession) drain() {
	for {
		select {
		case packet := <-s.conn.data:
			packet.data.Release()
		default:
			return
		}
	}
}

// gatedHandler deliberately never reads packets. It reports the session first,
// then waits for eviction/close and the test's release gate before running the
// service's cleanup callback.
type gatedHandler struct {
	ready chan *gatedSession
}

func (h gatedHandler) NewPacketConnectionEx(_ context.Context, packetConn N.PacketConn, _, _ M.Socksaddr, onClose N.CloseHandlerFunc) {
	session := &gatedSession{
		conn:    packetConn.(*conn),
		release: make(chan struct{}),
		exited:  make(chan struct{}),
	}
	h.ready <- session
	<-session.conn.ctx.Done()
	<-session.release
	onClose(nil)
	close(session.exited)
}

func newGatedService(capacity int) (*Service[netip.AddrPort], gatedHandler) {
	handler := gatedHandler{ready: make(chan *gatedSession, capacity+2)}
	return NewExWithCapacity[netip.AddrPort](60, capacity, true, handler), handler
}

func (h gatedHandler) wait(t *testing.T) *gatedSession {
	t.Helper()
	select {
	case session := <-h.ready:
		return session
	case <-time.After(2 * time.Second):
		t.Fatal("handler did not start")
		return nil
	}
}

func gatedSend(service *Service[netip.AddrPort], key netip.AddrPort) {
	buffer := buf.NewSize(1)
	_, _ = buffer.Write([]byte("x"))
	service.NewPacketEx(context.Background(), key, buffer, M.SocksaddrFromNetIP(key), M.ParseSocksaddr("127.0.0.1:9"), func(N.PacketConn) N.PacketWriter {
		return testWriter{}
	})
}

func cleanupGated(t *testing.T, service *Service[netip.AddrPort], sessions []*gatedSession) {
	t.Helper()
	for _, session := range sessions {
		_ = session.conn.Close()
		session.unblock()
	}
	service.nat.Clear()
	for _, session := range sessions {
		select {
		case <-session.exited:
		case <-time.After(2 * time.Second):
			t.Errorf("handler did not exit")
		}
		session.drain()
	}
}

func newPacket(service *Service[netip.AddrPort], key netip.AddrPort, destination M.Socksaddr) {
	buffer := buf.NewSize(1)
	_, _ = buffer.Write([]byte("x"))
	service.NewPacket(context.Background(), key, buffer, M.Metadata{
		Source:      M.SocksaddrFromNetIP(key),
		Destination: destination,
	}, func(N.PacketConn) N.PacketWriter {
		return testWriter{}
	})
}

func TestNewWithCapacityEvictsOldest(t *testing.T) {
	handler := testHandler{}
	service := NewWithCapacity[netip.AddrPort](60, 2, true, handler)
	defer service.nat.Clear()

	destination := M.ParseSocksaddr("127.0.0.1:9")
	keys := []netip.AddrPort{
		netip.MustParseAddrPort("127.0.0.1:1"),
		netip.MustParseAddrPort("127.0.0.1:2"),
		netip.MustParseAddrPort("127.0.0.1:3"),
	}
	for _, key := range keys {
		newPacket(service, key, destination)
	}
	count := 0
	service.nat.Range(func(netip.AddrPort, *conn) {
		count++
	})
	if count != 2 {
		t.Fatalf("cache holds %d sessions, want 2", count)
	}
	if service.nat.Exist(keys[0]) {
		t.Fatal("oldest session was not evicted at capacity")
	}
}

func reuseSessions(t *testing.T) (*Service[netip.AddrPort], []*gatedSession, *gatedSession, *gatedSession) {
	t.Helper()
	service, handler := newGatedService(1)
	sessions := make([]*gatedSession, 0, 3)
	t.Cleanup(func() { cleanupGated(t, service, sessions) })

	first := netip.MustParseAddrPort("127.0.0.1:1")
	second := netip.MustParseAddrPort("127.0.0.1:2")
	gatedSend(service, first)
	oldFirst := handler.wait(t)
	sessions = append(sessions, oldFirst)
	gatedSend(service, second)
	sessions = append(sessions, handler.wait(t))
	gatedSend(service, first)
	newFirst := handler.wait(t)
	sessions = append(sessions, newFirst)
	return service, sessions, oldFirst, newFirst
}

func TestOldSessionCleanupDoesNotDeleteReusedKey(t *testing.T) {
	service, _, oldFirst, newFirst := reuseSessions(t)
	first := netip.MustParseAddrPort("127.0.0.1:1")

	// The old generation is already evicted, but its cleanup has been held
	// back until the replacement for the same source key exists.
	oldFirst.unblock()
	select {
	case <-oldFirst.exited:
	case <-time.After(2 * time.Second):
		t.Fatal("old session cleanup did not run")
	}

	current, _, ok := service.nat.PeekWithLifetime(first)
	if !ok {
		t.Fatal("replacement session disappeared")
	}
	if current != newFirst.conn {
		t.Fatal("old session cleanup deleted the replacement source key")
	}
}

func TestOldSessionCannotUpdateReusedLifetime(t *testing.T) {
	service, _, oldFirst, newFirst := reuseSessions(t)

	if oldFirst.conn.SetTimeout(time.Hour) {
		t.Fatal("evicted generation refreshed the replacement lifetime")
	}
	current, _, ok := service.nat.PeekWithLifetime(netip.MustParseAddrPort("127.0.0.1:1"))
	if !ok || current != newFirst.conn {
		t.Fatal("replacement session was not current")
	}
	if !newFirst.conn.SetTimeout(time.Hour) {
		t.Fatal("current generation could not refresh its own lifetime")
	}
}

func TestConnTimeoutUpdatesLifetime(t *testing.T) {
	service := NewWithCapacity[netip.AddrPort](60, 10, true, testHandler{})
	defer service.nat.Clear()

	key := netip.MustParseAddrPort("127.0.0.1:1")
	newPacket(service, key, M.ParseSocksaddr("127.0.0.1:9"))
	c, loaded := service.nat.Load(key)
	if !loaded {
		t.Fatal("session not cached")
	}
	if remaining := c.Timeout(); remaining <= 0 || remaining > time.Minute {
		t.Fatalf("initial timeout = %v", remaining)
	}
	if !c.SetTimeout(time.Minute) {
		t.Fatal("SetTimeout rejected the current session")
	}
	if remaining := c.Timeout(); remaining <= 55*time.Second || remaining > time.Minute {
		t.Fatalf("updated timeout = %v", remaining)
	}
}

func TestNewExWithCapacityDropsWhenQueueFull(t *testing.T) {
	service, handler := newGatedService(1)
	sessions := make([]*gatedSession, 0, 1)
	t.Cleanup(func() { cleanupGated(t, service, sessions) })

	key := netip.MustParseAddrPort("127.0.0.1:1")
	gatedSend(service, key)
	session := handler.wait(t)
	sessions = append(sessions, session)

	// The gated handler never reads. The first datagram is queued before the
	// handler starts; 63 more fill the remaining slots.
	for range 63 {
		gatedSend(service, key)
	}
	if got := len(session.conn.data); got != cap(session.conn.data) {
		t.Fatalf("queue depth=%d, want full capacity %d", got, cap(session.conn.data))
	}

	done := make(chan struct{})
	go func() {
		defer close(done)
		gatedSend(service, key)
	}()
	blocked := false
	select {
	case <-done:
	case <-time.After(time.Second):
		blocked = true
		// Unblock a mutated blocking enqueue so cleanup and the mutation
		// diagnostic can finish instead of hanging the test process.
		select {
		case packet := <-session.conn.data:
			packet.data.Release()
		case <-time.After(time.Second):
			t.Fatal("full queue could not be drained")
		}
		select {
		case <-done:
		case <-time.After(2 * time.Second):
			t.Fatal("blocked enqueue did not finish after draining one slot")
		}
	}
	if blocked {
		t.Error("65th datagram blocked instead of being dropped when the queue was full")
	}
	if got := len(session.conn.data); got != cap(session.conn.data) {
		t.Fatalf("queue depth=%d after the 65th datagram, want %d", got, cap(session.conn.data))
	}
}
