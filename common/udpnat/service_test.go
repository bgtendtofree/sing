package udpnat

import (
	"context"
	"net/netip"
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

type testExHandler struct {
}

func (h testExHandler) NewPacketConnectionEx(_ context.Context, conn N.PacketConn, _, _ M.Socksaddr, _ N.CloseHandlerFunc) {
	for {
		packet := buf.NewSize(1)
		_, err := conn.ReadPacket(packet)
		packet.Release()
		if err != nil {
			return
		}
	}
}

type testWriter struct{}

func (testWriter) WritePacket(buffer *buf.Buffer, _ M.Socksaddr) error {
	buffer.Release()
	return nil
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

func TestClosedSessionDoesNotDeleteReusedKey(t *testing.T) {
	returned := make(chan struct{}, 1)
	service := NewWithCapacity[netip.AddrPort](60, 1, true, testHandler{returned: returned})
	defer service.nat.Clear()

	destination := M.ParseSocksaddr("127.0.0.1:9")
	first := netip.MustParseAddrPort("127.0.0.1:1")
	second := netip.MustParseAddrPort("127.0.0.1:2")
	newPacket(service, first, destination)
	newPacket(service, second, destination)

	select {
	case <-returned:
	case <-time.After(time.Second):
		t.Fatal("evicted session handler did not return")
	}
	if !service.nat.Exist(second) {
		t.Fatal("closing the evicted session deleted the reused key")
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

func TestEvictedConnCannotUpdateReusedLifetime(t *testing.T) {
	service := NewWithCapacity[netip.AddrPort](60, 1, true, testHandler{})
	defer service.nat.Clear()

	first := netip.MustParseAddrPort("127.0.0.1:1")
	second := netip.MustParseAddrPort("127.0.0.1:2")
	newPacket(service, first, M.ParseSocksaddr("127.0.0.1:9"))
	old, loaded := service.nat.Load(first)
	if !loaded {
		t.Fatal("first session not cached")
	}
	newPacket(service, second, M.ParseSocksaddr("127.0.0.1:9"))
	if old.SetTimeout(time.Minute) {
		t.Fatal("evicted session refreshed a lifetime")
	}
}

func TestNewExWithCapacityDropsWhenQueueFull(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	service := NewExWithCapacity[netip.AddrPort](60, 10, true, testExHandler{})
	defer service.nat.Clear()

	key := netip.MustParseAddrPort("127.0.0.1:1")
	destination := M.ParseSocksaddr("127.0.0.1:9")
	done := make(chan struct{})
	go func() {
		defer close(done)
		// The handler above never reads. The nonblocking queue must drop
		// packets after its 64 slots instead of blocking this loop.
		for range 65 {
			buffer := buf.NewSize(1)
			_, _ = buffer.Write([]byte("x"))
			service.NewPacketEx(ctx, key, buffer, M.SocksaddrFromNetIP(key), destination, func(N.PacketConn) N.PacketWriter {
				return testWriter{}
			})
		}
	}()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("full session queue blocked the shared reader")
	}
}
