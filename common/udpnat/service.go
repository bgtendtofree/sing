package udpnat

import (
	"context"
	"io"
	"net"
	"os"
	"time"

	"github.com/sagernet/sing/common"
	"github.com/sagernet/sing/common/buf"
	"github.com/sagernet/sing/common/cache"
	E "github.com/sagernet/sing/common/exceptions"
	M "github.com/sagernet/sing/common/metadata"
	N "github.com/sagernet/sing/common/network"
	"github.com/sagernet/sing/common/pipe"
)

// Deprecated: Use N.UDPConnectionHandler instead.
//
//nolint:staticcheck
type Handler interface {
	N.UDPConnectionHandler
	E.Handler
}

type Service[K comparable] struct {
	nat        *cache.LruCache[K, *conn]
	handler    Handler
	handlerEx  N.UDPConnectionHandlerEx
	dropOnFull bool
}

// Deprecated: Use NewEx instead.
func New[K comparable](maxAge int64, handler Handler) *Service[K] {
	return NewWithCapacity[K](maxAge, 0, false, handler)
}

// NewWithCapacity creates a legacy packet service with an optional session
// cap. When dropOnFull is true, a full per-session queue drops the datagram
// instead of blocking the shared reader.
//
// Deprecated: Use NewExWithCapacity instead.
func NewWithCapacity[K comparable](maxAge int64, maxEntries int, dropOnFull bool, handler Handler) *Service[K] {
	service := &Service[K]{
		nat:        newCache[K](maxAge, maxEntries),
		handler:    handler,
		dropOnFull: dropOnFull,
	}
	return service
}

func NewEx[K comparable](maxAge int64, handler N.UDPConnectionHandlerEx) *Service[K] {
	return NewExWithCapacity[K](maxAge, 0, false, handler)
}

func NewExWithCapacity[K comparable](maxAge int64, maxEntries int, dropOnFull bool, handler N.UDPConnectionHandlerEx) *Service[K] {
	service := &Service[K]{
		nat:        newCache[K](maxAge, maxEntries),
		handlerEx:  handler,
		dropOnFull: dropOnFull,
	}
	return service
}

func newCache[K comparable](maxAge int64, maxEntries int) *cache.LruCache[K, *conn] {
	options := []cache.Option[K, *conn]{
		cache.WithAge[K, *conn](maxAge),
		cache.WithUpdateAgeOnGet[K, *conn](),
		cache.WithEvict[K, *conn](func(key K, conn *conn) {
			conn.Close()
		}),
	}
	if maxEntries > 0 {
		options = append(options, cache.WithSize[K, *conn](maxEntries))
	}
	return cache.New(options...)
}

func (s *Service[T]) WriteIsThreadUnsafe() {
}

// Deprecated: don't use
func (s *Service[T]) NewPacketDirect(ctx context.Context, key T, conn N.PacketConn, buffer *buf.Buffer, metadata M.Metadata) {
	s.NewContextPacket(ctx, key, buffer, metadata, func(natConn N.PacketConn) (context.Context, N.PacketWriter) {
		return ctx, &DirectBackWriter{conn, natConn}
	})
}

type DirectBackWriter struct {
	Source N.PacketConn
	Nat    N.PacketConn
}

func (w *DirectBackWriter) WritePacket(buffer *buf.Buffer, addr M.Socksaddr) error {
	return w.Source.WritePacket(buffer, M.SocksaddrFromNet(w.Nat.LocalAddr()))
}

func (w *DirectBackWriter) Upstream() any {
	return w.Source
}

// Deprecated: use NewPacketEx instead.
func (s *Service[T]) NewPacket(ctx context.Context, key T, buffer *buf.Buffer, metadata M.Metadata, init func(natConn N.PacketConn) N.PacketWriter) {
	s.NewContextPacket(ctx, key, buffer, metadata, func(natConn N.PacketConn) (context.Context, N.PacketWriter) {
		return ctx, init(natConn)
	})
}

func (s *Service[T]) NewPacketEx(ctx context.Context, key T, buffer *buf.Buffer, source M.Socksaddr, destination M.Socksaddr, init func(natConn N.PacketConn) N.PacketWriter) {
	s.NewContextPacketEx(ctx, key, buffer, source, destination, func(natConn N.PacketConn) (context.Context, N.PacketWriter) {
		return ctx, init(natConn)
	})
}

// Deprecated: Use NewPacketConnectionEx instead.
func (s *Service[T]) NewContextPacket(ctx context.Context, key T, buffer *buf.Buffer, metadata M.Metadata, init func(natConn N.PacketConn) (context.Context, N.PacketWriter)) {
	s.NewContextPacketEx(ctx, key, buffer, metadata.Source, metadata.Destination, init)
}

func (s *Service[T]) NewContextPacketEx(ctx context.Context, key T, buffer *buf.Buffer, source M.Socksaddr, destination M.Socksaddr, init func(natConn N.PacketConn) (context.Context, N.PacketWriter)) {
	c, loaded := s.nat.LoadOrStore(key, func() *conn {
		c := &conn{
			data:         make(chan packet, 64),
			localAddr:    source,
			remoteAddr:   destination,
			readDeadline: pipe.MakeDeadline(),
		}
		c.ctx, c.cancel = context.WithCancelCause(ctx)
		c.timeout = func() time.Duration {
			current, lifetime, loaded := s.nat.PeekWithLifetime(key)
			if !loaded || current != c {
				return 0
			}
			return time.Until(lifetime)
		}
		c.setTimeout = func(timeout time.Duration) bool {
			return s.nat.UpdateLifetime(key, func(current *conn) bool {
				return current == c
			}, timeout)
		}
		return c
	})
	if !loaded {
		ctx, c.source = init(c)
		handlerCtx, cancelHandler := context.WithCancelCause(ctx)
		// init may replace the parent context. Retain its values and cancellation
		// while also canceling in-flight handler work when the session closes.
		stop := context.AfterFunc(c.ctx, func() {
			cancelHandler(context.Cause(c.ctx))
		})
		onClose := func(err error) {
			stop()
			cancelHandler(err)
			s.deleteIfCurrent(key, c)
		}
		go func() {
			if s.handlerEx != nil {
				s.handlerEx.NewPacketConnectionEx(handlerCtx, c, source, destination, onClose)
			} else {
				//nolint:staticcheck
				err := s.handler.NewPacketConnection(handlerCtx, c, M.Metadata{
					Source:      source,
					Destination: destination,
				})
				if err != nil {
					s.handler.NewError(handlerCtx, err)
				}
				c.Close()
				onClose(err)
			}
		}()
	}
	if common.Done(c.ctx) {
		s.deleteIfCurrent(key, c)
		if !common.Done(ctx) {
			s.NewContextPacketEx(ctx, key, buffer, source, destination, init)
		} else {
			buffer.Release()
		}
		return
	}
	nextPacket := packet{
		data:        buffer,
		destination: destination,
	}
	if s.dropOnFull {
		select {
		case c.data <- nextPacket:
		default:
			buffer.Release()
		}
	} else {
		c.data <- nextPacket
	}
}

func (s *Service[K]) deleteIfCurrent(key K, c *conn) {
	s.nat.DeleteIf(key, func(current *conn) bool {
		return current == c
	})
}

type packet struct {
	data        *buf.Buffer
	destination M.Socksaddr
}

var _ N.PacketConn = (*conn)(nil)

type conn struct {
	ctx             context.Context
	cancel          context.CancelCauseFunc
	data            chan packet
	localAddr       M.Socksaddr
	remoteAddr      M.Socksaddr
	source          N.PacketWriter
	readDeadline    pipe.Deadline
	readWaitOptions N.ReadWaitOptions
	timeout         func() time.Duration
	setTimeout      func(time.Duration) bool
}

func (c *conn) Timeout() time.Duration {
	if c.timeout == nil {
		return 0
	}
	return c.timeout()
}

func (c *conn) SetTimeout(timeout time.Duration) bool {
	if c.setTimeout == nil {
		return false
	}
	return c.setTimeout(timeout)
}

func (c *conn) ReadPacket(buffer *buf.Buffer) (addr M.Socksaddr, err error) {
	select {
	case p := <-c.data:
		_, err = buffer.ReadOnceFrom(p.data)
		p.data.Release()
		return p.destination, err
	case <-c.ctx.Done():
		return M.Socksaddr{}, io.ErrClosedPipe
	case <-c.readDeadline.Wait():
		return M.Socksaddr{}, os.ErrDeadlineExceeded
	}
}

func (c *conn) WritePacket(buffer *buf.Buffer, destination M.Socksaddr) error {
	return c.source.WritePacket(buffer, destination)
}

func (c *conn) Close() error {
	select {
	case <-c.ctx.Done():
	default:
		c.cancel(net.ErrClosed)
	}
	if sourceCloser, sourceIsCloser := c.source.(io.Closer); sourceIsCloser {
		return sourceCloser.Close()
	}
	return nil
}

func (c *conn) LocalAddr() net.Addr {
	return c.localAddr
}

func (c *conn) RemoteAddr() net.Addr {
	return c.remoteAddr
}

func (c *conn) SetDeadline(t time.Time) error {
	return os.ErrInvalid
}

func (c *conn) SetReadDeadline(t time.Time) error {
	c.readDeadline.Set(t)
	return nil
}

func (c *conn) SetWriteDeadline(t time.Time) error {
	return os.ErrInvalid
}

func (c *conn) Upstream() any {
	return c.source
}
