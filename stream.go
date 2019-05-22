package multiplex

import (
	"context"
	"errors"
	"io"
	"sync"
	"time"

	pool "github.com/libp2p/go-buffer-pool"
	streammux "github.com/libp2p/go-stream-muxer"
)

var errStreamClosed = errors.New("closed stream")

// streamID is a convenience type for operating on stream IDs
type streamID struct {
	id        uint64
	initiator bool
}

// header computes the header for the given tag
func (id *streamID) header(tag uint64) uint64 {
	header := id.id<<3 | tag
	if !id.initiator {
		header--
	}
	return header
}

type Stream struct {
	id     streamID
	name   string
	dataIn chan []byte
	mp     *Multiplex

	extra []byte

	// exbuf is for holding the reference to the beginning of the extra slice
	// for later memory pool freeing
	exbuf []byte

	deadlineLock                     sync.Mutex
	wDeadlineCtx, rDeadlineCtx       context.Context
	wDeadlineCancel, rDeadlineCancel func()

	clLock       sync.Mutex
	closedRemote bool

	// Closed when the connection is reset.
	reset chan struct{}

	// Closed when the writer is closed (reset will also be closed)
	closedLocal  context.Context
	doCloseLocal context.CancelFunc
}

func (s *Stream) Name() string {
	return s.name
}

// tries to preload pending data
func (s *Stream) preloadData() {
	select {
	case read, ok := <-s.dataIn:
		if !ok {
			return
		}
		s.extra = read
		s.exbuf = read
	default:
	}
}

func (s *Stream) waitForData() error {
	var ctx context.Context
	s.deadlineLock.Lock()
	if s.rDeadlineCtx != nil {
		ctx = s.rDeadlineCtx
	} else {
		ctx = context.Background()
	}
	s.deadlineLock.Unlock()

	select {
	case <-s.reset:
		// This is the only place where it's safe to return these.
		s.returnBuffers()
		return streammux.ErrReset
	case read, ok := <-s.dataIn:
		if !ok {
			return io.EOF
		}
		s.extra = read
		s.exbuf = read
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (s *Stream) returnBuffers() {
	if s.exbuf != nil {
		pool.Put(s.exbuf)
		s.exbuf = nil
		s.extra = nil
	}
	for {
		select {
		case read, ok := <-s.dataIn:
			if !ok {
				return
			}
			if read == nil {
				continue
			}
			pool.Put(read)
		default:
			return
		}
	}
}

func (s *Stream) Read(b []byte) (int, error) {
	select {
	case <-s.reset:
		return 0, streammux.ErrReset
	default:
	}
	if s.extra == nil {
		err := s.waitForData()
		if err != nil {
			return 0, err
		}
	}
	n := 0
	for s.extra != nil && n < len(b) {
		read := copy(b[n:], s.extra)
		n += read
		if read < len(s.extra) {
			s.extra = s.extra[read:]
		} else {
			if s.exbuf != nil {
				pool.Put(s.exbuf)
			}
			s.extra = nil
			s.exbuf = nil
			s.preloadData()
		}
	}
	return n, nil
}

func (s *Stream) Write(b []byte) (int, error) {
	var written int
	for written < len(b) {
		wl := len(b) - written
		if wl > MaxMessageSize {
			wl = MaxMessageSize
		}

		n, err := s.write(b[written : written+wl])
		if err != nil {
			return written, err
		}

		written += n
	}

	return written, nil
}

func (s *Stream) write(b []byte) (int, error) {
	if s.isClosed() {
		return 0, errors.New("cannot write to closed stream")
	}

	var ctx context.Context
	s.deadlineLock.Lock()
	if s.wDeadlineCtx != nil {
		ctx = s.wDeadlineCtx
	} else {
		ctx = s.closedLocal
	}
	s.deadlineLock.Unlock()

	err := s.mp.sendMsg(ctx, s.id.header(messageTag), b)

	if err != nil {
		if err == context.Canceled {
			err = errors.New("cannot write to closed stream")
		}
		return 0, err
	}

	return len(b), nil
}

func (s *Stream) isClosed() bool {
	return s.closedLocal.Err() != nil
}

func (s *Stream) Close() error {
	ctx, cancel := context.WithTimeout(context.Background(), ResetStreamTimeout)
	defer cancel()

	err := s.mp.sendMsg(ctx, s.id.header(closeTag), nil)

	if s.isClosed() {
		return nil
	}

	s.clLock.Lock()
	remote := s.closedRemote
	s.clLock.Unlock()

	s.doCloseLocal()

	if remote {
		s.cancelDeadlines()
		s.mp.chLock.Lock()
		delete(s.mp.channels, s.id)
		s.mp.chLock.Unlock()
	}

	if err != nil && !s.mp.isShutdown() {
		log.Warningf("Error closing stream: %s; killing connection", err.Error())
		s.mp.Close()
	}

	return err
}

func (s *Stream) Reset() error {
	s.clLock.Lock()

	// Don't reset when fully closed.
	if s.closedRemote && s.isClosed() {
		s.clLock.Unlock()
		return nil
	}

	// Don't reset twice.
	select {
	case <-s.reset:
		s.clLock.Unlock()
		return nil
	default:
	}

	close(s.reset)
	s.doCloseLocal()
	s.closedRemote = true
	s.cancelDeadlines()

	go s.mp.sendResetMsg(s.id.header(resetTag), true)

	s.clLock.Unlock()

	s.mp.chLock.Lock()
	delete(s.mp.channels, s.id)
	s.mp.chLock.Unlock()

	return nil
}

func (s *Stream) cancelDeadlines() {
	s.deadlineLock.Lock()
	defer s.deadlineLock.Unlock()

	if s.rDeadlineCancel != nil {
		s.rDeadlineCancel()
		s.rDeadlineCtx = nil
		s.rDeadlineCancel = nil
	}

	if s.wDeadlineCancel != nil {
		s.wDeadlineCancel()
		s.wDeadlineCtx = nil
		s.wDeadlineCancel = nil
	}
}

func (s *Stream) SetDeadline(t time.Time) error {
	s.deadlineLock.Lock()
	defer s.deadlineLock.Unlock()
	if s.isClosed() {
		return errStreamClosed
	}
	s.setReadDeadline(t)
	s.setWriteDeadline(t)
	return nil
}

func (s *Stream) SetReadDeadline(t time.Time) error {
	s.deadlineLock.Lock()
	defer s.deadlineLock.Unlock()
	s.setReadDeadline(t)
	return nil
}

func (s *Stream) setReadDeadline(t time.Time) {
	if s.rDeadlineCancel != nil {
		s.rDeadlineCancel()
	}
	if t.IsZero() {
		s.rDeadlineCtx = nil
		s.rDeadlineCancel = nil
	} else {
		s.rDeadlineCtx, s.rDeadlineCancel = context.WithDeadline(context.Background(), t)
	}
}

func (s *Stream) SetWriteDeadline(t time.Time) error {
	s.deadlineLock.Lock()
	defer s.deadlineLock.Unlock()
	if s.isClosed() {
		return errStreamClosed
	}
	s.setWriteDeadline(t)
	return nil
}

func (s *Stream) setWriteDeadline(t time.Time) {
	if s.wDeadlineCancel != nil {
		s.wDeadlineCancel()
	}
	if t.IsZero() {
		s.wDeadlineCtx = nil
		s.wDeadlineCancel = nil
	} else {
		s.wDeadlineCtx, s.wDeadlineCancel = context.WithDeadline(s.closedLocal, t)
	}
}
