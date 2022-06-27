package multiplex

import (
	"context"
	"errors"
	"io"
	"sync"
	"time"

	"go.uber.org/multierr"
)

var (
	ErrStreamReset  = errors.New("stream reset")
	ErrStreamClosed = errors.New("closed stream")
)

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

type extraBufs struct {
	extra []byte
	// exbuf is for holding the reference to the beginning of the extra slice
	// for later memory pool freeing
	exbuf []byte
}

type Stream struct {
	id     streamID
	name   string
	dataIn chan []byte
	mp     *Multiplex

	extraBufs chan extraBufs

	rDeadline, wDeadline pipeDeadline

	clLock                        sync.Mutex
	writeCancelErr, readCancelErr error
	writeCancel, readCancel       chan struct{}
}

func (s *Stream) Name() string {
	return s.name
}

func (s *Stream) waitForData() ([]byte, error, bool) {
	select {
	case read, ok := <-s.dataIn:
		if !ok {
			return nil, io.EOF, false
		}
		return read, nil, false
	case <-s.readCancel:
		return nil, s.readCancelErr, true
	case <-s.rDeadline.wait():
		// Close reading to clean up resources
		s.cancelRead(errTimeout)
		return nil, errTimeout, true
	}
}

func (s *Stream) returnBuffers() {
loop:
	for {
		select {
		case es := <-s.extraBufs:
			if es.exbuf != nil {
				s.mp.putBufferInbound(es.exbuf)
			}
		case s.extraBufs <- extraBufs{}:
			break loop
		}
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
			s.mp.putBufferInbound(read)
		default:
			return
		}
	}
}

func (s *Stream) Read(b []byte) (int, error) {
	// In case read is canceled, this case must take precedence over reading from s.extraBuffs
	select {
	case <-s.readCancel:
		return 0, s.readCancelErr
	default:
	}

	var es extraBufs
	select {
	case <-s.readCancel:
		return 0, s.readCancelErr
	case es = <-s.extraBufs:
	}

	if es.extra == nil {
		var err error
		var readIsCanceled bool
		es.extra, err, readIsCanceled = s.waitForData()
		es.exbuf = es.extra
		if err != nil {
			if !readIsCanceled {
				// Special case that handles the situation of read not canceled
				select {
				case <-s.readCancel:
				case s.extraBufs <- es:
				}
			}
			return 0, err
		}
	}
	n := 0
	for es.extra != nil && n < len(b) {
		read := copy(b[n:], es.extra)
		n += read
		if read < len(es.extra) {
			es.extra = es.extra[read:]
		} else {
			if es.exbuf != nil {
				s.mp.putBufferInbound(es.exbuf)
			}
			var read []byte
			// Preload data
			select {
			case read = <-s.dataIn:
			default:
			}
			es.exbuf = read
			es.extra = read
		}
	}
	select {
	case <-s.readCancel:
		if es.exbuf != nil {
			s.mp.putBufferInbound(es.exbuf)
		}
		// it's not necessary to write to s.extraBuffs because reader above will go on the readCancel case
		return n, s.readCancelErr
	case s.extraBufs <- es:
	}
	return n, nil
}

func (s *Stream) Write(b []byte) (int, error) {
	var written int
	for written < len(b) {
		wl := len(b) - written
		if wl > ChunkSize {
			wl = ChunkSize
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
	select {
	case <-s.writeCancel:
		return 0, s.writeCancelErr
	default:
	}

	err := s.mp.sendMsg(s.wDeadline.wait(), s.writeCancel, s.id.header(messageTag), b)
	if err != nil {
		return 0, err
	}

	return len(b), nil
}

func (s *Stream) cancelWrite(err error) bool {
	s.wDeadline.close()

	s.clLock.Lock()
	defer s.clLock.Unlock()
	select {
	case <-s.writeCancel:
		return false
	default:
		s.writeCancelErr = err
		close(s.writeCancel)
		return true
	}
}

func (s *Stream) cancelRead(err error) bool {
	// Always unregister for reading first, even if we're already closed (or
	// already closing). When handleIncoming calls this, it expects the
	// stream to be unregistered by the time it returns.
	s.mp.chLock.Lock()
	delete(s.mp.channels, s.id)
	s.mp.chLock.Unlock()

	s.rDeadline.close()

	s.clLock.Lock()
	defer s.clLock.Unlock()
	select {
	case <-s.readCancel:
		return false
	default:
		s.readCancelErr = err
		close(s.readCancel)
		// This is the only place where it's safe to return these.
		s.returnBuffers()
		return true
	}
}

func (s *Stream) CloseWrite() error {
	if !s.cancelWrite(ErrStreamClosed) {
		// Check if we closed the stream _nicely_. If so, we don't need
		// to report an error to the user.
		if s.writeCancelErr == ErrStreamClosed {
			return nil
		}
		// Closed for some other reason. Report it.
		return s.writeCancelErr
	}

	ctx, cancel := context.WithTimeout(context.Background(), ResetStreamTimeout)
	defer cancel()

	err := s.mp.sendMsg(ctx.Done(), nil, s.id.header(closeTag), nil)
	// We failed to close the stream after 2 minutes, something is probably wrong.
	if err != nil && !s.mp.isShutdown() {
		log.Warnf("Error closing stream: %s; killing connection", err.Error())
		s.mp.Close()
	}
	return err
}

func (s *Stream) CloseRead() error {
	s.cancelRead(ErrStreamClosed)
	return nil
}

func (s *Stream) Close() error {
	return multierr.Combine(s.CloseRead(), s.CloseWrite())
}

func (s *Stream) Reset() error {
	s.cancelRead(ErrStreamReset)

	if s.cancelWrite(ErrStreamReset) {
		// Send a reset in the background.
		go s.mp.sendResetMsg(s.id.header(resetTag), true)
	}

	return nil
}

func (s *Stream) SetDeadline(t time.Time) error {
	s.rDeadline.set(t)
	s.wDeadline.set(t)
	return nil
}

func (s *Stream) SetReadDeadline(t time.Time) error {
	s.rDeadline.set(t)
	return nil
}

func (s *Stream) SetWriteDeadline(t time.Time) error {
	s.wDeadline.set(t)
	return nil
}
