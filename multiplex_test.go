package multiplex

import (
	"context"
	"fmt"
	"io"
	"io/ioutil"
	"math/rand"
	"net"
	"os"
	"sync"
	"testing"
	"time"
)

func TestSlowReader(t *testing.T) {
	a, b := net.Pipe()

	mpa, err := NewMultiplex(a, false, nil)
	if err != nil {
		t.Fatal(err)
	}
	mpb, err := NewMultiplex(b, true, nil)
	if err != nil {
		t.Fatal(err)
	}

	defer mpa.Close()
	defer mpb.Close()

	mes := []byte("Hello world")

	sa, err := mpa.NewStream(context.Background())
	if err != nil {
		t.Fatal(err)
	}

	sb, err := mpb.Accept()
	if err != nil {
		t.Fatal(err)
	}

	res1 := make(chan error, 1)
	res2 := make(chan error, 1)

	const msgs = 1000
	go func() {
		defer sa.Close()
		var err error
		for i := 0; i < msgs; i++ {
			_, err = sa.Write(mes)
			if err != nil {
				break
			}
		}
		res1 <- err
	}()

	go func() {
		defer sb.Close()
		buf := make([]byte, len(mes))
		time.Sleep(time.Second)
		var err error
		for i := 0; i < msgs; i++ {
			time.Sleep(time.Millisecond)
			_, err = sb.Read(buf)
			if err != nil {
				break
			}
		}
		res2 <- err
	}()

	err = <-res1
	if err != nil {
		t.Fatal(err)
	}

	err = <-res2
	if err != nil {
		t.Fatal(err)
	}
}

func TestBasicStreams(t *testing.T) {
	a, b := net.Pipe()

	mpa, err := NewMultiplex(a, false, nil)
	if err != nil {
		t.Fatal(err)
	}

	mpb, err := NewMultiplex(b, true, nil)
	if err != nil {
		t.Fatal(err)
	}

	mes := []byte("Hello world")
	done := make(chan struct{})
	go func() {
		defer close(done)
		s, err := mpb.Accept()
		if err != nil {
			t.Error(err)
		}

		_, err = s.Write(mes)
		if err != nil {
			t.Error(err)
		}

		err = s.Close()
		if err != nil {
			t.Error(err)
		}
	}()
	defer func() { <-done }()

	s, err := mpa.NewStream(context.Background())
	if err != nil {
		t.Fatal(err)
	}

	buf, err := ioutil.ReadAll(s)
	if err != nil {
		t.Fatal(err)
	}

	if string(buf) != string(mes) {
		t.Fatal("got bad data")
	}

	s.Close()

	mpa.Close()
	mpb.Close()
}

func TestOpenStreamDeadline(t *testing.T) {
	a, _ := net.Pipe()
	mp, err := NewMultiplex(a, false, nil)
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	var counter int
	var deadlineExceeded bool
	for i := 0; i < 1000; i++ {
		if _, err := mp.NewStream(ctx); err != nil {
			if err != context.DeadlineExceeded {
				t.Fatalf("expected the error to be a deadline error, got %s", err.Error())
			}
			deadlineExceeded = true
			break
		}
		counter++
	}
	if counter == 0 {
		t.Fatal("expected at least some streams to open successfully")
	}
	if !deadlineExceeded {
		t.Fatal("expected a deadline error to occur at some point")
	}
}

func TestWriteAfterClose(t *testing.T) {
	a, b := net.Pipe()

	mpa, err := NewMultiplex(a, false, nil)
	if err != nil {
		t.Fatal(err)
	}

	mpb, err := NewMultiplex(b, true, nil)
	if err != nil {
		t.Fatal(err)
	}

	done := make(chan struct{})
	mes := []byte("Hello world")
	go func() {
		s, err := mpb.Accept()
		if err != nil {
			t.Error(err)
		}

		_, err = s.Write(mes)
		if err != nil {
			return
		}

		_, err = s.Write(mes)
		if err != nil {
			return
		}

		s.Close()

		close(done)
	}()

	s, err := mpa.NewStream(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	// wait for writes to complete and close to happen (and be noticed)
	<-done
	time.Sleep(time.Millisecond * 50)

	buf := make([]byte, len(mes)*10)
	n, _ := io.ReadFull(s, buf)
	if n != len(mes)*2 {
		t.Fatal("read incorrect amount of data: ", n)
	}

	// read after close should fail with EOF
	_, err = s.Read(buf)
	if err == nil {
		t.Fatal("read on closed stream should fail")
	}

	mpa.Close()
	mpb.Close()
}

func TestEcho(t *testing.T) {
	a, b := net.Pipe()

	mpa, err := NewMultiplex(a, false, nil)
	if err != nil {
		t.Fatal(err)
	}

	mpb, err := NewMultiplex(b, true, nil)
	if err != nil {
		t.Fatal(err)
	}

	mes := make([]byte, 40960)
	rand.Read(mes)
	go func() {
		s, err := mpb.Accept()
		if err != nil {
			t.Error(err)
		}

		defer s.Close()
		io.Copy(s, s)
	}()

	s, err := mpa.NewStream(context.Background())
	if err != nil {
		t.Fatal(err)
	}

	_, err = s.Write(mes)
	if err != nil {
		t.Fatal(err)
	}

	buf := make([]byte, len(mes))
	n, err := io.ReadFull(s, buf)
	if err != nil {
		t.Fatal(err)
	}

	if n != len(mes) {
		t.Fatal("read wrong amount")
	}

	if err := arrComp(buf, mes); err != nil {
		t.Fatal(err)
	}
	s.Close()

	mpa.Close()
	mpb.Close()
}

func TestFullClose(t *testing.T) {
	t.Skip("nonsensical flaky test")
	a, b := net.Pipe()
	mpa, err := NewMultiplex(a, false, nil)
	if err != nil {
		t.Fatal(err)
	}

	mpb, err := NewMultiplex(b, true, nil)
	if err != nil {
		t.Fatal(err)
	}

	mes := make([]byte, 40960)
	rand.Read(mes)
	s, err := mpa.NewStream(context.Background())
	if err != nil {
		t.Fatal(err)
	}

	{
		s2, err := mpb.Accept()
		if err != nil {
			t.Error(err)
		}

		_, err = s.Write(mes)
		if err != nil {
			t.Error(err)
		}
		s2.Close()
	}

	err = s.Close()
	if err != nil {
		t.Fatal(err)
	}

	if n, err := s.Write([]byte("foo")); err != ErrStreamClosed {
		t.Fatal("expected stream closed error on write to closed stream, got", err)
	} else if n != 0 {
		t.Fatal("should not have written any bytes to closed stream")
	}

	// We closed for reading, this should fail.
	if n, err := s.Read([]byte{0}); err != ErrStreamClosed {
		t.Fatal("expected stream closed error on read from closed stream, got", err)
	} else if n != 0 {
		t.Fatal("should not have read any bytes from closed stream, got", n)
	}

	mpa.Close()
	mpb.Close()
}

func TestHalfClose(t *testing.T) {
	a, b := net.Pipe()
	mpa, err := NewMultiplex(a, false, nil)
	if err != nil {
		t.Fatal(err)
	}

	mpb, err := NewMultiplex(b, true, nil)
	if err != nil {
		t.Fatal(err)
	}

	wait := make(chan struct{})
	mes := make([]byte, 40960)
	rand.Read(mes)
	go func() {
		s, err := mpb.Accept()
		if err != nil {
			t.Error(err)
		}

		defer s.Close()

		if err := s.CloseRead(); err != nil {
			t.Error(err)
		}

		<-wait
		_, err = s.Write(mes)
		if err != nil {
			t.Error(err)
		}
	}()

	s, err := mpa.NewStream(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	err = s.CloseWrite()
	if err != nil {
		t.Fatal(err)
	}

	bn, err := s.Write([]byte("foo"))
	if err == nil {
		t.Fatal("expected error on write to closed stream")
	}
	if bn != 0 {
		t.Fatal("should not have written any bytes to closed stream")
	}

	close(wait)

	buf, err := ioutil.ReadAll(s)
	if err != nil {
		t.Fatal(err)
	}

	if len(buf) != len(mes) {
		t.Fatal("read wrong amount", len(buf), len(mes))
	}

	if err := arrComp(buf, mes); err != nil {
		t.Fatal(err)
	}

	mpa.Close()
	mpb.Close()
}

func TestFuzzCloseConnection(t *testing.T) {
	a, b := net.Pipe()

	for i := 0; i < 1000; i++ {
		mpa, err := NewMultiplex(a, false, nil)
		if err != nil {
			t.Fatal(err)
		}

		mpb, err := NewMultiplex(b, true, nil)
		if err != nil {
			t.Fatal(err)
		}

		go mpa.Close()
		go mpa.Close()

		mpb.Close()
	}
}

func TestClosing(t *testing.T) {
	a, b := net.Pipe()

	mpa, err := NewMultiplex(a, false, nil)
	if err != nil {
		t.Fatal(err)
	}

	mpb, err := NewMultiplex(b, true, nil)
	if err != nil {
		t.Fatal(err)
	}

	_, err = mpb.NewStream(context.Background())
	if err != nil {
		t.Fatal(err)
	}

	_, err = mpa.Accept()
	if err != nil {
		t.Fatal(err)
	}

	err = mpa.Close()
	if err != nil {
		t.Fatal(err)
	}

	err = mpb.Close()
	if err != nil {
		// not an error, the other side is closing the pipe/socket
		t.Log(err)
	}
	if len(mpa.channels) > 0 || len(mpb.channels) > 0 {
		t.Fatal("leaked closed streams")
	}
}

func TestCloseChan(t *testing.T) {
	a, b := net.Pipe()

	mpa, err := NewMultiplex(a, false, nil)
	if err != nil {
		t.Fatal(err)
	}

	mpb, err := NewMultiplex(b, true, nil)
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*1)
	defer cancel()

	_, err = mpb.NewStream(ctx)
	if err != nil {
		t.Fatal(err)
	}

	_, err = mpa.Accept()
	if err != nil {
		t.Fatal(err)
	}

	go func() {
		mpa.Close()
	}()

	select {
	case <-ctx.Done():
		t.Fatal("did not receive from CloseChan for mpa within timeout")
	case <-mpa.CloseChan():
	}

	select {
	case <-ctx.Done():
		t.Fatal("did not receive from CloseChan for mpb within timeout")
	case <-mpb.CloseChan():
	}
}

func TestReset(t *testing.T) {
	a, b := net.Pipe()

	mpa, err := NewMultiplex(a, false, nil)
	if err != nil {
		t.Fatal(err)
	}

	mpb, err := NewMultiplex(b, true, nil)
	if err != nil {
		t.Fatal(err)
	}

	defer mpa.Close()
	defer mpb.Close()

	sa, err := mpa.NewStream(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	sb, err := mpb.Accept()
	if err != nil {
		t.Fatal(err)
	}

	buf := make([]byte, 10)

	sa.Reset()
	n, err := sa.Read(buf)
	if n != 0 {
		t.Fatalf("read %d bytes on reset stream", n)
	}
	if err == nil {
		t.Fatalf("successfully read from reset stream")
	}

	n, err = sa.Write([]byte("test"))
	if n != 0 {
		t.Fatalf("wrote %d bytes on reset stream", n)
	}
	if err == nil {
		t.Fatalf("successfully wrote to reset stream")
	}

	time.Sleep(200 * time.Millisecond)

	n, err = sb.Write([]byte("test"))
	if n != 0 {
		t.Fatalf("wrote %d bytes on reset stream", n)
	}
	if err == nil {
		t.Fatalf("successfully wrote to reset stream")
	}
	n, err = sb.Read(buf)
	if n != 0 {
		t.Fatalf("read %d bytes on reset stream", n)
	}
	if err == nil {
		t.Fatalf("successfully read from reset stream")
	}
}

func TestCancelRead(t *testing.T) {
	a, b := net.Pipe()

	mpa, err := NewMultiplex(a, false, nil)
	if err != nil {
		t.Fatal(err)
	}

	mpb, err := NewMultiplex(b, true, nil)
	if err != nil {
		t.Fatal(err)
	}

	defer mpa.Close()
	defer mpb.Close()

	sa, err := mpa.NewStream(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer sa.Reset()

	sb, err := mpb.Accept()
	if err != nil {
		t.Fatal(err)
	}
	defer sb.Reset()

	// spin off a read
	done := make(chan struct{})
	go func() {
		defer close(done)
		_, err := sa.Read([]byte{0})
		if err != ErrStreamClosed {
			t.Error(err)
		}
	}()
	// give it a chance to start.
	time.Sleep(time.Millisecond)

	// cancel it.
	err = sa.CloseRead()
	if err != nil {
		t.Fatal(err)
	}

	// It should be canceled.
	<-done

	// Writing should still succeed.
	_, err = sa.Write([]byte("foo"))
	if err != nil {
		t.Fatal(err)
	}
	err = sa.Close()
	if err != nil {
		t.Fatal(err)
	}
	// Data should still be sent.
	buf, err := ioutil.ReadAll(sb)
	if err != nil {
		t.Fatal(err)
	}
	if string(buf) != "foo" {
		t.Fatalf("expected foo, got %#v", err)
	}
}

func TestCancelWrite(t *testing.T) {
	a, b := net.Pipe()

	mpa, err := NewMultiplex(a, false, nil)
	if err != nil {
		t.Fatal(err)
	}

	mpb, err := NewMultiplex(b, true, nil)
	if err != nil {
		t.Fatal(err)
	}

	defer mpa.Close()
	defer mpb.Close()

	sa, err := mpa.NewStream(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer sa.Reset()

	sb, err := mpb.Accept()
	if err != nil {
		t.Fatal(err)
	}
	defer sb.Reset()

	// spin off a read
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		for {
			_, err := sa.Write([]byte("foo"))
			if err != nil {
				if err != ErrStreamClosed {
					t.Error("unexpected error", err)
				}
				return
			}
		}
	}()
	// give it a chance to fill up.
	time.Sleep(time.Millisecond)

	go func() {
		defer wg.Done()
		// close it.
		err := sa.CloseWrite()
		if err != nil {
			t.Error(err)
		}
	}()
	_, err = ioutil.ReadAll(sb)
	if err != nil {
		t.Fatalf("expected stream to be closed correctly")
	}

	// It should be canceled.
	wg.Wait()

	// Reading should still succeed.
	_, err = sb.Write([]byte("bar"))
	if err != nil {
		t.Fatal(err)
	}
	err = sb.Close()
	if err != nil {
		t.Fatal(err)
	}
	// Data should still be sent.
	buf, err := ioutil.ReadAll(sa)
	if err != nil {
		t.Fatal(err)
	}
	if string(buf) != "bar" {
		t.Fatalf("expected foo, got %#v", err)
	}
}

func TestCancelReadAfterWrite(t *testing.T) {
	a, b := net.Pipe()

	mpa, err := NewMultiplex(a, false, nil)
	if err != nil {
		t.Fatal(err)
	}

	mpb, err := NewMultiplex(b, true, nil)
	if err != nil {
		t.Fatal(err)
	}

	defer mpa.Close()
	defer mpb.Close()

	sa, err := mpa.NewStream(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer sa.Reset()

	sb, err := mpb.Accept()
	if err != nil {
		t.Fatal(err)
	}
	defer sb.Reset()

	// Write small messages till we would block.
	sa.SetWriteDeadline(time.Now().Add(time.Millisecond))
	for {
		_, err = sa.Write([]byte("foo"))
		if err != nil {
			if os.IsTimeout(err) {
				break
			} else {
				t.Fatal(err)
			}
		}
	}

	// Cancel inbound reads.
	sb.CloseRead()
	// We shouldn't read anything.
	n, err := sb.Read([]byte{0})
	if n != 0 || err != ErrStreamClosed {
		t.Fatal("got data", err)
	}
}

func TestResetAfterEOF(t *testing.T) {
	a, b := net.Pipe()

	mpa, err := NewMultiplex(a, false, nil)
	if err != nil {
		t.Fatal(err)
	}

	mpb, err := NewMultiplex(b, true, nil)
	if err != nil {
		t.Fatal(err)
	}

	defer mpa.Close()
	defer mpb.Close()

	sa, err := mpa.NewStream(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	sb, err := mpb.Accept()
	if err != nil {
		t.Fatal(err)
	}

	if err := sa.CloseWrite(); err != nil {
		t.Fatal(err)
	}

	n, err := sb.Read([]byte{0})
	if n != 0 || err != io.EOF {
		t.Fatal(err)
	}

	sb.Reset()

	n, err = sa.Read([]byte{0})
	if n != 0 || err != ErrStreamReset {
		t.Fatal(err)
	}
}

func TestOpenAfterClose(t *testing.T) {
	a, b := net.Pipe()

	mpa, err := NewMultiplex(a, false, nil)
	if err != nil {
		t.Fatal(err)
	}

	mpb, err := NewMultiplex(b, true, nil)
	if err != nil {
		t.Fatal(err)
	}

	sa, err := mpa.NewStream(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	sb, err := mpb.Accept()
	if err != nil {
		t.Fatal(err)
	}

	sa.Close()
	sb.Close()

	mpa.Close()

	s, err := mpa.NewStream(context.Background())
	if err == nil || s != nil {
		t.Fatal("opened a stream on a closed connection")
	}

	s, err = mpa.NewStream(context.Background())
	if err == nil || s != nil {
		t.Fatal("opened a stream on a closed connection")
	}

	mpb.Close()
}

func TestDeadline(t *testing.T) {
	a, b := net.Pipe()

	mpa, err := NewMultiplex(a, false, nil)
	if err != nil {
		t.Fatal(err)
	}

	mpb, err := NewMultiplex(b, true, nil)
	if err != nil {
		t.Fatal(err)
	}

	defer mpa.Close()
	defer mpb.Close()

	sa, err := mpa.NewStream(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	_, err = mpb.Accept()
	if err != nil {
		t.Fatal(err)
	}

	sa.SetDeadline(time.Now().Add(time.Second))

	_, err = sa.Read(make([]byte, 1024))
	if err != errTimeout {
		t.Fatal("expected timeout")
	}
}

func TestReadAfterClose(t *testing.T) {
	a, b := net.Pipe()

	mpa, err := NewMultiplex(a, false, nil)
	if err != nil {
		t.Fatal(err)
	}

	mpb, err := NewMultiplex(b, true, nil)
	if err != nil {
		t.Fatal(err)
	}

	defer mpa.Close()
	defer mpb.Close()

	sa, err := mpa.NewStream(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	sb, err := mpb.Accept()
	if err != nil {
		t.Fatal(err)
	}

	sa.Close()

	_, err = sb.Read(make([]byte, 1024))
	if err != io.EOF {
		t.Fatal("expected EOF")
	}
}

func TestFuzzCloseStream(t *testing.T) {
	timer := time.AfterFunc(10*time.Second, func() {
		// This is really the *only* reliable way to set a timeout on
		// this test...
		// Don't add *anything* to this function. The go scheduler acts
		// a bit funny when it encounters a deadlock...
		panic("timeout")
	})
	defer timer.Stop()

	a, b := net.Pipe()

	mpa, err := NewMultiplex(a, false, nil)
	if err != nil {
		t.Fatal(err)
	}

	mpb, err := NewMultiplex(b, true, nil)
	if err != nil {
		t.Fatal(err)
	}

	defer mpa.Close()
	defer mpb.Close()

	done := make(chan struct{})

	go func() {
		streams := make([]*Stream, 100)
		for i := range streams {
			var err error
			streams[i], err = mpb.NewStream(context.Background())
			if err != nil {
				t.Error(err)
			}

			go streams[i].Close()
			go streams[i].Close()
		}
		// Make sure they're closed before we move on.
		for _, s := range streams {
			if s == nil {
				continue
			}
			s.Close()
		}
		close(done)
	}()

	streams := make([]*Stream, 100)
	for i := range streams {
		var err error
		streams[i], err = mpa.Accept()
		if err != nil {
			t.Fatal(err)
		}
	}

	<-done

	for _, s := range streams {
		if s == nil {
			continue
		}
		err := s.Close()
		if err != nil {
			t.Fatal(err)
		}
	}

	time.Sleep(10 * time.Millisecond)

	nchannels := 0

	mpa.chLock.Lock()
	nchannels += len(mpa.channels)
	mpa.chLock.Unlock()

	mpb.chLock.Lock()
	nchannels += len(mpb.channels)
	mpb.chLock.Unlock()

	if nchannels > 0 {
		t.Fatal("leaked closed streams")
	}
}

func TestLargeWrite(t *testing.T) {
	oldChunkSize := ChunkSize
	ChunkSize = 16384
	t.Cleanup(func() {
		ChunkSize = oldChunkSize
	})

	a, b := net.Pipe()

	mpa, err := NewMultiplex(a, false, nil)
	if err != nil {
		t.Fatal(err)
	}
	mpb, err := NewMultiplex(b, true, nil)
	if err != nil {
		t.Fatal(err)
	}

	defer mpa.Close()
	defer mpb.Close()

	const msgsize = 65536
	msg := make([]byte, msgsize)
	if _, err := rand.Read(msg); err != nil {
		t.Fatal(err)
	}

	res1 := make(chan error, 1)
	res2 := make(chan error, 1)
	go func() {
		s, err := mpa.NewStream(context.Background())
		if err != nil {
			res1 <- err
			return
		}
		defer s.Close()

		_, err = s.Write(msg)
		res1 <- err
	}()

	go func() {
		s, err := mpb.Accept()
		if err != nil {
			res2 <- err
			return
		}

		defer s.Close()

		buf := make([]byte, msgsize)
		_, err = io.ReadFull(s, buf)
		if err != nil {
			res2 <- err
			return
		}

		res2 <- arrComp(buf, msg)
	}()

	err = <-res1
	if err != nil {
		t.Fatal(err)
	}

	err = <-res2
	if err != nil {
		t.Fatal(err)
	}
}

func arrComp(a, b []byte) error {
	msg := ""
	if len(a) != len(b) {
		msg += fmt.Sprintf("arrays differ in length: %d %d\n", len(a), len(b))
	}

	for i := 0; i < len(a) && i < len(b); i++ {
		if a[i] != b[i] {
			msg += fmt.Sprintf("content differs at index %d [%d != %d]", i, a[i], b[i])
			return fmt.Errorf(msg)
		}
	}
	if len(msg) > 0 {
		return fmt.Errorf(msg)
	}
	return nil
}
