package multiplex

import (
	"net"
	"testing"
)

func TestBasicStreams(t *testing.T) {
	a, b := net.Pipe()

	mpa := NewMultiplex(a, false)
	mpb := NewMultiplex(b, true)

	mes := []byte("Hello world")
	go func() {
		err := mpb.Serve(func(s *Stream) {
			_, err := s.Write(mes)
			if err != nil {
				t.Fatal(err)
			}

			err = s.Close()
			if err != nil {
				t.Fatal(err)
			}
		})
		if err != nil {
			t.Fatal(err)
		}
	}()

	go func() {
		mpa.Serve(func(s *Stream) {
			s.Close()
		})
	}()

	s := mpa.NewStream()

	buf := make([]byte, len(mes))
	n, err := s.Read(buf)
	if err != nil {
		t.Fatal(err)
	}

	if n != len(mes) {
		t.Fatal("read wrong amount")
	}

	if string(buf) != string(mes) {
		t.Fatal("got bad data")
	}

	s.Close()

	mpa.Close()
	mpb.Close()
}
