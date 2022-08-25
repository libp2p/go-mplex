package main

import (
	"context"
	"fmt"
	"io"
	"net"
	"sync"

	mplex "github.com/libp2p/go-mplex"
)

var jsTestData = "test data from js %d"
var goTestData = "test data from go %d"

func main() {
	conn, err := net.Dial("tcp4", "127.0.0.1:9991")
	if err != nil {
		panic(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sess, err := mplex.NewMultiplex(conn, true, nil)
	if err != nil {
		panic(err)
	}
	defer sess.Close()

	var wg sync.WaitGroup

	for i := 0; i < 100; i++ {
		wg.Add(1)

		go func() {
			defer wg.Done()
			s, err := sess.NewStream(ctx)
			if err != nil {
				panic(err)
			}
			readWrite(s)
		}()
	}
	for i := 0; i < 100; i++ {
		s, err := sess.Accept()
		if err != nil {
			panic(err)
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			readWrite(s)
		}()
	}
	wg.Wait()
}

func readWrite(s *mplex.Stream) {
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		for i := 0; i < 100; i++ {
			_, err := fmt.Fprintf(s, goTestData, i)
			if err != nil {
				panic(err)
			}
		}
		err := s.CloseWrite()
		if err != nil {
			panic(err)
		}
	}()
	go func() {
		defer wg.Done()
		for i := 0; i < 100; i++ {
			expected := fmt.Sprintf(jsTestData, i)
			actual := make([]byte, len(expected))
			_, err := io.ReadFull(s, actual)
			if err != nil {
				panic(err)
			}
			if expected != string(actual) {
				panic("bad bytes")
			}
		}
		buf, err := io.ReadAll(s)
		if err != nil {
			panic(err)
		}
		if len(buf) > 0 {
			panic("expected EOF")
		}
	}()
	wg.Wait()
	err := s.Close()
	if err != nil {
		panic(err)
	}
}
