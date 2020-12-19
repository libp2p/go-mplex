package multiplex

import (
	"context"
	"io"
	"math/rand"
	"net"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	netbench "github.com/libp2p/go-libp2p-testing/net"
	"google.golang.org/grpc/benchmark/latency"
)

func MakeLinearWriteDistribution(b *testing.B) [][]byte {
	b.Helper()

	n := 1000
	itms := make([][]byte, n)
	for i := 0; i < n; i++ {
		itms[i] = make([]byte, i+1)
	}
	return itms
}

func MakeSmallPacketDistribution(b *testing.B) [][]byte {
	b.Helper()

	n := 1000
	itms := make([][]byte, n)
	i := 0
	for ; i < int(float64(n)*(3.0/4.0)); i++ {
		itms[i] = make([]byte, 64)
	}
	for ; i < n; i++ {
		itms[i] = make([]byte, 1024)
	}
	rand.Shuffle(n, func(i, j int) { itms[i], itms[j] = itms[j], itms[i] })
	return itms
}

func TestSmallPackets(t *testing.T) {
	kbps, lat, err := netbench.FindNetworkLimit(testSmallPackets, 0.5)
	if err != nil {
		t.Skip()
	}
	slowdown, err := netbench.ParallelismSlowdown(testSmallPackets, kbps, lat)
	if err != nil {
		t.Fatal(err)
	}
	if slowdown > 0.15 && !raceEnabled {
		t.Fatalf("Slowdown from mplex was >15%%: %f", slowdown)
	}
}

func testSmallPackets(b *testing.B, n1, n2 net.Conn) {
	msgs := MakeSmallPacketDistribution(b)
	mpa := NewMultiplex(n1, false)
	mpb := NewMultiplex(n2, true)
	mp := runtime.GOMAXPROCS(0)
	runtime.GOMAXPROCS(mp)

	streamPairs := make([][]*Stream, 0)
	for i := 0; i < mp; i++ {
		sa, err := mpa.NewStream(context.Background())
		if err != nil {
			b.Error(err)
		}
		sb, err := mpb.Accept()
		if err != nil {
			b.Error(err)
		}
		streamPairs = append(streamPairs, []*Stream{sa, sb})
	}

	receivedBytes := uint64(0)
	sentBytes := uint64(0)
	idx := int32(0)
	b.ResetTimer()

	var wg sync.WaitGroup

	b.RunParallel(func(pb *testing.PB) {
		localIdx := atomic.AddInt32(&idx, 1) - 1
		localA := streamPairs[localIdx][0]
		localB := streamPairs[localIdx][1]

		wg.Add(1)
		go func() {
			defer wg.Done()
			defer localB.Close()
			receiveBuf := make([]byte, 2048)

			for {
				n, err := localB.Read(receiveBuf)
				if err != nil && err != io.EOF {
					b.Error(err)
				}
				if n == 0 || err == io.EOF {
					return
				}
				atomic.AddUint64(&receivedBytes, uint64(n))
			}
		}()
		defer localA.Close()
		i := 0
		for {
			n, err := localA.Write(msgs[i])
			atomic.AddUint64(&sentBytes, uint64(n))
			if err != nil && err != io.EOF {
				b.Error(err)
			}
			i = (i + 1) % 1000
			if !pb.Next() {
				break
			}
		}
	})
	b.StopTimer()
	wg.Wait()

	if sentBytes != receivedBytes {
		b.Fatal("sent != received", sentBytes, receivedBytes)
	}
	b.SetBytes(int64(receivedBytes))
	mpa.Close()
	mpb.Close()
}

func BenchmarkSmallPackets(b *testing.B) {
	msgs := MakeSmallPacketDistribution(b)
	benchmarkPackets(b, msgs)
}

func BenchmarkSlowConnSmallPackets(b *testing.B) {
	msgs := MakeSmallPacketDistribution(b)
	slowNetwork := latency.Network{
		Kbps:    100,
		Latency: 30 * time.Millisecond,
		MTU:     1500,
	}
	var wg sync.WaitGroup
	wg.Add(1)
	var lb net.Conn
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		b.Error(err)
	}
	slowL := slowNetwork.Listener(l)
	go func() {
		defer wg.Done()
		lb, err = slowL.Accept()
		if err != nil {
			b.Error(err)
		}
	}()
	dialer := slowNetwork.Dialer(net.Dial)
	la, err := dialer("tcp4", slowL.Addr().String())
	if err != nil {
		b.Error(err)
	}
	defer la.Close()
	wg.Wait()
	defer lb.Close()
	mpa := NewMultiplex(la, false)
	mpb := NewMultiplex(lb, true)
	defer mpa.Close()
	defer mpb.Close()
	benchmarkPacketsWithConn(b, 1, msgs, mpa, mpb)
}

func BenchmarkLinearPackets(b *testing.B) {
	msgs := MakeLinearWriteDistribution(b)
	benchmarkPackets(b, msgs)
}

func benchmarkPackets(b *testing.B, msgs [][]byte) {
	pa, pb := net.Pipe()
	defer pa.Close()
	defer pb.Close()
	mpa := NewMultiplex(pa, false)
	mpb := NewMultiplex(pb, true)
	defer mpa.Close()
	defer mpb.Close()
	benchmarkPacketsWithConn(b, 1, msgs, mpa, mpb)
}

func benchmarkPacketsWithConn(b *testing.B, parallelism int, msgs [][]byte, mpa, mpb *Multiplex) {
	streamPairs := make([][]*Stream, 0)
	for i := 0; i < parallelism*runtime.GOMAXPROCS(0); i++ {
		sa, err := mpa.NewStream(context.Background())
		if err != nil {
			b.Error(err)
		}
		sb, err := mpb.Accept()
		if err != nil {
			b.Error(err)
		}
		streamPairs = append(streamPairs, []*Stream{sa, sb})
	}

	receivedBytes := uint64(0)
	sentBytes := uint64(0)
	idx := int32(0)
	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		localIdx := atomic.AddInt32(&idx, 1) - 1
		localA := streamPairs[localIdx][0]
		localB := streamPairs[localIdx][1]

		var wg sync.WaitGroup
		wg.Add(1)
		go func() {
			defer wg.Done()
			receiveBuf := make([]byte, 2048)

			for {
				n, err := localB.Read(receiveBuf)
				if err != nil && err != io.EOF {
					b.Error(err)
				}
				if n == 0 || err == io.EOF {
					return
				}
				atomic.AddUint64(&receivedBytes, uint64(n))
			}
		}()

		i := 0
		for {
			n, err := localA.Write(msgs[i])
			atomic.AddUint64(&sentBytes, uint64(n))
			if err != nil && err != io.EOF {
				b.Error(err)
			}
			i = (i + 1) % 1000
			if !pb.Next() {
				break
			}
		}
		localA.Close()
		b.StopTimer()
		wg.Wait()
	})
	if sentBytes != receivedBytes {
		b.Fatal("sent != received", sentBytes, receivedBytes)
	}
	b.SetBytes(int64(receivedBytes))
}
