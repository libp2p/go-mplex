package multiplex

import (
	"math/rand"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cevatbarisyilmaz/lossy"
)

func MakeLinearWriteDistribution() [][]byte {
	n := 1000
	itms := make([][]byte, n)
	for i := 0; i < n; i++ {
		itms[i] = make([]byte, i+1)
	}
	return itms
}

func MakeSmallPacketDistribution() [][]byte {
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

func BenchmarkSmallPackets(b *testing.B) {
	msgs := MakeSmallPacketDistribution()
	benchmarkPackets(b, msgs)
}

func BenchmarkSlowConnSmallPackets(b *testing.B) {
	msgs := MakeSmallPacketDistribution()
	pa, pb := net.Pipe()
	la := lossy.NewConn(pa, 100*1024, 30*time.Millisecond, 100*time.Millisecond, 0.0, 48)
	mpa := NewMultiplex(la, false)
	mpb := NewMultiplex(pb, true)
	benchmarkPacketsWithConn(b, msgs, mpa, mpb)
}

func BenchmarkLinearPackets(b *testing.B) {
	msgs := MakeLinearWriteDistribution()
	benchmarkPackets(b, msgs)
}

func benchmarkPackets(b *testing.B, msgs [][]byte) {
	pa, pb := net.Pipe()
	mpa := NewMultiplex(pa, false)
	mpb := NewMultiplex(pb, true)
	benchmarkPacketsWithConn(b, msgs, mpa, mpb)
}

func benchmarkPacketsWithConn(b *testing.B, msgs [][]byte, mpa, mpb *Multiplex) {
	defer mpa.Close()
	defer mpb.Close()

	streamPairs := make([][]*Stream, 0)
	for i := 0; i < 64; i++ {
		sa, err := mpa.NewStream()
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
	idx := int32(0)
	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		localIdx := atomic.AddInt32(&idx, 1)
		if localIdx >= 64 {
			panic("parallelism running into unallocated streams.")
		}
		localA := streamPairs[localIdx][0]
		localB := streamPairs[localIdx][1]

		var wg sync.WaitGroup
		wg.Add(1)
		go func() {
			defer wg.Done()
			receiveBuf := make([]byte, 2048)

			for {
				n, err := localB.Read(receiveBuf)
				if n == 0 || err != nil {
					return
				}
				atomic.AddUint64(&receivedBytes, uint64(n))
			}
		}()

		i := 0
		for {
			_, _ = localA.Write(msgs[i])
			i = (i + 1) % 1000
			if !pb.Next() {
				break
			}
		}
		localA.Close()
		wg.Wait()
	})
	b.SetBytes(int64(receivedBytes))
}
