package multiplex

import (
	"math/rand"
	"net"
	"testing"
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

func BenchmarkLinearPackets(b *testing.B) {
	msgs := MakeLinearWriteDistribution()
	benchmarkPackets(b, msgs)
}

func benchmarkPackets(b *testing.B, msgs [][]byte) {
	pa, pb := net.Pipe()
	mpa := NewMultiplex(pa, false)
	mpb := NewMultiplex(pb, true)
	defer mpa.Close()
	defer mpb.Close()

	b.RunParallel(func(pb *testing.PB) {
		receiveBuf := make([]byte, 2048)
		sa, err := mpa.NewStream()
		if err != nil {
			b.Error(err)
		}
		sb, err := mpb.Accept()
		if err != nil {
			b.Error(err)
		}
		i := 0
		for {
			_, _ = sa.Write(msgs[i])
			_, _ = sb.Read(receiveBuf)
			i = (i + 1) % 1000
			if !pb.Next() {
				return
			}
		}
	})
}
