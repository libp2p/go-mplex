# DEPRECATED: go-mplex

mplex is deprecated. When on TCP, we recommend using [Yamux](https://github.com/libp2p/go-yamux/) instead.
If TCP is not a requirement, consider using QUIC instead.

Ref: Discussion around mplex deprecation: https://github.com/libp2p/specs/issues/553

---

[![](https://img.shields.io/badge/made%20by-Protocol%20Labs-blue.svg?style=flat-square)](https://protocol.ai)
[![Go Reference](https://pkg.go.dev/badge/github.com/libp2p/go-mplex.svg)](https://pkg.go.dev/github.com/libp2p/go-mplex)
[![](https://img.shields.io/badge/project-libp2p-yellow.svg?style=flat-square)](https://libp2p.io/)
[![](https://img.shields.io/badge/freenode-%23libp2p-yellow.svg?style=flat-square)](http://webchat.freenode.net/?channels=%23libp2p)
[![Discourse posts](https://img.shields.io/discourse/https/discuss.libp2p.io/posts.svg)](https://discuss.libp2p.io)

A super simple [stream muxing](https://docs.libp2p.io/concepts/stream-multiplexing/) library implementing [mplex](https://github.com/libp2p/specs/tree/master/mplex).

Users should prefer [yamux](https://github.com/libp2p/go-libp2p/tree/master/p2p/muxer/yamux) over mplex. We would like to deprecate mplex in the future.

## Usage

```go
mplex := multiplex.NewMultiplex(mysocket)

s, _ := mplex.NewStream()
s.Write([]byte("Hello World!"))
s.Close()

os, _ := mplex.Accept()
// echo back everything received
io.Copy(os, os)
```

---

The last gx published version of this module was: 0.2.35: QmWGQQ6Tz8AdUpxktLf3zgnVN9Vy8fcWVezZJSU3ZmiANj
