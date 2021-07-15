const assert = require('assert')

const Mplex = require('libp2p-mplex')
const pipe = require('it-pipe')
const { collect } = require('streaming-iterables')
const tcp = require('tcp')
const { BufferList } = require('bl')
const toConnection = require('libp2p-tcp/src/socket-to-conn')

const jsTestData = 'test data from js'
const goTestData = 'test data from go'

async function readWrite (stream) {
  await Promise.all([
    (async () => {
      let data = new BufferList(await pipe(stream, collect))
      let offset = 0
      for (let i = 0; i < 100; i++) {
        let expected = goTestData + ' ' + i
        assert.equal(data.slice(offset, offset + expected.length).toString(), expected)
        offset += expected.length
      }
    })(),
    stream.sink((async function* (data) {
      for (let i = 0; i < 100; i++) {
        yield jsTestData + ' ' + i
      }
    })())
  ])
}

const listener = tcp.createServer(async socket => {
  socket.on('close', () => listener.close())

  let muxer = new Mplex({onStream: readWrite})
  let conn = toConnection(socket)
  let promises = [pipe(conn, muxer, conn)]
  for (let i = 0; i < 100; i++) {
    const stream = muxer.newStream()
    promises.push(readWrite(stream))
  }
  await Promise.all(promises)
})

listener.listen(9991)
