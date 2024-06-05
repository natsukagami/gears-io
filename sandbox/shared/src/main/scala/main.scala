package sandbox

import sandbox.async.given

import java.net.InetSocketAddress
import gears.async.Async
import gears.async.net, net.*
import gears.async.asyncio.*
import gears.util.*, either.*
import scala.annotation.tailrec
import java.nio.charset.StandardCharsets
import gears.async.*
import gears.async.Future.MutableCollector
import scala.util.Using
import scala.util.boundary
import java.nio.ByteBuffer
import java.io.IOException

def runClient(client: net.TcpStream)(using Async) =
  Using(client): client =>
    either:
      val buffer = ByteBuffer.allocate(100)
      boundary:
        while true do
          client.readBuf(buffer) match
            case Left(asyncio.Error.EOF) => boundary.break()
            case r                       => r.?
      val arr =
        buffer.flip()
        val b = new Array[Byte](buffer.remaining())
        buffer.get(b)
        b
      val str = new String(arr, StandardCharsets.UTF_8)
      println(s"Read: $str ${arr.toSeq}")
      str.trim().toInt
  .get

def runServer(server: net.TcpListener, runs: Int)(using Async) =
  Async.group:
    either:
      val futures =
        for i <- 1 to runs
        yield Future:
          val conn = server.accept().?
          // println(s"accepting $i")
          Using(conn): conn =>
            either:
              val buf = ByteBuffer.allocate(10)
              val toSend = i.toString().getBytes(StandardCharsets.UTF_8)
              buf.put(toSend)
              buf.flip()
              conn.writeBuf(buf).?
              // println(s"wrote $i")
              conn.close()
      futures.awaitAll.foreach(_.get.?)
  server.close()

@main def main(n: Int) =
  val address = "127.0.0.1"
  val port = 65432
  val addr = InetSocketAddress(address, port)

  Async.blocking:
    either:
      // set up server
      val server = TcpSupport.listen(addr).?
      val serverFut = Future(runServer(server, n))

      // set up clients
      val futures =
        for i <- 1 to n
        yield Future:
          either:
            val client = TcpSupport.connect(addr).?
            // println(s"connected")
            runClient(client).?
      serverFut.await
      futures.awaitAll.map(_.?)
    .match
      case Left(value)  => throw Exception(s"IO error: $value")
      case Right(value) => ()
