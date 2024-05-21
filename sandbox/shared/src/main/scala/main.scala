package sandbox

import sandbox.async.given

import gears.async.net.TcpSupport
import java.net.InetSocketAddress
import gears.async.Async
import gears.async.net
import gears.async.asyncio.Result
import scala.annotation.tailrec
import gears.async.asyncio.BufferedReader
import gears.util.either.*
import gears.util.either
import java.nio.charset.StandardCharsets
import gears.async.Future.MutableCollector
import gears.async.Future
import java.nio.ByteBuffer
import scala.util.Using

def runClient(client: net.TcpStream)(using Async) =
  Using(client): client =>
    val buffered = BufferedReader(1000)(client)
    either:
      val out = buffered.readAll().?
      val str = new String(out.toArray, StandardCharsets.UTF_8)
      println(s"Read: $str")
      str.trim().toInt
  .get

def runServer(server: net.TcpListener, runs: Int)(using Async) =
  Async.group:
    either:
      val futures =
        for i <- 1 to runs
        yield Future:
          val conn = server.accept().?
          println(s"accepting $i")
          Using(conn): conn =>
            either:
              val buf = ByteBuffer.allocate(10)
              val toSend = i.toString().getBytes(StandardCharsets.UTF_8)
              buf.put(toSend)
              buf.flip()
              conn.writeBuf(buf).?
              println(s"wrote $i")
              conn.close()
      futures.awaitAll.foreach(_.get.?)
  server.close()

@main def main() =
  val address = "127.0.0.1"
  val port = 65432
  val addr = InetSocketAddress(address, port)

  Async.blocking:
    either:
      // set up server
      val n = 100
      val server = TcpSupport.listen(addr).?
      val serverFut = Future(runServer(server, n))

      // set up clients
      val futures =
        for i <- 1 to n
        yield Future:
          either:
            val client = TcpSupport.connect(addr).?
            runClient(client).?
      serverFut.await
      futures.awaitAll.map(_.?)
    .match
      case Left(value)  => throw Exception(s"IO error: $value")
      case Right(value) => ()
