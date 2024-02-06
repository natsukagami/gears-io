package gears.async.jvm.net

import gears.async.asyncio.Error
import gears.async.net as net
import gears.async.asyncio.Buffer
import gears.async.asyncio.Result
import gears.async.asyncio.IOFuture
import gears.async
import gears.async.Async
import java.net.SocketAddress
import gears.util.either
import scala.util.Try

/** The simplest possible TCP stream implementation, technically just wrapping
  * Java's [[java.net.Socket]] and rely on Virtual Thread suspensions.
  */
class TcpStream(val socket: java.net.Socket) extends net.TcpStream:
  val input = socket.getInputStream()
  val output = socket.getOutputStream()

  override def readBuf(buf: Buffer)(using Async): Result[Unit] = either:
    val bytesRead = input.read(
      buf.array(),
      buf.arrayOffset() + buf.position(),
      buf.limit() - buf.position()
    )
    if bytesRead == -1 then either.error(Error.EOF)
    else buf.position(buf.position() + bytesRead)

  override def close(): Unit =
    socket.close()

  override def writeBuf(buf: Buffer)(using Async): Result[Unit] = either:
    output.write(
      buf.array(),
      buf.arrayOffset() + buf.position(),
      buf.limit() - buf.position()
    )
    buf.position(buf.limit())

  override lazy val localAddress: SocketAddress = socket.getLocalSocketAddress()
  override lazy val remoteAddress: SocketAddress =
    socket.getRemoteSocketAddress()

class TcpListener(val socket: java.net.ServerSocket) extends net.TcpListener:
  type Stream = TcpStream

  override def close(): Unit = socket.close()

  override def accept()(using Async): Result[Stream] =
    // spawn a virtual thread and try to accept, we can cancel with interrupt
    var stream: Try[java.net.Socket] = null
    val thread = Thread
      .ofVirtual()
      .start: () =>
        stream = Try { socket.accept() }
    async.cancellationScope(() => thread.interrupt()):
      thread.join()
      Right(TcpStream(stream.get))

  override def localAddress: SocketAddress = socket.getLocalSocketAddress()

object JvmTcpSupport extends net.TcpSupport {
  type Stream = TcpStream
  type Listener = TcpListener

  override def connect(address: SocketAddress): Result[Stream] =
    val socket = java.net.Socket()
    socket.connect(address)
    Right(TcpStream(socket))

  override def listen(address: SocketAddress): Result[Listener] =
    val socket = java.net.ServerSocket()
    socket.bind(address)
    Right(TcpListener(socket))
}
