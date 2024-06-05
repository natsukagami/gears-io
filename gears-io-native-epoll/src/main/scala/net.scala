package gears.async.asyncio.epoll

import gears.async.Async
import gears.async.asyncio.Buffer
import gears.async.asyncio.Error
import gears.async.asyncio.Result
import gears.async.net
import gears.util.either

import java.io.OutputStream
import java.net.SocketAddress
import java.nio.channels.Channels
import scala.scalanative.posix.errno
import scala.scalanative.posix.sys.{socket => posixSocket}
import scala.scalanative.posix.unistd
import scala.scalanative.runtime._
import scala.scalanative.unsafe._
import scala.scalanative.unsigned._
import scala.annotation.tailrec
import java.net.SocketException
import java.io.InputStream
import java.net.SocketTimeoutException
import gears.async.net.SocketOption
import java.net.InetSocketAddress

final class EpollTcpStream private[epoll] (private val socket: UnixSocket)(using
    poller: EpollPoller
) extends net.TcpStream:
  stream =>
  inline def debug[T](msg: String)(inline value: => T): T =
    val t = value
    println(s"$msg: $t")
    t

  override def close(): Unit = socket.close()

  override def readBuf(buf: Buffer)(using Async): Result[Unit] = either:
    val inputStream = new InputStream:
      override def read(): Int =
        val b = scala.Array(0.toByte)
        if read(b) == -1 then -1 else b(0).toByte
      override def read(b: scala.Array[Byte], off: Int, len: Int): Int =
        println(s"Read from [fd=${socket.getFd} #${socket.index}]")
        val read = socket.read(b, off, len)
        println(s"Read from [fd=${socket.getFd} #${socket.index}] -> $read")
        read
    if Channels.newChannel(inputStream).read(buf) == -1 then
      either.error(Error.EOF)

  override def writeBuf(buf: Buffer)(using Async): Result[Unit] = either:
    val outputStream = new OutputStream:
      override def write(b: Int): Unit = write(scala.Array(b.toByte))
      override def write(b: scala.Array[Byte], off: Int, len: Int): Unit =
        if (off > b.length || off < 0 || len < 0 || len > b.length - off) then
          throw new IndexOutOfBoundsException()
        socket.write(b, off, len)

    Channels.newChannel(outputStream).write(buf)

  override inline def localAddress: SocketAddress = socket.getLocalSocketAddress
  override inline def remoteAddress: SocketAddress =
    socket.getRemoteSocketAddress

class EpollTcpListener private[epoll] (socket: UnixServerSocket)(using
    poller: EpollPoller
) extends net.TcpListener:
  type Stream = EpollTcpStream

  override def close() = socket.close()

  override def accept()(using Async): Result[Stream] =
    val acceptSocket = socket.accept()
    Result.ok(EpollTcpStream(acceptSocket))

  override def localAddress: SocketAddress = socket.getLocalSocketAddress

trait EpollTcpSupport(using poller: EpollPoller) extends net.TcpSupport:
  type Stream = EpollTcpStream
  type Listener = EpollTcpListener

  override def connect(address: SocketAddress, options: Seq[SocketOption])(using
      Async
  ): Result[Stream] =
    require(address.isInstanceOf[InetSocketAddress])
    val socket = UnixClientSocket(isStreaming = true)
    // TODO set options
    // options.foreach(op => socket.setOption(op.key, op.value))
    socket.connect(address.asInstanceOf[InetSocketAddress])
    either.ok(EpollTcpStream(socket))

  override def listen(address: SocketAddress, options: Seq[SocketOption])(using
      Async
  ): Result[Listener] =
    require(address.isInstanceOf[InetSocketAddress])
    val addr = address.asInstanceOf[InetSocketAddress]
    val socket = UnixServerSocket(isStreaming = true)
    // TODO set options
    // options.foreach(op => socket.setOption(op.key, op.value))
    // socket.setReuseAddress(true)
    socket.bind(addr)
    socket.listen(50)
    either.ok(EpollTcpListener(socket))
