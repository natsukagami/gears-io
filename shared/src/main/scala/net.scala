package gears.async.net

import gears.async.Async
import gears.async.asyncio.{Reader, Writer, Buffer}
import java.io.Closeable
import gears.async.asyncio.Result
import java.net.SocketAddress

// Represents a connected TCP stream.
abstract class TcpStream extends Reader, Writer, Closeable:
  def localAddress: SocketAddress
  def remoteAddress: SocketAddress

// Represents a TCP server/listener.
abstract class TcpListener extends Closeable:
  type Stream <: TcpStream

  def accept()(using Async): Result[Stream]

  def localAddress: SocketAddress

trait TcpSupport:
  type Stream <: TcpStream
  type Listener <: TcpListener { type Stream = TcpSupport.this.Stream }

  def connect(address: SocketAddress): Result[Stream]
  def listen(address: SocketAddress): Result[Listener]
