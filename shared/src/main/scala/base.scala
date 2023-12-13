package gears.async.asyncio

import java.nio.ByteBuffer
import scala.collection.mutable
import gears.util.either
import gears.util.either._
import gears.async.Async
import gears.async.Async.Source

/** The general buffer type of the traits. */
type Buffer = java.nio.ByteBuffer

/** An asynchronous reader. */
trait Reader:
  /** Creates a source that reads into the given (shared) buffer. Every read
    * event returns the number of bytes written to the buffer.
    */
  def readBuf(buf: Buffer): Source[Result[Int]]

/** An asynchronous writer. */
trait Writer:
  /** Creates a source that writes into the given (shared) buffer. Every send
    * event returns the number of bytes written to the buffer.
    */
  def writeBuf(buf: Buffer): Source[Result[Int]]

/** Wraps a reader and provides with with buffering capabilities.
  *
  * Note that the nature of buffering means that buffered reads are not raceable
  * (as buffered reads might automatically over-poll), and reads that have been
  * done cannot be rolled back.
  *
  * A [[BufferedReader]] is *not* thread-safe.
  */
class BufferedReader(bufSize: Int)(r: Reader):
  assert(bufSize > 0, "buffer size must be larger than 0")
  // the buffer we have
  private val buffer = java.nio.ByteBuffer.allocate(bufSize)
  private var gotEOF = false

  /** Tries to fill the given buffer. Returns the number of bytes filled. If no
    * bytes were filled, and the reader returns [[Error.EOF]], [[readBuf]] also
    * returns [[Error.EOF]].
    */
  def readBuf(buf: Buffer)(using Async) = either:
    if gotEOF then Left(Error.EOF).?
    else ???

  private val readSrc = r.readBuf(buffer)
  private def fillBuffer()(using Async) =
    if gotEOF then Left(Error.EOF)
    else
      readSrc.awaitResult match
        case Left(Error.EOF) =>
          gotEOF = true
          Right(0)
        case v => v

  /** Get the underlying buffer. Accessing this buffer is *dangerous* and should
    * only be used if this BufferedReader is not meant to be used again.
    */
  // inline def getBuffer: Seq[Byte] = buffer.toSeq

  /** Returns whether there is no more data to be read. */
  inline def isEOF = !buffer.hasRemaining() && gotEOF

/** Possible errors that could occur during IO. */
enum Error:
  case EOF // end of the reader has been reached.

/** The error type for IO operations. */
type Result[T] = Either[Error, T]

/** The exception type that wraps the error num, representing an unwrapped error
  * that happened during IO operations.
  */
class AsyncIOException(err: Error) extends Exception

extension [T](r: Result[T])
  /** Unwraps the IO operation result. Throws [[AsyncIOException]] on unwrap
    * failure.
    */
  def get = r match
    case Left(value)  => throw AsyncIOException(value)
    case Right(value) => value
