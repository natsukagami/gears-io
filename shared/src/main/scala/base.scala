package gears.async.asyncio

import java.nio.ByteBuffer
import scala.collection.mutable
import gears.util.either
import gears.util.either._
import gears.async.{Async, Future}
import gears.async.Async.Source
import scala.util.boundary
import scala.util.Success
import java.nio.channels.ReadPendingException
import java.util.concurrent.atomic.AtomicBoolean
import gears.async.Listener
import scala.util.Failure
import scala.util.Try
import gears.async.Async.OriginalSource
import java.util.concurrent.atomic.AtomicReference

/** The general buffer type of the traits. */
type Buffer = java.nio.ByteBuffer

/** An asynchronous reader. */
trait Reader:
  /** Creates a [[Future]] that resolves when the buffer is written to with
    * data. The [[Future]] might be resolved with an IO error (most notably,
    * [[Error.EOF]]).
    *
    * Safety notes:
    *   - **Cancellation**: cancelling this future should be optimistic -- the
    *     future might not resolve immediately after cancellation, and can still
    *     resolve with the buffers filled. Care must be taken to ensure data is
    *     not lost.
    *   - **Scope**: the returned [[Future]] is not required to be bound to any
    *     completion group, but will often be bound by the captured [[Async]]
    *     context of the implemented class (i.e. a `File`). Care must be taken
    *     to correctly link the [[Future]] to the desired context/completion
    *     group.
    *   - **Data Race**: the buffer is effectively owned by the [[Reader]] from
    *     the call to [[unsafeReadBuf]] until the returned [[Future]] is
    *     resolved. Any attempt to read from/write to the given buffer is
    *     considered a data race.
    *   - **Concurrent reads**: [[Reader]] implementations that does not allow
    *     multiple outstanding reads at the same time *must* throw
    *     [[ReadPendingException]] when multiple outstanding reads are detected.
    */
  def unsafeReadBuf(buf: Buffer): IOFuture[Unit]

  /** Wraps the [[Reader]] inside a new [[BufferedReader]]. */
  final def buffered(bufferSize: Int) = BufferedReader(bufferSize)(this)

/** An asynchronous writer. */
trait Writer:
  /** Creates a source that writes into the given (shared) buffer.
    */
  def unsafeWriteBuf(buf: Buffer)(using Async): IOFuture[Int]

/** Wraps a reader and provides with with buffering capabilities.
  *
  * A [[BufferedReader]] is *not* thread-safe, nor does it allow multiple
  * outstanding reads.
  */
class BufferedReader(bufSize: Int)(reader: Reader) extends Reader:
  private inline def errReadPending = Failure(ReadPendingException())

  /** Returns the underlying buffer. Panics if there are any pending reads. */
  def toBuffer =
    assert(readPending.get == null)
    buffer

  /** Returns whether the buffered reader has no more data to offer (i.e. any
    * more read requests will result in an [[Error.EOF]]). Panics if there are
    * any pending reads.
    */
  def isEOF =
    assert(readPending.get == null)
    gotEOF && !buffer.hasRemaining()

  def readBuf(buf: Buffer): Source[Result[Unit]] =
    new OriginalSource[Result[Unit]]:
      src =>
      // Sets the `readPending` flag, returns true if it's successful.
      private inline def setPending(k: Listener[Result[Unit]]): Boolean =
        if readPending.compareAndExchange(null, k) != null then
          k.completeNow(errReadPending, src)
          false
        else true
      private inline def unsetPending() =
        readPending.set(null)
        underlyingFut = null

      override def poll(k: Listener[Result[Unit]]): Boolean =
        if !setPending(k) then true
        else
          val result =
            if buffer.hasRemaining() then copyAndComplete(k)
            else if gotEOF then
              k.completeNow(Result.err(Error.EOF), src) || true
            else false
          unsetPending()
          result

      def copyAndComplete(k: Listener[Result[Unit]]): true =
        k.lockCompletely(src) match
          case Listener.Gone => ()
          case Listener.Locked =>
            buf.put(buffer.limit(buf.remaining()))
            k.complete(Result.ok(), src)
        true

      override def dropListener(k: Listener[Result[Unit]]): Unit =
        // cancel the future being created by the addListener
        // *if the corresponding listener is the one being waited on*.
        if readPending.get() == k then underlyingFut.cancel()

      override protected def addListener(k: Listener[Result[Unit]]): Unit =
        if setPending(k) then
          // we know the buffer is empty here, but no EOF, so we try to read to the underlying buffer.
          underlyingFut = reader.unsafeReadBuf(buffer)
          underlyingFut.onComplete(Listener: (res, _) =>
            res match
              case Failure(exception) => k.completeNow(res, src)
              case Success(Left(err)) =>
                if err == Error.EOF then gotEOF = true
                k.completeNow(res, src)
              case Success(Right(_)) =>
                copyAndComplete(k)
            unsetPending()
          )

  def unsafeReadBuf(buf: Buffer): IOFuture[Unit] =
    if readPending.compareAndExchange(null, BufferedReader.readListener) == null
    then Future.now(errReadPending)
    else if buffer.hasRemaining() then
      buf.put(buffer.limit(buf.remaining()))
      readPending.set(null)
      Future.now(Result.ok())
    else if gotEOF then
      readPending.set(null)
      Future.now(Result.err(Error.EOF))
    else
      Future.withResolver: r =>
        underlyingFut = reader.unsafeReadBuf(buffer)
        r.onCancel(() => underlyingFut.cancel())
        underlyingFut.onComplete(
          Listener((res, _) =>
            r.complete(Try:
              res.get match
                case Left(value) =>
                  if value == Error.EOF then gotEOF = true
                  Left(value)
                case Right(value) =>
                  // fill the buffer
                  buf.put(buffer.limit(buf.remaining()))
                  Right(())
            )
            readPending.set(null)
            underlyingFut = null
          )
        )
      underlyingFut

  // private stuff
  private val buffer = java.nio.ByteBuffer.allocate(bufSize)
  private var gotEOF = false
  private val readPending = AtomicReference[Listener[Result[Unit]]](null)
  private var underlyingFut: IOFuture[Unit] =
    null // underlying future being waited on, to be cancelled if needed.

object BufferedReader:
  // Simple listener that we have internally just for putting into `unsafeReadBuf`.
  private val readListener = Listener.acceptingListener((_, _) => ())

/** Possible errors that could occur during IO. */
enum Error:
  /** End of the reader has been reached. */
  case EOF

/** The error type for IO operations. */
type Result[T] = Try[Either[Error, T]]
type IOFuture[T] = Future[Either[Error, T]]

private object Result:
  def err(err: Error): Result[Nothing] = Success(Left(err))
  def ok[T](v: T): Result[T] = Success(Right(v))
  def ok(): Result[Unit] = Success(Right(()))

/** The exception type that wraps the error num, representing an unwrapped error
  * that happened during IO operations.
  */
class AsyncIOException(err: Error) extends Exception

extension [T](r: Result[T])
  /** Unwraps the IO operation result. Throws [[AsyncIOException]] on unwrap
    * failure.
    */
  def unwrap = r.get match
    case Left(value)  => throw AsyncIOException(value)
    case Right(value) => value
