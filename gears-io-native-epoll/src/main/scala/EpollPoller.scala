package gears.async.asyncio.epoll

import gears.async.Async
import gears.async.Async.Source
import gears.async.Cancellable
import gears.async.Listener
import gears.async.asyncio.epoll.unsafe.epoll._
import gears.async.asyncio.epoll.unsafe.epollImplicits._

import java.io.Closeable
import java.nio.channels.ReadPendingException
import java.util.concurrent.CancellationException
import scala.annotation.tailrec
import scala.collection.mutable
import scala.concurrent.duration._
import scala.scalanative.libc.errno
import scala.scalanative.posix.errno.EINTR
import scala.scalanative.posix.fcntl
import scala.scalanative.posix.string
import scala.scalanative.posix.unistd
import scala.scalanative.runtime.Intrinsics
import scala.scalanative.runtime._
import scala.scalanative.unsafe.Zone
import scala.scalanative.unsafe._
import scala.scalanative.unsigned._
import scala.util.Failure
import scala.util.Success
import scala.util.Try
import javax.management.monitor.Monitor

class IOException(error: Int, extra: String = "") extends Exception:
  override def toString(): String =
    "IO Error: " + fromCString(string.strerror(error)) + extra

class EpollPoller(epfd: Int) extends Closeable, Poller:
  import EpollPoller.*
  // store handles for lookup
  private val handles = mutable.Map[Int, EpollHandle]()

  val (pipeRead, pipeWrite) =
    Zone.acquire: zone =>
      val pipeFds = alloc[CArray[CInt, Nat._2]]()(using zone)
      if unistd.pipe(pipeFds.at(0)) < 0 then throw IOException(errno.errno)
      registerFd(!pipeFds.at(0), read = true, write = false)
      (!pipeFds.at(0), !pipeFds.at(1))

  override def close(): Unit =
    for toClose <- Seq(epfd, pipeRead, pipeWrite) do
      if unistd.close(toClose) != 0 then
        throw IOException(errno.errno, s" while trying to close $toClose")

  // Overrides handles with cancellation what is aware of this poller.
  // Note that this handle must still be manually added to the `handles` set.
  private class EpollHandle(fd: Int) extends PollHandle(fd):
    override def cancel() = EpollPoller.this.synchronized:
      epoll_ctl(epfd, EPOLL_CTL_DEL, fd, null)
      super.cancel()
      handles.remove(fd)

  override def wake() = synchronized:
    val size = 1.toUInt
    val buf = stackalloc[Byte](size)
    !buf = 1
    unistd.write(pipeWrite, buf, size)

  private def drain() = synchronized:
    val size = 32.toUInt
    val buf = stackalloc[Byte](size)
    while unistd.read(pipeRead, buf, size) > 0 do println(s"draining...")

  override def poll(timeout: Duration) =
    drain()
    Zone.acquire: zone =>
      pollImpl(timeout)(using zone)

  private def pollImpl(timeout: Duration)(using Zone) =
    val MAX_EVENTS = 64
    val events = alloc[epoll_event](MAX_EVENTS)
    @tailrec def loop(timeoutSecs: Int): Unit =
      println(s"Waiting for ${handles.keys.toSeq}")
      val count = epoll_wait(epfd, events, MAX_EVENTS, timeoutSecs)
      if count < 0 then
        if errno.errno == EINTR then ()
        else throw IOException(errno.errno)
      else
        for i <- 0 until count do
          val event = events + i
          val handle = event.data.toInt
          handles.get(handle).map(_.notify(event.events.toInt))
          // println(s"Notifying ${handle.fd} with ${event.events}")
        if count == MAX_EVENTS then loop(0)

    val timeoutSecs =
      if timeout < 0.seconds then -1 else timeout.toSeconds.toInt
    loop(timeoutSecs)

  def registerFd(
      fd: Int,
      read: Boolean,
      write: Boolean
  ): PollHandle = synchronized:
    val handle = new EpollHandle(fd)
    Zone.acquire: zone =>
      setNonBlocking(fd)
      val event = alloc[epoll_event]()(using zone)
      event.events =
        (EPOLLET | (if read then EPOLLIN else 0) | (if write then EPOLLOUT
                                                    else 0)).toUInt
      event.data = fromRawPtr(Intrinsics.castIntToRawPtr(fd))
      handles += (fd -> handle)
      if epoll_ctl(epfd, EPOLL_CTL_ADD, fd, event) == -1 then
        handles -= fd
        throw IOException(errno.errno, s" while adding $fd to $epfd")
      handle

  private def setNonBlocking(fd: Int) =
    // get the fd flags and modify it if needed
    val statusFlags = fcntl.fcntl(fd, fcntl.F_GETFL, 0)
    if statusFlags < 0 then
      throw IOException(errno.errno, s" while getting flags for $fd")
    if (statusFlags & fcntl.O_NONBLOCK) == 0 then
      if fcntl.fcntl(
          fd,
          fcntl.F_SETFL,
          (statusFlags | fcntl.O_NONBLOCK)
        ) < 0
      then throw IOException(errno.errno, " while setting NONBLOCK for $fd")

object EpollPoller:
  class MonitorChange extends Source[Try[Int]], Cancellable:
    // Increases every time the monitor is updated.
    @volatile private var _counter = 0
    private val listeners: mutable.Set[Listener[Try[Int]]] =
      mutable.Set()

    def counter = _counter

    override def poll(k: Listener[Try[Int]]): Boolean =
      false // assume nothing is coming until update()
    override def onComplete(k: Listener[Try[Int]]): Unit = synchronized:
      listeners += k
    override def dropListener(k: Listener[Try[Int]]): Unit = synchronized:
      listeners -= k

    /* Returns whether the counter has been updated from the current known state. */
    def poll(current: Int = 0) = current < _counter

    def onUpdate(listener: Listener[Try[Int]], current: Int): Unit =
      val runNow = synchronized:
        if current < _counter then true
        else
          listeners += listener
          false
      if runNow then listener.completeNow(Success(_counter), this)

    def onUpdate(current: Int)(using Async): Int =
      if current < _counter then _counter
      else this.await

    def cancel() =
      val toLoop = synchronized:
        val ls = listeners.toSeq
        listeners.clear()
        ls
      for listener <- toLoop do
        listener.completeNow(Failure(CancellationException()), this)

    // Increment the counter and trigger the listener if it exists.
    private[EpollPoller] def update() =
      val n = _counter + 1
      _counter = n
      val toLoop = synchronized:
        val ls = listeners.toSeq
        listeners.clear()
        ls
      for listener <- toLoop do listener.completeNow(Success(n), this)
  end MonitorChange

  class PollHandle(val fd: Int) extends Cancellable:
    val read = MonitorChange()
    val write = MonitorChange()

    private[EpollPoller] def notify(mask: Int) =
      if ((mask & EPOLLIN) > 0) then read.update()
      if ((mask & EPOLLOUT) > 0) then write.update()

    def cancel() =
      read.cancel()
      write.cancel()
