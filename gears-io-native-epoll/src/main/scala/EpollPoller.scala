package gears.async.asyncio.epoll

import gears.async.Listener
import gears.async.Async.Source
import scala.util.{Try, Success}
import java.nio.channels.ReadPendingException
import gears.async.asyncio.epoll.unsafe.epoll.{EPOLLIN, EPOLLOUT}
import scala.collection.mutable
import gears.async.asyncio.epoll.unsafe.epoll.epoll_ctl
import java.io.Closeable
import scala.scalanative.posix.unistd
import scala.scalanative.posix.string
import scala.scalanative.libc.errno

class IOException(error: Int) extends Exception:
  override def toString(): String =
    "IO Error: " + scala.scalanative.unsafe.fromCString(string.strerror(error))

class Poller(epfd: Int) extends Closeable:

  override def close(): Unit =
    if unistd.close(epfd) != 0 then throw IOException(errno.errno)

  val handles = mutable.Set[PollHandle]()

  def registerFd(fd: Int, read: Boolean, write: Boolean) =
    val handle = PollHandle()
    epoll_ctl(epfd, ???, fd, ???)

class MonitorChange() extends Source[Try[Int]]:
  // Increases every time the monitor is updated.
  @volatile private var counter = 0
  @volatile private var listener: Listener[Try[Int]] = null

  override def poll(k: Listener[Try[Int]]): Boolean =
    false // assume nothing is coming until update()
  override def onComplete(k: Listener[Try[Int]]): Unit = synchronized:
    if this.listener != null then
      throw /* TODO make this the correct exception */ ReadPendingException()
    this.listener = listener
  override def dropListener(k: Listener[Try[Int]]): Unit = synchronized:
    if listener == k then listener = null

  /* Returns whether the counter has been updated from the current known state. */
  def poll(current: Int = 0) = counter != current

  def onUpdate(current: Int = 0)(listener: Listener[Try[Int]]) =
    val runNow = synchronized:
      if this.listener != null then
        throw /* TODO make this the correct exception */ ReadPendingException()
      if current != counter then true
      else
        this.listener = listener
        false
    if runNow then listener.completeNow(Success(current), this)

  // Increment the counter and trigger the listener if it exists.
  def update() =
    val n = counter + 1
    counter = n
    val lis = listener
    listener = null
    if lis != null then lis.completeNow(Success(n), this)

class PollHandle():
  val read = MonitorChange()
  val write = MonitorChange()

  def notify(mask: Int) =
    if ((mask & EPOLLIN) > 0) then read.update()
    if ((mask & EPOLLOUT) > 0) then write.update()
