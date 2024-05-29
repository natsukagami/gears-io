/* A lot of code in this package is taken from scala-native. */
package gears.io.epoll

import gears.async.*
import scala.scalanative.meta.LinktimeInfo
import scalanative.posix.sys.socket
import java.io.IOException
import java.net.{InetSocketAddress => SocketAddress}
import gears.async.asyncio.epoll.EpollPoller
import java.net.ConnectException
import scala.util.Success
import scala.util.Failure
import java.net.SocketException
import java.net.Inet6Address
import scala.scalanative.unsafe.CInt
import java.net.InetAddress
import scala.util.Try

private[epoll] class UnixSocket(isStreaming: Boolean)(using
    poller: EpollPoller
):
  /** The socket to be used */
  val fd =
    val af = if Helper.useIPv4Stack then socket.AF_INET else socket.AF_INET6
    val sockType = if isStreaming then socket.SOCK_STREAM else socket.SOCK_DGRAM
    val sock = socket.socket(af, sockType, 0)
    if sock < 0 then
      throw new IOException(
        s"Could not create a socket in address family: ${af}" +
          s" streaming: ${isStreaming}"
      )
    sock

  private var address: InetAddress = _
  private var port: Int = _
  private var localPort: Int = _

  private inline def subscribeRead =
    poller.registerFd(fd, read = true, write = false)
  private inline def subscribeWrite =
    poller.registerFd(fd, read = false, write = true)

  // Read and Write

  def readFut(buffer: Array[Byte], offset: Int, count: Int): Future[Int] =
    import scalanative.unsafe.*
    import scalanative.unsigned.*
    import scalanative.posix.errno.*
    inline def readNow =
      socket.recv(fd, buffer.at(offset), count.toUInt, 0).toInt

    Future.withResolver: resolver =>
      var handle: EpollPoller.PollHandle = null
      def loop(current: Int, bytes: Int): Unit =
        bytes match
          case _ if bytes > 0 => resolver.resolve(bytes)
          case 0              => resolver.resolve(if count == 0 then 0 else -1)
          case _ => // < 0
            errno match
              case e if e == EAGAIN || e == EWOULDBLOCK =>
                if handle == null then
                  handle = subscribeRead
                  resolver.onCancel(handle.cancel)
                handle.read.onUpdate(
                  Listener:
                    case (Success(next), _) => loop(next, readNow)
                    case (Failure(exc), _) => resolver.reject(exc),
                  current
                )
              case e =>
                resolver.reject(
                  new SocketException(s"read failed with errno $e")
                )

  // Connection set up

  def bind(addr: InetAddress, port: Int) =
    import scala.scalanative.unsafe.*
    import scala.scalanative.posix.netinet.in
    import scala.scalanative.posix.netinet.inOps._
    def throwCannotBind(addr: InetAddress, extraMsg: String = ""): Nothing =
      throw new java.net.BindException(
        "Couldn't bind to address " + addr.getHostAddress() +
          " on port " + port.toString + (if extraMsg == "" then s": $extraMsg"
                                         else "")
      )

    def bind4(addr: InetAddress, port: Int) =
      val sa4 = stackalloc[in.sockaddr_in]()
      val sa4Len = sizeof[in.sockaddr_in].toUInt
      addr match
        case addr: java.net.Inet4Address =>
          Helper.SockAddr.prepare4(addr, port, sa4)
        case _ => throwCannotBind(addr, s"$addr is not an IPv4 address")

      val bindRes = socket.bind(
        fd,
        sa4.asInstanceOf[Ptr[socket.sockaddr]],
        sa4Len
      )

      if bindRes < 0 then throwCannotBind(addr)

      this.localPort = fetchLocalPort(socket.AF_INET).getOrElse:
        throwCannotBind(addr)
    end bind4

    def bind6(addr: InetAddress, port: Int) =
      val sa6 = stackalloc[in.sockaddr_in6]()
      val sa6Len = sizeof[in.sockaddr_in6].toUInt

      // By contract, all the bytes in sa6 are zero going in.
      Helper.SockAddr.prepare6(addr, port, sa6)

      val bindRes = socket.bind(
        fd,
        sa6.asInstanceOf[Ptr[socket.sockaddr]],
        sa6Len
      )

      if bindRes < 0 then throwCannotBind(addr)

      this.localPort = fetchLocalPort(sa6.sin6_family.toInt).getOrElse:
        throwCannotBind(addr)
    end bind6

    if Helper.useIPv4Stack then bind4(addr, port) else bind6(addr, port)
  end bind

  def listen(backlog: Int): Unit =
    withErrno()(socket.listen(fd, backlog))

  private inline def withErrno(
      inline msg: Int => String = err => s"Socket failed with errno = $err"
  )(inline op: => Int) =
    val res = op
    if res >= 0 then res
    else
      val err = scalanative.posix.errno.errno
      throw new IOException(msg(err))

  def connect(address: SocketAddress)(using Async) = connectFut(address).await
  def connectFut(address: SocketAddress): Future[Unit] =
    import scalanative.unsafe.*
    import scalanative.unsigned.*
    import scalanative.posix.{netdb, netdbOps}, netdb.*, netdbOps.*
    import scalanative.posix.netinet.in
    import scalanative.posix.errno.*
    // different logic for ipv4 and ipv6
    def connect4(address: SocketAddress): Future[Unit] =
      val sa4 = stackalloc[in.sockaddr_in]()
      val sa4Len = sizeof[in.sockaddr_in].toUInt
      address.getAddress() match
        case addr: java.net.Inet4Address =>
          Helper.SockAddr.prepare4(addr, address.getPort(), sa4)
        case _ =>
          throw new ConnectException(
            s"Trying to connect to $address: not an IPv4 address"
          )
      setNonBlocking()

      val connectErr =
        if socket.connect(
            fd,
            sa4.asInstanceOf[Ptr[socket.sockaddr]],
            sa4Len
          ) == 0
        then 0
        else errno

      this.address = address.getAddress()
      this.port = address.getPort()
      handleConnectWait(connectErr, socket.AF_INET)
    end connect4

    def connect6(address: SocketAddress): Future[Unit] =
      val sa6 = stackalloc[in.sockaddr_in6]()
      val sa6Len = sizeof[in.sockaddr_in6].toUInt
      Helper.SockAddr.prepare6(address.getAddress(), address.getPort(), sa6)
      setNonBlocking()

      val connectErr =
        if socket.connect(
            fd,
            sa6.asInstanceOf[Ptr[socket.sockaddr]],
            sa6Len
          ) == 0
        then 0
        else errno
      handleConnectWait(connectErr, socket.AF_INET6)
    end connect6

    def handleConnectWait(connectErr: Int, family: Int) =
      var handle: EpollPoller.PollHandle = null
      def handleErr(
          errOrZero: Int,
          current: Int
      )(resolver: Future.Resolver[Unit]): Unit = errOrZero match
        case 0 =>
          resolver.complete:
            Try:
              this.localPort = fetchLocalPort(family).getOrElse:
                throw new ConnectException(
                  "Could not resolve a local port when connecting"
                )
        case err
            if err == EINPROGRESS | err == EAGAIN /* TODO: check `connect` again for Unix sockets */ =>
          if handle == null then
            poller.registerFd(fd, read = false, write = true)
            resolver.onCancel(handle.cancel)
          handle.write.onUpdate(
            current = current,
            listener = Listener:
              case (Success(next), _) =>
                // collect error
                val opt = stackalloc[CInt]()
                val optLen = stackalloc[socket.socklen_t]()
                !optLen = 1.toUInt
                if socket.getsockopt(
                    fd,
                    socket.SOL_SOCKET,
                    socket.SO_ERROR,
                    opt,
                    optLen
                  ) != 0
                then
                  resolver.reject(
                    ConnectException(
                      "Exception while getting socket option, errno: " + errno
                    )
                  )
                handleErr(!opt, next)(resolver)
              case (Failure(err), _) => resolver.reject(err)
          )
        case err =>
          resolver.reject(
            ConnectException("Connect failed with errno = " + err)
          )

      Future.withResolver(handleErr(connectErr, 0))

    if Helper.useIPv4Stack then connect4(address) else connect6(address)
  end connectFut

  private def fetchLocalPort(family: CInt): Option[Int] =
    import scalanative.unsafe.*
    import scalanative.posix.netinet.{in, inOps}, inOps.*
    import scalanative.posix.arpa.inet
    val len = stackalloc[socket.socklen_t]()
    val res = if family == socket.AF_INET then
      val sin = stackalloc[in.sockaddr_in]()
      !len = sizeof[in.sockaddr_in].toUInt
      if socket.getsockname(
          fd,
          sin.asInstanceOf[Ptr[socket.sockaddr]],
          len
        ) == -1
      then None
      else Some(sin.sin_port)
    else if family == socket.AF_INET6 then
      val sin = stackalloc[in.sockaddr_in6]()
      !len = sizeof[in.sockaddr_in6].toUInt
      if socket.getsockname(
          fd,
          sin.asInstanceOf[Ptr[socket.sockaddr]],
          len
        ) == -1
      then None
      else Some(sin.sin6_port)
    else None

    res.map(inet.ntohs(_).toInt)

  // low level socketFd opts

  import scala.scalanative.unsafe.CInt
  import scala.scalanative.posix.fcntl.*

  private def setNonBlocking(): Unit = updateSocketFdOpts(fd)(_ | O_NONBLOCK)

  private inline def getSocketFdOpts(fdFd: Int): CInt =
    withErrno("connect failed, fcntl F_GETFL, errno: " + _):
      fcntl(fdFd, F_GETFL, 0)

  private inline def setSocketFdOpts(fdFd: Int, opts: Int): Unit =
    withErrno(
      s"connect failed, fcntl F_SETFL for opts: $opts, errno: " + _
    ):
      fcntl(fdFd, F_SETFL, opts)

  private inline def updateSocketFdOpts(fdFd: Int)(mapping: CInt => CInt): Int =
    val oldOpts = getSocketFdOpts(fdFd)
    setSocketFdOpts(fdFd, mapping(oldOpts))
    oldOpts

end UnixSocket

private object Helper:
  // A Single Point of Truth to toggle IPv4/IPv6 underlying transport protocol.
  lazy val useIPv4Stack =
    // Java defaults to "false"
    val systemPropertyForcesIPv4 =
      java.lang.Boolean.parseBoolean(
        System.getProperty("java.net.preferIPv4Stack", "false")
      )

    // Do the expensive test last.
    systemPropertyForcesIPv4 || !isIPv6Configured

  // Sockaddr helpers
  object SockAddr:
    import java.net.Inet4Address
    import scala.scalanative.unsafe.*
    import scala.scalanative.unsigned.*
    import scala.scalanative.posix.netinet.{in, inOps}, in.*, inOps.*
    import scala.scalanative.posix.sys.socket, socket.*
    import scala.scalanative.posix.arpa.inet
    import scala.scalanative.posix.string.memcpy

    def prepare4(
        inetAddress: Inet4Address,
        port: Int,
        sa4: Ptr[in.sockaddr_in]
    ): Unit =
      sa4.sin_family = AF_INET.toUShort
      sa4.sin_port = inet.htons(port.toUShort)
      val src = inetAddress.getAddress()
      val from = src.asInstanceOf[scala.scalanative.runtime.Array[Byte]].at(0)
      val dst = sa4.sin_addr.at1.asInstanceOf[Ptr[Byte]]
      memcpy(dst, from, 4.toUInt)

    def prepare6(
        inetAddress: InetAddress,
        port: Int,
        sa6: Ptr[in.sockaddr_in6]
    ): Unit =
      // from scalanative's prepareSockaddrIn6
      sa6.sin6_family = socket.AF_INET6.toUShort
      sa6.sin6_port = inet.htons(port.toUShort)
      val src = inetAddress.getAddress()
      inetAddress match
        case addr: Inet6Address =>
          val from =
            src.asInstanceOf[scala.scalanative.runtime.Array[Byte]].at(0)
          val dst = sa6.sin6_addr.at1.at(0).asInstanceOf[Ptr[Byte]]
          memcpy(dst, from, 16.toUInt)

          sa6.sin6_scope_id = addr
            .getScopeId()
            .toUShort
        case _ => // Use IPv4mappedIPv6 address
          // IPv4 addresses do not have a scope_id, so leave at current value 0

          val dst = sa6.sin6_addr.toPtr.s6_addr

          // By contract, the leading bytes are already zero already.
          val FF = 255.toUByte
          dst(10) = FF // set the IPv4mappedIPv6 indicator bytes
          dst(11) = FF

          // add the IPv4 trailing bytes, unrolling small loop
          dst(12) = src(0).toUByte
          dst(13) = src(1).toUByte
          dst(14) = src(2).toUByte
          dst(15) = src(3).toUByte
  end SockAddr

  private lazy val isIPv6Configured =
    if LinktimeInfo.isWindows then false
    else
      import scalanative.posix.sys.{socket, socketOps}, socket.*, socketOps.*
      import scala.scalanative.posix.netdb.*
      import scala.scalanative.posix.netdbOps.*
      import scala.scalanative.posix.netinet.in
      import scala.scalanative.unsafe.*
      /* The lookup can not be a local address. This one of two IPv6
       * addresses for the famous, in the IPv6 world, www.kame.net
       * IPv6 dancing kame (turtle). The url from Ipv6 for fun some time
       */
      val kameIPv6Addr = c"2001:2F0:0:8800:0:0:1:1"

      val hints = stackalloc[addrinfo]() // stackalloc clears its memory
      val ret = stackalloc[Ptr[addrinfo]]()

      hints.ai_family = AF_INET6
      hints.ai_flags = AI_NUMERICHOST | AI_ADDRCONFIG | AI_PASSIVE
      hints.ai_socktype = SOCK_STREAM
      hints.ai_protocol = in.IPPROTO_TCP

      val gaiStatus = getaddrinfo(kameIPv6Addr, null, hints, ret)
      val result =
        if gaiStatus != 0 then false
        else
          try
            val ai = !ret
            if ai == null || ai.ai_addr == null then false
            else ai.ai_addr.sa_family == AF_INET6
          finally freeaddrinfo(!ret)
      result
end Helper
