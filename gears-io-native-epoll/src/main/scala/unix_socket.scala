/* A lot of code in this package is taken from scala-native. */
package gears.async.asyncio.epoll

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
import scala.scalanative.windows.WinSocketApi
import java.io.Closeable

sealed class UnixSocket protected (
    protected var fd: Int,
    protected var address: InetAddress = null,
    protected var port: Int = 0,
    protected var _localAddress: InetAddress = null,
    protected var localPort: Int = 0
)(using poller: EpollPoller)
    extends Closeable:
  private var _handle: EpollPoller.PollHandle = _

  def close() = synchronized:
    if _handle != null then
      _handle.cancel()
      _handle = null
    scalanative.posix.unistd.close(fd)

  // Address interfaces

  def getRemoteSocketAddress =
    if address == null then null
    else java.net.InetSocketAddress(address, port)

  def getLocalSocketAddress =
    if _localAddress == null then null
    else java.net.InetSocketAddress(_localAddress, localPort)

  // Handle

  protected inline def handle = synchronized:
    if _handle == null then
      _handle = poller.registerFd(fd, read = true, write = false)
    _handle

  protected inline def isMustWait(errno: Int) =
    import scalanative.posix.errno.{EAGAIN, EWOULDBLOCK}
    errno == EAGAIN || errno == EWOULDBLOCK

  // Read and Write

  def read(buffer: Array[Byte], offset: Int, count: Int)(using Async): Int =
    import scalanative.unsafe.*
    import scalanative.unsigned.*
    import scalanative.posix.errno.*

    @scala.annotation.tailrec
    def loop(current: Int): Int =
      val bytesRead = socket.recv(fd, buffer.at(offset), count.toUInt, 0).toInt
      if bytesRead > 0 then bytesRead
      else if bytesRead == 0 then (if count == 0 then 0 else -1)
      else
        errno match
          case e if isMustWait(e) => loop(handle.read.onUpdate(current))
          case e => throw new SocketException(s"read failed with errno $e")

    loop(0)
  end read

  def write(buffer: Array[Byte], offset: Int, count: Int)(using Async): Int =
    import scalanative.unsafe.*
    import scalanative.unsigned.*
    import scalanative.posix.errno.*

    val cArr = buffer.at(offset)
    var current = 0
    var sent = 0
    while sent < count do
      val ret = socket
        .send(fd, cArr + sent, (count - sent).toUInt, socket.MSG_NOSIGNAL)
        .toInt
      if (ret < 0) then
        errno match
          case e if isMustWait(e) => current = handle.write.onUpdate(current)
          case e => throw new SocketException(s"write failed with errno $e")
      else sent += ret
    sent
  end write

  def readFut(buffer: Array[Byte], offset: Int, count: Int): Future[Int] =
    import scalanative.unsafe.*
    import scalanative.unsigned.*
    import scalanative.posix.errno.*
    inline def readNow =
      socket.recv(fd, buffer.at(offset), count.toUInt, 0).toInt

    Future.withResolver: resolver =>
      def loop(current: Int, bytes: Int): Unit =
        bytes match
          case _ if bytes > 0 => resolver.resolve(bytes)
          case 0              => resolver.resolve(if count == 0 then 0 else -1)
          case _ => // < 0
            errno match
              case e if isMustWait(e) =>
                val listener =
                  Listener[Try[Int]]:
                    case (Success(next), _) => loop(next, readNow)
                    case (Failure(exc), _)  => resolver.reject(exc)
                resolver.onCancel(() => handle.read.dropListener(listener))
                handle.read.onUpdate(listener, current)
              case e =>
                resolver.reject(
                  new SocketException(s"read failed with errno $e")
                )
      loop(0, 0)
  end readFut

  def writeFut(buffer: Array[Byte], offset: Int, count: Int): Future[Int] =
    import scalanative.unsafe.*
    import scalanative.unsigned.*
    import scalanative.posix.errno.*
    val cArr = buffer.at(offset)

    Future.withResolver: resolver =>
      def loop(sent: Int, current: Int): Unit =
        if sent == count then resolver.resolve(0)
        val bytesWritten =
          socket.send(fd, cArr + sent, (count - sent).toUInt, 0).toInt
        if bytesWritten >= 0 then loop(sent + bytesWritten, current)
        else
          errno match
            case e if isMustWait(e) =>
              val listener = Listener[Try[Int]]:
                case (Success(next), _) => loop(sent, next)
                case (Failure(exc), _)  => resolver.reject(exc)
              resolver.onCancel(() => handle.write.dropListener(listener))
              handle.write.onUpdate(listener, current)
            case e =>
              resolver.reject(
                new SocketException(s"write failed with errno $e")
              )
      end loop
      loop(0, 0)
  end writeFut

  // low level socketFd opts

  import scala.scalanative.unsafe.CInt
  import scala.scalanative.posix.fcntl.*

  protected def fetchLocalPort(family: CInt): Option[Int] =
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

  protected inline def withErrno(
      inline msg: Int => String = err => s"Socket failed with errno = $err"
  )(inline op: => Int) =
    val res = op
    if res >= 0 then res
    else
      val err = scalanative.posix.errno.errno
      throw new IOException(msg(err))

  protected def setNonBlocking(): Unit = updateSocketFdOpts(fd)(_ | O_NONBLOCK)

  private inline def getSocketFdOpts(fdFd: Int): CInt =
    withErrno("connect failed, fcntl F_GETFL, errno: " + _):
      fcntl(fdFd, F_GETFL, 0)

  private inline def setSocketFdOpts(fdFd: Int, opts: Int): Unit =
    withErrno(
      s"connect failed, fcntl F_SETFL for opts: $opts, errno: " + _
    ):
      fcntl(fdFd, F_SETFL, opts)

  protected inline def updateSocketFdOpts(fdFd: Int)(
      mapping: CInt => CInt
  ): Int =
    val oldOpts = getSocketFdOpts(fdFd)
    setSocketFdOpts(fdFd, mapping(oldOpts))
    oldOpts
end UnixSocket

private[epoll] class UnixServerSocket(isStreaming: Boolean)(using
    poller: EpollPoller
) extends UnixSocket(fd = Helper.createFd(isStreaming)):
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
          Helper.SockAddr.toSockAddr4(addr, port, sa4)
        case _ => throwCannotBind(addr, s"$addr is not an IPv4 address")

      val bindRes = socket.bind(
        fd,
        sa4.asInstanceOf[Ptr[socket.sockaddr]],
        sa4Len
      )

      if bindRes < 0 then throwCannotBind(addr)

      this._localAddress = addr
      this.localPort = fetchLocalPort(socket.AF_INET).getOrElse:
        throwCannotBind(addr)
    end bind4

    def bind6(addr: InetAddress, port: Int) =
      val sa6 = stackalloc[in.sockaddr_in6]()
      val sa6Len = sizeof[in.sockaddr_in6].toUInt

      // By contract, all the bytes in sa6 are zero going in.
      Helper.SockAddr.toSockAddr6(addr, port, sa6)

      val bindRes = socket.bind(
        fd,
        sa6.asInstanceOf[Ptr[socket.sockaddr]],
        sa6Len
      )

      if bindRes < 0 then throwCannotBind(addr)

      this._localAddress = addr
      this.localPort = fetchLocalPort(sa6.sin6_family.toInt).getOrElse:
        throwCannotBind(addr)
    end bind6

    if Helper.useIPv4Stack then bind4(addr, port) else bind6(addr, port)
  end bind

  def listen(backlog: Int): Unit =
    withErrno()(socket.listen(fd, backlog))

  def accept()(using Async): UnixSocket =
    import scalanative.unsafe.*
    import scalanative.posix.errno.*
    import scalanative.posix.netinet.in
    @scala.annotation.tailrec
    def loop(current: Int): UnixSocket =
      Zone.acquire: zone =>
        given zone.type = zone
        val storage = alloc[socket.sockaddr_storage]()
        val address = storage.asInstanceOf[Ptr[socket.sockaddr]]
        val addressLen = alloc[socket.socklen_t]()
        !addressLen = sizeof[in.sockaddr_in6].toUInt

        val newFd = socket.accept(fd, address, addressLen)
        if newFd == -1 then Left(errno)
        else
          val insAddr = Helper.SockAddr.fromSockAddr(address)
          Right(
            new UnixSocket(
              fd = newFd,
              address = insAddr.getAddress(),
              port = insAddr.getPort(),
              localPort = this.localPort
            )
          )
      match
        case Right(value)                 => value
        case Left(err) if isMustWait(err) => loop(handle.read.onUpdate(current))
        case Left(err) =>
          throw SocketException(s"Accept failed with errno = $err")
    loop(0)

  def acceptFut(): Future[UnixSocket] =
    import scalanative.unsafe.*
    import scalanative.posix.errno.*
    import scalanative.posix.netinet.in
    Future.withResolver: resolver =>
      def loop(current: Int): Unit =
        val result = Zone.acquire: zone =>
          given zone.type = zone
          val storage = alloc[socket.sockaddr_storage]()
          val address = storage.asInstanceOf[Ptr[socket.sockaddr]]
          val addressLen = alloc[socket.socklen_t]()
          !addressLen = sizeof[in.sockaddr_in6].toUInt

          val newFd = socket.accept(fd, address, addressLen)
          if newFd == -1 then Left(errno)
          else
            val insAddr = Helper.SockAddr.fromSockAddr(address)
            Right(
              new UnixSocket(
                fd = newFd,
                address = insAddr.getAddress(),
                port = insAddr.getPort(),
                localPort = this.localPort
              )
            )
        result match
          case Right(value) => resolver.resolve(value)
          case Left(err) if isMustWait(err) =>
            val listener = Listener[Try[Int]]:
              case (Success(next), _) => loop(next)
              case (Failure(exc), _)  => resolver.reject(exc)
            resolver.onCancel(() => handle.read.dropListener(listener))
            handle.read.onUpdate(listener, current)
          case Left(err) =>
            resolver.reject(SocketException(s"Accept failed with errno = $err"))
      loop(0)
end UnixServerSocket

private[epoll] class UnixClientSocket(isStreaming: Boolean)(using
    poller: EpollPoller
) extends UnixSocket(fd = Helper.createFd(isStreaming)):
  // Connection set up
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
          Helper.SockAddr.toSockAddr4(addr, address.getPort(), sa4)
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
      Helper.SockAddr.toSockAddr6(address.getAddress(), address.getPort(), sa6)
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
end UnixClientSocket

private object Helper:
  def createFd(isStreaming: Boolean) =
    val af = if useIPv4Stack then socket.AF_INET else socket.AF_INET6
    val sockType = if isStreaming then socket.SOCK_STREAM else socket.SOCK_DGRAM
    val sock = socket.socket(af, sockType, 0)
    if sock < 0 then
      throw new IOException(
        s"Could not create a socket in address family: ${af}" +
          s" streaming: ${isStreaming}"
      )
    sock

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
    import scalanative.posix.sys.socketOps.*
    import scala.scalanative.posix.arpa.inet
    import scala.scalanative.posix.string.memcpy

    def toSockAddr4(
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

    def toSockAddr6(
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
    end toSockAddr6

    def fromSockAddr(sockAddr: Ptr[socket.sockaddr]): SocketAddress =
      val addr: InetAddress = sockaddrToInetAddres(sockAddr, "")
      val port: Int = sockaddrToPort(sockAddr)
      new SocketAddress(addr, port)

    private def sockaddrToInetAddres(
        sin: Ptr[socket.sockaddr],
        host: String
    ): InetAddress =
      if sin.sa_family == AF_INET then
        InetAddress.getByAddress(host, sockaddrToByteArray(sin))
      else
        val sin6 = sin.asInstanceOf[Ptr[sockaddr_in6]]
        val addrBytes = sin6.sin6_addr.at1.at(0).asInstanceOf[Ptr[Byte]]
        // Scala JVM down-converts even when preferIPv6Addresses is "true"
        if isIPv4MappedAddress(addrBytes) then
          InetAddress.getByAddress(host, extractIP4Bytes(addrBytes))
        else
          /* Yes, Java specifies Int for scope_id in a way which disallows
           * some values POSIX/IEEE/IETF allows.
           */

          val scope_id = sin6.sin6_scope_id.toInt

          /* Be aware some trickiness here.
           * Java treats a 0 scope_id (qua NetworkInterface index)
           * as having been not supplied.
           * Exactly the same 0 scope_id explicitly passed to
           * Inet6Address.getByAddress() is considered supplied and
           * displayed as such.
           */

          // Keep address bytes passed in immutable, get new Array.
          val clonedBytes = sockaddrToByteArray(sin)
          if scope_id == 0 then InetAddress.getByAddress(host, clonedBytes)
          else Inet6Address.getByAddress(host, clonedBytes, scope_id)
    end sockaddrToInetAddres

    private def sockaddrToByteArray(
        sockAddr: Ptr[sockaddr]
    ): Array[Byte] =
      val af = sockAddr.sa_family.toInt
      val (src, size) = if af == AF_INET6 then
        val v6addr = sockAddr.asInstanceOf[Ptr[in.sockaddr_in6]]
        val sin6Addr = v6addr.sin6_addr.at1.asInstanceOf[Ptr[Byte]]
        // Scala JVM down-converts even when preferIPv6Addresses is "true"
        if isIPv4MappedAddress(sin6Addr) then (sin6Addr + 12, 4)
        else (sin6Addr, 16)
      else if af == AF_INET then
        val v4addr = sockAddr.asInstanceOf[Ptr[in.sockaddr_in]]
        val sin4Addr = v4addr.sin_addr.at1.asInstanceOf[Ptr[Byte]]
        (sin4Addr, 4)
      else throw new SocketException(s"Unsupported address family: ${af}")

      val byteArray = new Array[Byte](size)
      memcpy(byteArray.at(0), src, size.toUInt)

      byteArray
    end sockaddrToByteArray

    private def sockaddrToPort(sockAddr: Ptr[sockaddr]): Int =
      val af = sockAddr.sa_family.toInt
      val inPort =
        if af == AF_INET6 then
          sockAddr.asInstanceOf[Ptr[in.sockaddr_in6]].sin6_port
        else if af == AF_INET then
          sockAddr.asInstanceOf[Ptr[in.sockaddr_in]].sin_port
        else throw SocketException(s"Unsupported address family: ${af}")
      inet.ntohs(inPort).toInt

    private def isIPv4MappedAddress(pb: Ptr[Byte]): Boolean =
      val ptrInt = pb.asInstanceOf[Ptr[Int]]
      val ptrLong = pb.asInstanceOf[Ptr[Long]]
      (ptrInt(2) == 0xffff0000) && (ptrLong(0) == 0x0L)

    private def extractIP4Bytes(pb: Ptr[Byte]): Array[Byte] =
      val buf = new Array[Byte](4)
      buf(0) = pb(12)
      buf(1) = pb(13)
      buf(2) = pb(14)
      buf(3) = pb(15)
      buf
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
