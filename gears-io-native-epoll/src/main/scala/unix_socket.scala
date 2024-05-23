/* A lot of code in this package is taken from scala-native. */
package gears.io.epoll

import scala.scalanative.meta.LinktimeInfo
import scalanative.posix.sys.socket
import java.io.IOException

class UnixSocket(isStreaming: Boolean):
  /** The socket to be used */
  val sock =
    val af = if Helper.useIPv4Stack then socket.AF_INET else socket.AF_INET6
    val sockType = if isStreaming then socket.SOCK_STREAM else socket.SOCK_DGRAM
    val sock = socket.socket(af, sockType, 0)
    if sock < 0 then
      throw new IOException(
        s"Could not create a socket in address family: ${af}" +
          s" streaming: ${isStreaming}"
      )
    sock

  def listen(backlog: Int): Unit =
    withErrno(socket.listen(sock, backlog))

  private inline def withErrno(inline op: => Int) =
    val res = op
    if res >= 0 then res
    else
      val err = scalanative.posix.errno.errno
      throw new IOException(s"Socket failed with errno = $err")

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
        if gaiStatus != 0 then
          false
        else 
          try 
            val ai = !ret
            if ai == null || ai.ai_addr == null then
              false
            else 
              ai.ai_addr.sa_family == AF_INET6
          finally 
            freeaddrinfo(!ret)
      result
end Helper
