package sandbox.async

import gears.async
import gears.async.asyncio.epoll

val forkJoinEpollSupport = new epoll.ForkJoinEpollSupport()
given epoll.ForkJoinEpollSupport = forkJoinEpollSupport
given epoll.EpollTcpSupport = forkJoinEpollSupport.tcpSupport
