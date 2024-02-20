package sandbox.async

import gears.async
import gears.async.jvm.net
import gears.async.net.TcpSupport

given gears.async.VThreadScheduler.type = gears.async.default.given_Scheduler
given gears.async.VThreadSupport.type =
  gears.async.default.given_VThreadSupport_type
given net.JvmTcpSupport.type = net.JvmTcpSupport
