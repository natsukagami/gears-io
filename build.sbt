import sbtcrossproject.CrossPlugin.autoImport.{crossProject, CrossType}
import scalanative.build._

ThisBuild / scalaVersion := "3.3.1"

lazy val root =
  crossProject(JVMPlatform, NativePlatform)
    .crossType(CrossType.Full)
    .in(file("."))
    .settings(
      Seq(
        name := "Gears Socket IO",
        organization := "ch.epfl.lamp",
        version := "0.1.0-SNAPSHOT",
        libraryDependencies += "ch.epfl.lamp" %%% "gears" % "0.1.0-SNAPSHOT",
        testFrameworks += new TestFramework("munit.Framework")
      )
    )
    .jvmSettings(
      Seq(
        javaOptions += "--version 21",
        libraryDependencies += "org.scalameta" %% "munit" % "1.0.0-M10" % Test
      )
    )
    .nativeSettings(
      Seq(
        nativeConfig ~= { c =>
          c.withMultithreadingSupport(true)
        },
        libraryDependencies += "org.scalameta" %%% "munit" % "1.0.0-M10+15-3940023e-SNAPSHOT" % Test
      )
    )

lazy val jvm =
  project
    .in(file("./gears-io-jvm"))
    .dependsOn(root.jvm)
    .settings(
      Seq(
        name := "Gears IO JVM",
        organization := "ch.epfl.lamp",
        version := "0.1.0-SNAPSHOT"
      )
    )

lazy val nativeEpoll = project
  .in(file("./gears-io-native-epoll"))
  .enablePlugins(ScalaNativePlugin)
  .dependsOn(root.native)
  .settings(
    Seq(
      nativeConfig ~= { c =>
        c.withMultithreadingSupport(true)
          .withLTO(LTO.none)
          .withMode(Mode.debug)
          .withGC(GC.immix)
      },
      name := "Gears IO Native epoll",
      organization := "ch.epfl.lamp",
      version := "0.1.0-SNAPSHOT",
      libraryDependencies ++= Seq(
        "ch.epfl.lamp" %%% "gears" % "0.1.0-SNAPSHOT",
        "org.scala-native" % "javalib-intf" % "0.5.0-SNAPSHOT"
      )
    )
  )

lazy val sandbox =
  crossProject(JVMPlatform, NativePlatform)
    .crossType(CrossType.Full)
    .in(file("./sandbox"))
    .dependsOn(root)
    .configurePlatform(JVMPlatform)(_.dependsOn(jvm))
    .configurePlatform(NativePlatform)(_.dependsOn(nativeEpoll))
    .settings(
      Seq(name := "gears IO sandbox")
    )
    .nativeSettings(
      Seq(
        nativeConfig ~= { c =>
          c.withMultithreadingSupport(true)
            .withLTO(LTO.none)
            .withMode(Mode.debug)
            .withGC(GC.immix)
        }
      )
    )
