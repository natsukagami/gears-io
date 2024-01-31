package gears.util

import scala.util.boundary

object either:
  type Label[-E, -T] = boundary.Label[Either[E, T]]

  inline def error[E](e: E)(using Label[E, Nothing]) = boundary.break(Left(e))

  /** Starts a body that returns a `T` wrapped in an [[Either]]. Within this
    * body, `.?` is available on [[Either]] values, short-circuiting back to
    * this function's caller if a [[Left]] is seen.
    */
  inline def apply[E, T](inline body: Label[E, T] ?=> T): Either[E, T] =
    boundary:
      Right(body)

  extension [E, T](e: Either[E, T])
    /** Unwraps an [[Either]], short-circuiting to the matching [[either]] call
      * if the value is a [[Left]].
      */
    def ?(using Label[E, Nothing]): T = e match
      case Left(ex)     => boundary.break(Left(ex))
      case Right(value) => value
