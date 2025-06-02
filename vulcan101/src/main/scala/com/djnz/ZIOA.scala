package com.djnz

import zio.Scope
import zio.ZIO
import zio.ZIOAppArgs
import zio.ZIOAppDefault

object ZIOA extends ZIOAppDefault {

  import domain._

  val p = Person("abc", 17)

  override def run: ZIO[Any with ZIOAppArgs with Scope, Any, Any] =
    for {
      _     <- ZIO.succeedBlocking(pprint.log(p))
      bytes <- ser.serialize("topic1", hh, p)
      bytes0=bytes.drop(1)
      _     <- ZIO.succeedBlocking(pprint.log(bytes))
      _     <- ZIO.succeedBlocking(pprint.log(bytes0))
      p2    <- des.deserialize("topic1", hh, bytes)
      _     <- ZIO.succeedBlocking(pprint.log(p2))
    } yield ()

}
