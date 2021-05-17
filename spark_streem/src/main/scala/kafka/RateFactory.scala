package kafka

import java.util.Date

import scala.util.Random

object RateFactory {

  def createMessage(): Rate = {
    val r: scala.util.Random = new Random
    new Rate(
      new Date(),
      r.nextDouble()
    )
  }


}
