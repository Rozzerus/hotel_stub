package kafka

import java.util.Date

class Rate(
  var dateTime: Date,//date_time
  var rate: Double,//orig_destination_distance
){

  override def toString = s"Rate($dateTime,  $rate)"
}
