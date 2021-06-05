package kafka

class Rate(
            var month: Int,
            var rate: Double,
){

  override def toString = s"Rate($month,  $rate)"
}
