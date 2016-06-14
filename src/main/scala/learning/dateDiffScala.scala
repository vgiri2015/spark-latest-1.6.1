package learning

import java.time.LocalDate
import java.time.format.DateTimeFormatter

/**
 * Created by gfp2ram on 10/23/2015.
 */
object dateDiffScala {
  def main(args: Array[String]) {
    val startDate = "1970-01-01"
    val formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd")
    val oldDate = LocalDate.parse(startDate, formatter)
    val currentDate = "2015-02-25"
    val newDate = LocalDate.parse(currentDate, formatter)
    println(newDate.toEpochDay() - oldDate.toEpochDay())
  }
}
