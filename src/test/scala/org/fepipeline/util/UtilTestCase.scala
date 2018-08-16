package org.fepipeline.util

import org.joda.time.format.DateTimeFormat
import org.joda.time.{DateTime, DateTimeZone}
import org.scalatest.FunSuite

/**
  * ClassDescription.
  *
  * @author Zhang Chaoli
  */
class UtilTestCase extends FunSuite {
  test("DatePathUtil") {
    println(DatePathUtil.getDateCertainDayAgo(1))

    println(DatePathUtil.getDateCertainDayAgo("20180725", 1))
    println(DatePathUtil.getDateCertainDayAgo("20180725", 3))

  }

  test("getDateIntervalDays") {

//    val dateTime = DateTimeFormat.forPattern("yyyyMMdd").parseDateTime("20180725").toLocalDateTime
    println(DatePathUtil.getDateIntervalDays("20180725", 3).mkString(", "))
    println(DatePathUtil.getDateIntervalDays("20180725", "20180802").mkString(", "))
    println(DatePathUtil.getDateIntervalDaysAgo("20180725", 3).mkString(", "))

//    val dataTime = DateTime.parse("20180725")
//    val dateStr = DatePathUtil.getDateString(dateTime.plusDays(2).toDate)/*.take(8)*/
//    println(DatePathUtil.getDateString(dateTime.plusDays(1).toDate)/*.take(8)*/)
//    println(DatePathUtil.getDateString(dateTime.plusDays(2).toDate)/*.take(8)*/)
//    println(DatePathUtil.getDateString(dateTime.plusDays(3).toDate)/*.take(8)*/)
//    println(DatePathUtil.getDateString(dateTime.plusDays(4).toDate)/*.take(8)*/)
//
//    println("----------")
//    println(DatePathUtil.getDateString(dateTime.minusDays(1).toDate).take(8))
//    println(DatePathUtil.getDateString(dateTime.minusDays(2).toDate).take(8))
//    println(DatePathUtil.getDateString(dateTime.minusDays(3).toDate).take(8))
//    println(DatePathUtil.getDateString(dateTime.minusDays(60).toDate).take(8))
  }

}
