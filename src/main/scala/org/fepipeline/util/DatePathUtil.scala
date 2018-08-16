package org.fepipeline.util

import java.io.File
import java.text.{DateFormat, SimpleDateFormat}
import java.util.{Calendar, Date}

import org.apache.spark.SparkContext
import org.fepipeline.common.Logging
import org.joda.time.format.DateTimeFormat
import org.slf4j.LoggerFactory

import scala.collection.mutable
import scala.util.control.Breaks._

/**
  * DatePathUtil.
  *
  * @author Zhang Chaoli
  */
object DatePathUtil extends Logging {

  /**
    * 从昨天开始前n（interval）天
    *
    * @param basePath
    * @param interval
    * @return
    */
  def getPathByIntervalDate(basePath: String, interval: Int): String = {
    getPathByDate(basePath, 1, interval)
  }

  /**
    * 从昨天,开始前n（interval）天（昨天为1）的hfds路径
    * 注意: 若某个hfds路径不存在,则不加入。该方式可用于多个hfds输入路径的生成
    *
    * @param basePath
    * @param suffix 路径后缀
    * @param interval
    * @return
    */
  def getPathByIntervalDateIgnore(sc: SparkContext, basePath: String, suffix: String, interval: Int): String = {
    getPathByDateIgnore(sc, basePath, suffix, 1, interval)
  }

  /**
    *
    * @param basePath
    * @param frontDayNum 前第N天,当为0时,表示为当天
    * @return
    */
  def getPathByDate(basePath: String, frontDayNum: Int): String = {
    getPathByDate(basePath, frontDayNum, frontDayNum)
  }

  /**
    * 注意: 自动检查路径路径是否存在,不存在则自动忽略该路径
    *
    * @param basePath
    * @param frontDayNum 前第N天,当为0时,表示为当天
    * @return
    */
  def getPathByDateIgnore(sc: SparkContext, basePath: String, frontDayNum: Int): String = {
    getPathByDateIgnore(sc, basePath, frontDayNum, frontDayNum)
  }

  /**
    * 生成特定前几天的hdfs路径, 输入格式: /home/clzhang/:1,/home/clzhang/:3,/home/clzhang/:4
    * 注意: 不自动忽略路径
    *
    * @param path
    * @return
    */
  def getPathByDate(path: String): String = {
    val pathSb = new StringBuilder
    try {
      for (pathIdx <- path.split(",", -1)) {
        val basePath = pathIdx.split(":")(0)
        val frontDayNum = pathIdx.split(":")(1).toInt
        pathSb.append(DatePathUtil.getPathByDate(basePath, frontDayNum)).append(",")
      }
    } catch {
      case t: Throwable => log.warn("getHdfsPathByDate Exception: ", t)
    }

    pathSb.toString.dropRight(1)
  }

  /**
    * 生成特定几天的hdfs路径, 输入格式: /home/clzhang/:1,/home/clzhang/:3,/home/clzhang/:4
    * 注意: 自动检查路径路径是否存在,不存在则自动忽略该路径
    *
    * @param sc
    * @param path
    * @return
    */
  def getPathByDateIgnore(sc: SparkContext, path: String): String = {
    val pathSb = new StringBuilder
    try {
      for (pathIdx <- path.split(",", -1)) {
        val basePath = pathIdx.split(":")(0)
        val frontDayNum = pathIdx.split(":")(1).toInt
        pathSb.append(DatePathUtil.getPathByDateIgnore(sc, basePath, frontDayNum)).append(",")
      }
    } catch {
      case t: Throwable => log.warn("getHdfsPathByDate Exception: ", t)
    }

    val pathStr = pathSb.toString.dropRight(1)
    if (pathStr.length < 1) {
      log.error("All input paths NOT exist Exception: " + path)
      System.exit(0)
      null
    } else pathStr
  }

  /**
    * 返回区间内的日期（包括边界）对应的路径
    *
    * @param basePath
    * @param startDate
    * @param endDate
    * @return
    */
  def getPathByDate(basePath: String, startDate: Int, endDate: Int): String = {
    val basePathNew = if (basePath.endsWith("/")) basePath else basePath /* + "/"*/
    var dateStr = ""
    Range(startDate, endDate + 1).foreach { elem =>
      val path = basePathNew + getDateString(certainDayAgo(elem))
      dateStr += (path + ",")
    }
    dateStr.dropRight(1)
  }

  /**
    * 返回区间内的日期（包括边界）对应的路径
    * 注意: 自动检查路径路径是否存在,不存在则自动忽略该路径。若直接使用该方法,则有返回“”路径的风险（Spark会报错）
    *
    * @param basePath
    * @param startDate
    * @param endDate
    * @return
    */
  def getPathByDateIgnore(sc: SparkContext, basePath: String, startDate: Int, endDate: Int): String = {
    val basePathNew = if (basePath.endsWith("/")) basePath else basePath /* + "/"*/
    var dateStr = ""
    Range(startDate, endDate + 1).foreach { elem =>
      val path = basePathNew + getDateString(certainDayAgo(elem))
      if (HDFSUtil.exist(sc, path)) {
        dateStr += (path + ",")
      }
    }
    dateStr.dropRight(1)
  }

  /**
    * 返回区间内日期（包括边界）对应的hfds路径
    * 注意: 若某个hfds路径不存在,则不加入。该方式可用于多个hfds输入路径的生成
    *
    * @param basePath
    * @param suffix 路径后缀
    * @param startDate
    * @param endDate
    * @return
    */
  def getPathByDateIgnore(sc: SparkContext, basePath: String, suffix: String, startDate: Int, endDate: Int): String = {
    val basePathNew = if (basePath.endsWith("/")) basePath else basePath /* + "/"*/
    var dateStr = ""
    Range(startDate, endDate + 1).foreach { elem =>
      val path = basePathNew + getDateString(certainDayAgo(elem)) + suffix
      if (HDFSUtil.exist(sc, path)) {
        dateStr += (path + ",")
      }
    }
    dateStr.dropRight(1)
  }

  /**
    * 返回 frontDayNum 天前日期的hdfs输出路径
    * 注意: 若hdfs输出路径已存在,则会覆盖写入
    *
    * @param sc
    * @param basePath
    * @param frontDayNum
    * @return
    */
  def getOutpathByDate(sc: SparkContext, basePath: String, frontDayNum: Int): String = {
    val basePathNew = if (basePath.endsWith("/")) basePath else basePath /* + "/"*/
    val outPath = basePathNew + getDateString(certainDayAgo(frontDayNum))
    HDFSUtil.del(sc, outPath)
    outPath
  }

  /**
    * 返回特定第N天的hdfs输出路径, 输入格式: /home/clzhang/:1
    * 注意: 若hdfs输出路径已存在,则会覆盖写入
    *
    * @param sc
    * @param outpath
    * @return
    */
  def getOutpathByDate(sc: SparkContext, outpath: String): String = {
    try {
      val basePath = outpath.split(":")(0)
      val frontDayNum = outpath.split(":")(1).toInt
      getOutpathByDate(sc, basePath, frontDayNum)
    } catch {
      case t: Throwable => log.warn("getHdfsOutpathByDate Exception: ", t); System.exit(0); null
    }
  }

  /**
    * 返回 frontDayNum 天前日期的输出路径
    *
    * @param basePath
    * @param frontDayNum
    * @return
    */
  def getOutpathByDate(basePath: String, frontDayNum: Int): String = {
    val basePathNew = if (basePath.endsWith("/")) basePath else basePath /* + "/"*/
    val outPath = basePathNew + getDateCertainDayAgo(frontDayNum)
    outPath
  }

  /**
    * 返回特定第N天的输出路径, 输入格式: /home/clzhang/:1
    *
    * @param outpath
    * @return
    */
  def getOutpathByDate(outpath: String): String = {
    try {
      val basePath = outpath.split(":")(0)
      val frontDayNum = outpath.split(":")(1).toInt
      getOutpathByDate(basePath, frontDayNum)
    } catch {
      case t: Throwable => log.warn("getHdfsOutpathByDate Exception: ", t); System.exit(0); null
    }
  }

  /**
    *
    * @param num 前 num 天的日期, 比如num=1,表示昨天; num=2,表示前天
    * @return
    */
  def getDateCertainDayAgo(num: Int): String = {
    getDateString(certainDayAgo(num))
  }

  def getDateCertainDayAgo(endDate: String, num: Int): String = {
    val localDateTime = DateTimeFormat.forPattern("yyyyMMdd").parseDateTime(endDate).toLocalDateTime
    getDateString(localDateTime.minusDays(num).toDate)
  }

  def getDateIntervalDaysAgo(endDate: String, num: Int): Array[String] = {
    val localDateTime = DateTimeFormat.forPattern("yyyyMMdd").parseDateTime(endDate).toLocalDateTime
    val dates = mutable.ArrayBuffer[String]()
    (0 to num).foreach { idx =>
      dates += getDateString(localDateTime.minusDays(idx).toDate)
    }
    dates.toArray
  }

  def getDateIntervalDays(startDate: String, num: Int): Array[String] = {
    val localDateTime = DateTimeFormat.forPattern("yyyyMMdd").parseDateTime(startDate).toLocalDateTime
    val dates = mutable.ArrayBuffer[String]()
    (0 to num).foreach { idx =>
      dates += getDateString(localDateTime.plusDays(idx).toDate)
    }
    dates.toArray
  }

  def getDateIntervalDays(startDate: String, endDate: String): Array[String] = {
    val startLocalDateTime = DateTimeFormat.forPattern("yyyyMMdd").parseDateTime(startDate).toLocalDateTime
    val dates = mutable.ArrayBuffer[String]()
    breakable {
      var currDate = ""
      for (idx <- 0 to Int.MaxValue) {
        currDate = getDateString(startLocalDateTime.plusDays(idx).toDate)
        dates += currDate
        if (currDate.equals(endDate)) break
      }
    }
    dates.toArray
  }

  def getDateString(date: Date): String = {
    val dateFormat: DateFormat = new SimpleDateFormat("yyyyMMdd")
    dateFormat.format(date)
  }

  private def certainDayAgo(num: Int): Date = {
    var cal: Calendar = Calendar.getInstance()
    cal.add(Calendar.DATE, -num)
    cal.getTime
  }

  /**
    *
    * @param path if path has suffix after date, then set ":" between path and suffix
    * @param date
    * @return
    */
  def concatPathWithDate(path: String, date: String): String = {
    if (path.indexOf(":") > -1) path.replace(":", date)
    else if (path.endsWith(File.separator) || path.endsWith("dt=")) path + date
    else path + File.separator + date
  }

  def main(args: Array[String]): Unit = {
    //    println(getPathByDate("/home/clzhang/", 2)) // /home/clzhang/20170816
    //    println(getPathByIntervalDate("/home/clzhang/", 2)) // /home/clzhang/20170817,/home/clzhang/20170816
    //    println(getPathByDate("/home/clzhang/", 0, 2)) // /home/clzhang/20170818,/home/clzhang/20170817,/home/clzhang/20170816
    //    println(getPathByDate("/home/hdp_lbg_supin/middata/jiang_chunfeng/sample/infostatisticsV1/:2"))
    //    println(getOutpathByDate("/home/hdp_lbg_supin/middata/jiang_chunfeng/sample/infostatisticsV1/:32"))
  }

}
