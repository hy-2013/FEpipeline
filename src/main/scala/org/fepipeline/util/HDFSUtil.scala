package org.fepipeline.util

import java.text.SimpleDateFormat
import java.util.{Date, Locale}

import org.apache.hadoop.fs.Path
import org.apache.spark.SparkContext

import scala.collection.mutable.ArrayBuffer

/**
  * 关于HDFS的所有操作
  *
  */
object HDFSUtil {

  /**
    * 获取pathString目录下的所有文件夹名称
    *
    * @param sc
    * @param pathString
    * @return
    */
  def getAllFileName(sc: SparkContext, pathString: String): Array[String] = {
    if (null == sc || null == pathString) {
      println(s"Error:HDFSUtil getAllFileName's param is null")
      sys.exit(0)
    }

    val path = new Path(pathString)
    val fs = path.getFileSystem(sc.hadoopConfiguration)
    val names = fs.listStatus(path).map(_.getPath.getName)
    fs.close()
    names
  }

  /**
    * 获取pathString目录下最新的文件夹路径
    *
    * @param sc
    * @param pathString
    * @return
    */
  def getLatelyFolder(sc: SparkContext, pathString: String): String = {
    if (null == sc || null == pathString) {
      println(s"Error:HDFSUtil getLatelyFolder's param is null")
      sys.exit(0)
    }

    val names = getAllFileName(sc, pathString).sorted
    s"$pathString/${names(names.length - 1)}"
  }

  /**
    *
    * @param sc
    * @param pathString
    * @param dateFormat
    * @param day
    * @param currentTime
    * @param today 是否包含当天文件夹 默认false
    * @return
    */
  def getAllPathByDay(sc: SparkContext,
                      pathString: String,
                      dateFormat: String,
                      day: Int,
                      currentTime: Long,
                      today: Boolean = false): ArrayBuffer[String] = {
    if (null == sc || null == pathString || null == dateFormat || null == day || null == currentTime) {
      println(s"Error:HDFSUtil getAllPathByDay's param is null")
      sys.exit(0)
    }

    val names = getAllFileName(sc, pathString)
    //将天转换为毫秒
    val periodTime: Long = day * 86400000L
    val simpleDateFormat = new SimpleDateFormat(dateFormat, Locale.ROOT)
    val currentDate = simpleDateFormat.parse(simpleDateFormat.format(new Date(currentTime))).getTime
    val paths = new ArrayBuffer[String]()
    names.foreach { f =>
      try {
        val time = currentDate - simpleDateFormat.parse(f).getTime
        if (time <= periodTime) {
          if (today) {
            if (time < periodTime)
              paths.+=(s"$pathString/$f/")
          } else if (time > 0)
            paths.+=(s"$pathString/$f/")
        }
      } catch {
        case e: Throwable =>
          println(s"Error:getAllPathByDay:获取路径错误，${e.getMessage}")
      }
    }
    paths
  }

  /**
    * 创建pathString路径
    *
    * @param sc
    * @param pathString
    * @return
    */
  def create(sc: SparkContext,
             pathString: String) = {
    if (null == sc || null == pathString) {
      println(s"Error:HDFSUtil delete's param is null")
      sys.exit(0)
    }
    val path = new Path(pathString)
    val fs = path.getFileSystem(sc.hadoopConfiguration)
    fs.create(path, false)
    fs.close()
  }

  /**
    * 如果pathString路径存在,则删除
    *
    * @param sc
    * @param pathString
    * @return
    */
  def del(sc: SparkContext, pathString: String): Unit = {
    if (null == sc || null == pathString) {
      println(s"Error:HDFSUtil delete's param is null")
      sys.exit(0)
    }
    val path = new Path(pathString)
    val fs = path.getFileSystem(sc.hadoopConfiguration)
    if (fs.exists(path)) {
      fs.delete(path, true)
    }
    fs.close()
  }

  /**
    * 删除pathString路径
    *
    * @param sc
    * @param pathString
    * @return
    */
  def delete(sc: SparkContext,
             pathString: String): Boolean = {
    if (null == sc || null == pathString) {
      println(s"Error:HDFSUtil delete's param is null")
      sys.exit(0)
    }
    val path = new Path(pathString)
    val fs = path.getFileSystem(sc.hadoopConfiguration)
    val flag = fs.delete(path, true)
    fs.close()
    flag
  }

  /**
    * 判断pathString路径是否存在
    *
    * @param sc
    * @param pathString
    * @return
    */
  def exist(sc: SparkContext,
            pathString: String) = {
    if (null == sc || null == pathString) {
      println(s"Error:HDFSUtil delete's param is null")
      sys.exit(0)
    }
    val path = new Path(pathString)
    val fs = path.getFileSystem(sc.hadoopConfiguration)
    val flag = fs.exists(path)
    fs.close()
    flag
  }

  /**
    * 获取pathString路径下文件（夹）的个数
    *
    * @param sc
    * @param pathString
    */
  def getLenByPath(sc: SparkContext,
                   pathString: String) = {
    if (null == sc || null == pathString) {
      println(s"Error:HDFSUtil delete's param is null")
      sys.exit(0)
    }
    val path = new Path(pathString)
    val fs = path.getFileSystem(sc.hadoopConfiguration)
    val len = fs.listStatus(path).length
    fs.close()
    len
  }

  def getFilename(filePath: String): String = {
    if (filePath == null || filePath.length < 2)
      throw new RuntimeException(s"No such file $filePath")
    filePath.split("/").last
  }

  /**
    * 获取pathString路径下数据大小
    *
    * @param sc
    * @param pathString
    */
  def getContentSize(sc: SparkContext,
                     pathString: String) = {
    if (null == sc || null == pathString) {
      println(s"Error:param is null")
      sys.exit(0)
    }
    var len = 0L
    pathString.split(",").foreach { ps =>
      val path = new Path(ps)
      val fs = path.getFileSystem(sc.hadoopConfiguration)
      len = len + fs.getContentSummary(path).getLength
      fs.close()
    }
    len
  }

}
