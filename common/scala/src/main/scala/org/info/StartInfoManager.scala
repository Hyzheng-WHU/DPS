package org.apache.openwhisk.core.scheduler.container

import org.apache.openwhisk.common._
import spray.json._
import java.io.{File, PrintWriter}
import java.util.concurrent.Executors
import scala.concurrent.duration._

import scala.util.{Try, Failure, Success}
import java.nio.file.{Files, Paths, StandardCopyOption}
import scala.io.Source

// 定义数据结构，用来映射 JSON 文件中的记录
case class StartInfo(
  activationId: String,
  containerId: String,
  creationId: String,
  dbName: String,
  startMode: String
)

// 让 spray-json 支持 StartInfo 的 JSON 格式解析
object StartInfoJsonProtocol extends DefaultJsonProtocol {
  implicit val startInfoFormat = jsonFormat5(StartInfo)
}

object StartInfoManager {
  import StartInfoJsonProtocol._ 

  // 定义文件路径为对象的成员变量
  private val startInfoFilePath: String = "/db/start_info.json"
  private val tempFilePath: String = "/db/start_info.json.tmp"
  private val completeMarkerPath: String = "/db/start_info.complete"

  // 内存缓存，初始为空
  @volatile private var cachedStartInfo: Seq[StartInfo] = Seq.empty

  // 定期刷新间隔，设置为 1 秒
  private val flushInterval = 5.seconds

  // 用于定期刷新缓存到磁盘的线程池
  private val scheduler = Executors.newSingleThreadScheduledExecutor()

  // 初始化 StartInfoManager
  def init()(implicit logging: Logging): Unit = {
    logging.info(this, "Initializing StartInfoManager...")
    // loadCacheFromDisk() // 从磁盘加载数据到缓存
    startPeriodicFlush()
  }

  /** 从磁盘加载数据到缓存 */
  // private def loadCacheFromDisk()(implicit logging: Logging): Unit = synchronized {
  //   logging.info(this, s"Loading startInfo from $startInfoFilePath into memory cache.")
  //   if (!Files.exists(Paths.get(startInfoFilePath))) {
  //     logging.warn(this, s"File $startInfoFilePath does not exist. Initializing empty cache.")
  //     cachedStartInfo = Seq.empty
  //   } else {
  //     val source = Source.fromFile(startInfoFilePath)
  //     val fileContent = try source.mkString finally source.close()
  //     cachedStartInfo = Try(fileContent.parseJson.convertTo[Seq[StartInfo]]).getOrElse {
  //       logging.error(this, s"Failed to parse JSON from $startInfoFilePath. Initializing empty cache.")
  //       Seq.empty
  //     }
  //   }
  // }

  /** 定期将缓存内容刷到磁盘 */
  private def startPeriodicFlush()(implicit logging: Logging): Unit = {
    scheduler.scheduleAtFixedRate(
      new Runnable {
        override def run(): Unit = {
          try {
            flushCacheToDisk()
          } catch {
            case e: Exception =>
              logging.error(this, s"Error flushing startInfo to disk: ${e.getMessage}")
          }
        }
      },
      flushInterval.toMillis,
      flushInterval.toMillis,
      java.util.concurrent.TimeUnit.MILLISECONDS
    )
    logging.info(this, s"Periodic flush to disk started with interval: $flushInterval")
  }

  /** 删除完成标记 */
  private def removeCompleteMarker(): Unit = {
    val markerFile = new File(completeMarkerPath)
    if (markerFile.exists()) {
      markerFile.delete()
    }
  }
  
  /** 创建完成标记 */
  private def createCompleteMarker(): Unit = {
    val markerFile = new File(completeMarkerPath)
    markerFile.createNewFile()
  }
  
  /** 检查写入是否完成 */
  private def isWriteComplete(): Boolean = {
    new File(completeMarkerPath).exists()
  }

  /** 带有重试机制的文件读取 */
  private def readWithRetry[T](attempts: Int = 3, delayMs: Long = 100)(op: => T): T = {
    var lastException: Throwable = null
    for (attempt <- 1 to attempts) {
      try {
        return op
      } catch {
        case e: Exception =>
          lastException = e
          if (attempt < attempts) {
            Thread.sleep(delayMs)
          }
      }
    }
    throw lastException
  }

  /** 刷新缓存到磁盘 */
  private def flushCacheToDisk()(implicit logging: Logging): Unit = synchronized {
    // 移除完成标记，表示开始写入
    removeCompleteMarker()
    
    val tempFile = new File(tempFilePath)
    val targetFile = new File(startInfoFilePath)
    
    // 写入临时文件
    val writer = new PrintWriter(tempFile)
    try {
      writer.write(cachedStartInfo.toJson.prettyPrint)
      writer.flush()
    } finally {
      writer.close()
    }
    
    // 原子性地重命名文件
    if (!tempFile.renameTo(targetFile)) {
      logging.error(this, s"Failed to rename temp file to target file")
      // 如果重命名失败，进行复制
      Files.copy(tempFile.toPath, targetFile.toPath, 
        StandardCopyOption.REPLACE_EXISTING)
      tempFile.delete()
    }
    
    // 创建完成标记，表示写入完成
    createCompleteMarker()
  }

  /** 停止定期刷盘并立即刷新缓存 */
  def shutdown()(implicit logging: Logging): Unit = {
    logging.info(this, "Shutting down StartInfoManager.")
    scheduler.shutdown()
    flushCacheToDisk()
  }

  /** 获取内存中的 startInfo */
  def getStartInfo()(implicit logging: Logging): Seq[StartInfo] = synchronized {
    logging.info(this, "Fetching startInfo from memory cache.")
    cachedStartInfo
  }

  // 提供的业务方法

  // 通过 containerId 获取对应的 creationId
  def findCreationIdByContainerId(containerId: String)(implicit logging: Logging): Option[String] = synchronized {
    // 查找匹配的条目并返回 creationId
    val creationId = cachedStartInfo.find(_.containerId == containerId).map(_.creationId)
    logging.info(this, s"Find creationId: $creationId for containerId: $containerId")
    logging.info(this, s"cachedStartInfo: $cachedStartInfo")
    creationId
  }

  // 通过 containerId 获取对应的 creationId
  def findStartModeByCreationId(creationId: String)(implicit logging: Logging): Option[String] = synchronized {
    
    // 查找匹配的条目并返回 creationId
    val startMode = cachedStartInfo.find(_.creationId == creationId).map(_.startMode)
    
    logging.info(this, s"Find startMode: $startMode for creationId: $creationId.")
    startMode
  }

  def findDbNameByActivationId(targetActivationId: String)(implicit logging: Logging): Option[String] = synchronized {
    logging.info(this, s"Finding dbName for activationId: $targetActivationId.")
    // logging.info(this, s"now cachedStartInfo: $cachedStartInfo.")
    // logging.info(this, s"StartInfoManager instance: ${System.identityHashCode(this)}")

    // 查找匹配的条目并返回 creationId
    cachedStartInfo.find(_.activationId == targetActivationId).map(_.dbName)
  }

  /** 根据 creationId 查找条目 */
  def findEntryByCreationId(creationId: String)(implicit logging: Logging): Option[StartInfo] = synchronized {
    logging.info(this, s"Searching for entry with creationId: $creationId in StartInfo cache.")
    val entry = cachedStartInfo.find(_.creationId == creationId)
    entry match {
      case Some(info) =>
        logging.info(this, s"Found entry: $info")
      case None =>
        logging.error(this, s"No entry found for creationId: $creationId")
    }
    entry
  }


  // 检查是否存在指定的 activationId
  def existActivationId(activationId: String)(implicit logging: Logging): Boolean = synchronized {
    logging.info(this, s"Checking existence of activationId: $activationId.")
    
    // 判断缓存中是否存在指定的 activationId
    cachedStartInfo.exists(_.activationId == activationId)
  }

  // 针对热启动情况，更新StartInfo 文件, 根据creationId更新ContainerId，并在dbInfo中查找dbName来更新
  def updateContainerIdByCreationId(creationId: String, newContainerId: String)(implicit logging: Logging): Unit = synchronized {
    logging.info(this, s"Updating containerId for creationId: $creationId with newContainerId: $newContainerId.")

    // 读取 dbInfo
    val dbInfo = ContainerDbInfoManager.getDbInfo()

    // 更新内存缓存
    cachedStartInfo = cachedStartInfo.map { info =>
      if (info.creationId == creationId) {
        logging.info(this, s"Found info: $info")
        dbInfo.get(newContainerId) match {
          case Some(containerDbInfo) =>
            logging.info(this, s"new containerId: $newContainerId. new dbName: ${containerDbInfo.dbName}")
            info.copy(containerId = newContainerId, dbName = containerDbInfo.dbName)
          case None =>
            logging.error(this, s"未找到容器: $newContainerId 的dbInfo, 仅更新startInfo中的 containerId.")
            info.copy(containerId = newContainerId)
        }
      } else info
    }
    // 立即刷盘
    flushCacheToDisk()
  }

  // 根据activationId更新containerId
  def updateContainerIdByActivationId(activationId: String, newContainerId: String)(implicit logging: Logging): Unit = synchronized {
    logging.info(this, s"Updating containerId for activationId: $activationId with newContainerId: $newContainerId.")

    // 更新内存缓存
    cachedStartInfo = cachedStartInfo.map { info =>
      if (info.activationId == activationId) {
        logging.info(this, s"Found startInfo, 更新成功.")
        info.copy(containerId = newContainerId)
      } else info
    }
    // 立即刷盘
    flushCacheToDisk()
  }

  // 创建新的配对并添加到 StartInfo 内存缓存中
  def createStartInfo(creationId: String, activationId: String, containerId: String, dbName: String, startMode: String)(implicit logging: Logging): Unit = synchronized {
    logging.info(this, s"Creating new StartInfo entry for creationId: $creationId.")
    
    // 创建新的条目并添加到缓存
    val newEntry = StartInfo(activationId, containerId, creationId, dbName, startMode)
    cachedStartInfo :+= newEntry
    // 立即刷盘
    flushCacheToDisk()
  }

  // 根据 activationId 删除指定的条目
  def deleteEntryByContainerId(deleteContainerId: String)(implicit logging: Logging): Unit = synchronized {
    logging.info(this, s"Deleting StartInfo entry with containerId: $deleteContainerId.")
    
    // 从内存缓存中删除条目
    cachedStartInfo = cachedStartInfo.filterNot(_.containerId == deleteContainerId)
    // 立即刷盘
    flushCacheToDisk()
  }

  // 根据传入的activationId在activationIdToCreationIdMap中找到对应的CreationId，并在StartInfoArray中找到对应的条目，更新其containerId以及activationId值
  def updateActivationAndContainerId(
      activationIdToCreationIdMap: Map[String, String],
      containerId: String,
      activationId: String)(implicit logging: Logging): Unit = synchronized {
    
    logging.info(this, s"Updating activationId and containerId for activationId: $activationId.")
    
    // 在映射中找到对应的 creationId
    val maybeCreationId = activationIdToCreationIdMap.get(activationId)
    
    if (maybeCreationId.isEmpty) {
      logging.error(this, s"No matching creationId found for activationId: $activationId in activationIdToCreationIdMap: $activationIdToCreationIdMap")
      return
    }

    val creationId = maybeCreationId.get

    // 更新缓存数据
    cachedStartInfo = cachedStartInfo.map { info =>
      if (info.creationId == creationId) {
        logging.info(this, s"Found entry for creationId: $creationId. Updating activationId: $activationId and containerId: $containerId")
        info.copy(activationId = activationId, containerId = containerId)
      } else info
    }
    // 立即刷盘
    flushCacheToDisk()
  }

  // from disk的业务函数，由于invoker不能访问scheduler0的内存，所以不能通过内存计算，只能通过磁盘

  // 从磁盘读取 JSON 并解析为 StartInfo 列表
  private def readStartInfoFromDisk()(implicit logging: Logging): Try[Seq[StartInfo]] = {
    // 等待并检查写入完成标记
    readWithRetry() {
      if (!isWriteComplete()) {
        throw new IllegalStateException("Write operation not complete")
      }
      
      if (!Files.exists(Paths.get(startInfoFilePath))) {
        Success(Seq.empty)
      } else {
        val source = Source.fromFile(startInfoFilePath)
        try {
          val fileContent = source.mkString
          Success(fileContent.parseJson.convertTo[Seq[StartInfo]])
        } catch {
          case e: Exception =>
            Failure(e)
        } finally {
          source.close()
        }
      }
    }
  }
  
  // 从磁盘读取并查找 dbName
  def findDbNameByActivationIdFromDisk(targetActivationId: String)(implicit logging: Logging): Option[String] = synchronized {
    logging.info(this, s"Searching for dbName for activationId: $targetActivationId from disk.")
    readStartInfoFromDisk() match {
      case Success(startInfoList) =>
        val dbName = startInfoList.find(_.activationId == targetActivationId).map(_.dbName)
        if (dbName.isDefined) {
          logging.info(this, s"Found dbName: ${dbName.get} for activationId: $targetActivationId.")
        } else {
          logging.error(this, s"No matching dbName found for activationId: $targetActivationId.")
        }
        dbName
        
      case Failure(exception) =>
        logging.error(this, s"Failed to read from disk: ${exception.getMessage}")
        None
    }
  }
  // 从磁盘读取并查找条目
  def findEntryByCreationIdFromDisk(targetCreationId: String)(implicit logging: Logging): Option[StartInfo] = synchronized {
    logging.info(this, s"Searching for entry with creationId: $targetCreationId from disk.")
    readStartInfoFromDisk() match {
      case Success(startInfoList) =>
        val entry = startInfoList.find(_.creationId == targetCreationId)
        if (entry.isDefined)
          logging.info(this, s"Found entry: ${entry.get}")
        else
          logging.error(this, s"No entry found for creationId: $targetCreationId.")
        entry
      case Failure(exception) =>
        logging.error(this, s"Failed to read from disk: ${exception.getMessage}")
        None
    }
  }

}