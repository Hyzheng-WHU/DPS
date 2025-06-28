package org.apache.openwhisk.core.scheduler.container

import org.apache.openwhisk.common._
import spray.json._
import java.nio.file.{Files, Paths}
import scala.collection.concurrent.TrieMap
import java.io.{File, FileWriter, PrintWriter}
import scala.util.Try
import java.util.concurrent.Executors
import scala.concurrent.duration._
import java.time.Instant

import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.Http
import akka.actor.ActorSystem
import akka.stream.Materializer
import spray.json._
import scala.util.{Success, Failure, Try}
import com.typesafe.config.ConfigFactory

// 数据模型
case class UpdateContainerRequest(dbName: String, state: String, workingTimestamp: Double)
case class UpdateResponse(status: String, message: String)

object HttpJsonProtocol extends DefaultJsonProtocol {
  implicit val updateRequestFormat = jsonFormat3(UpdateContainerRequest)
  implicit val updateResponseFormat = jsonFormat2(UpdateResponse)
}


// 用于接受来自容器的HTTP请求，修改对应dbInfo的属性
object ContainerDbInfoManagerHttp {
  
  // 导入 JSON Protocol
  import HttpJsonProtocol._

  // 创建 ActorSystem 和 Materializer
  // 定义独立的配置，避免影响其他组件
  val customConfig = ConfigFactory.parseString("""
  akka {
    actor {
      provider = local
      allow-java-serialization = off
    }
    remote.enabled = off  
    coordinated-shutdown.exit-jvm = off
    loggers = ["akka.event.slf4j.Slf4jLogger"]
    loglevel = "INFO"
    logging-filter = "akka.event.slf4j.Slf4jLoggingFilter"
    
    http {
      server {
        idle-timeout = 60s
        request-timeout = 30s
        max-connections = 1024
      }
    }
  }
  """).withFallback(ConfigFactory.load())
  implicit val system: ActorSystem = ActorSystem("ContainerDbInfoManagerHttp", customConfig)
  implicit val materializer: Materializer = Materializer(system)
  implicit val executionContext = system.dispatcher

  def startHttpServer()(implicit logging: Logging): Unit = {
    val route =
      path("dbInfo" / "updateByDbName") { // API 路径
        post {
          entity(as[String]) { jsonBody => // 接收 JSON 请求体
            val parsedRequest = Try(jsonBody.parseJson.convertTo[UpdateContainerRequest]) // 转换为数据模型
            parsedRequest match {
              case Success(request) =>
                // 提取字段
                val dbName = request.dbName
                val state = request.state
                val updateStateTimestamp = request.workingTimestamp

                // 执行业务逻辑
                if (dbName.nonEmpty && state.nonEmpty) {
                  ContainerDbInfoManager.updateDbInfoByDbName(dbName, state, updateStateTimestamp)
                  val response = UpdateResponse("success", s"Container with dbName $dbName updated successfully.")
                  complete(response.toJson.prettyPrint) // 返回 JSON 响应
                } else {
                  val response = UpdateResponse("error", "Missing required fields.")
                  complete(response.toJson.prettyPrint)
                }

              case Failure(ex) =>
                val response = UpdateResponse("error", s"Invalid request: ${ex.getMessage}")
                complete(response.toJson.prettyPrint)
            }
          }
        }
      }
    
    // 监听端口 8090
    Http().newServerAt("0.0.0.0", 6789).bind(route).onComplete {
      case Success(binding) =>
        val address = binding.localAddress
        logging.info(this, s"Server is listening on ${address.getHostString}:${address.getPort}")
      case Failure(ex) =>
        logging.error(this, s"Failed to bind HTTP endpoint: ${ex.getMessage}")
        system.terminate()
    }
  }
}

// 定义 ContainerDbInfo 类
case class ContainerDbInfo(activationId: String, 
                          creationId: String, 
                          containerId: String, 
                          dbName: String,  // 本容器挂载的db文件名
                          tables: List[String], // 当前msg中SQL涉及的的table名
                          state: String, // 容器当前状态，有 warm 、loading 、working 三种状态
                          updateStateTimestamp: Option[Double], // 容器进入 working 状态的时间戳，用于计算容器已经进行SQL的时间
                          predictWorkingTime: Option[Double],       // 预测SQL执行时间，以秒为单位
                          dbSizeLastRecord: Double, // 上次记录时，对应db文件的大小（MB）
                          invoker: String, // 容器所在的invoker
                          useCount: Int, // 容器使用次数
                          createTimestamp: Double // 创建时间戳
                          )

// 定义 JsonProtocol
object ContainerDbInfoJsonProtocol extends DefaultJsonProtocol {
  // 定义 ContainerDbInfo 的 JsonFormat
  implicit val containerDbInfoFormat: RootJsonFormat[ContainerDbInfo] = jsonFormat12(ContainerDbInfo)
}

object ContainerDbInfoManager {
  import ContainerDbInfoJsonProtocol._ // 导入 JsonFormat

  // 记录waiting请求初次决定waiting的时间戳，用于计算waiting的总时间
  private val idleStartTimeMap = TrieMap[String, Double]()
  private val idleTimeFilePath = "/db/container_idle_times.csv"

  private val dbInfoFilePath = "/db/container_db_info.json"

  // 内存缓存，初始为空
  @volatile private var cachedDbInfo: Map[String, ContainerDbInfo] = Map.empty

  // 全局增长速度 Map，记录每个条目的 dbSize 增长速度（单位：字节/秒）
  var dbSizeGrowthRateMap: Map[String, Double] = Map.empty

  // 定期刷新间隔，设置为 1 秒
  private val flushInterval = 1.seconds
  private val dbSizeFlushIntervalSec: Long = 5

  // 用于定期刷新缓存到磁盘的线程池
  private val scheduler = Executors.newSingleThreadScheduledExecutor()

  private def initIdleTimeRecorder()(implicit logging: Logging): Unit = {
    logging.info(this, "正在初始化idle时间记录器...")
    
    val file = new File(idleTimeFilePath)
    // 如果文件存在，检查其内容
    if (file.exists()) {
      var source: scala.io.Source = null
      try {
        // 读取并检查文件内容
        source = scala.io.Source.fromFile(file)
        val lines = source.getLines().toList
        
        if (lines.length <= 1) {
          // 文件为空或只有表头，直接删除
          logging.info(this, s"检测到空文件或只有表头，删除文件: ${file.getName}")
          file.delete()
        } else {
          // 文件有实际数据，执行重命名操作
          val dir = file.getParentFile
          val baseOldFileName = "old_idle_time_"
          
          // 查找现有的old文件，确定新的序号
          val oldFiles = dir.listFiles((_, name) => 
            name.startsWith(baseOldFileName) && name.endsWith(".csv"))
          
          val maxNumber = if (oldFiles == null || oldFiles.isEmpty) {
            0
          } else {
            oldFiles.map { f =>
              val numStr = f.getName.stripPrefix(baseOldFileName).stripSuffix(".csv")
              try {
                numStr.toInt
              } catch {
                case _: NumberFormatException => 0
              }
            }.max
          }
          
          // 生成新的old文件名
          val newNumber = maxNumber + 1
          val oldFile = new File(dir, s"${baseOldFileName}${newNumber}.csv")
          
          // 重命名现有文件
          if (file.renameTo(oldFile)) {
            logging.info(this, s"已将包含数据的文件重命名为: ${oldFile.getName}")
          } else {
            logging.error(this, s"无法重命名文件 ${file.getName} 到 ${oldFile.getName}")
          }
        }
      } catch {
        case e: Exception =>
          logging.error(this, s"处理已存在的容器idle时间记录文件时发生错误: ${e.getMessage}")
      } finally {
        if (source != null) {
          source.close()
        }
      }
    }
    
    // 创建新的等待时间文件
    val writer = new PrintWriter(file)
    try {
      writer.println("container_id,idle_time(s)")
      logging.info(this, s"已创建新容器idle时间记录文件: ${file.getName}")
    } finally {
      writer.close()
    }
  }
  
  // 初始化方法
  def init()(implicit logging: Logging): Unit = {
    logging.info(this, "Initializing ContainerDbInfoManager...")
    // loadCacheFromDisk() // 从磁盘加载数据到缓存
    startPeriodicFlush() // 启动定期刷新任务
    startPeriodicDbSizeRefresh()
    ContainerDbInfoManagerHttp.startHttpServer() // 启动 HTTP 服务
    ContainerStatsRecorder.init() // 初始化容器状态统计记录器
    initIdleTimeRecorder() // 初始化idle时间记录器
  }
  
  // 程序退出时的清理方法
  def shutdown()(implicit logging: Logging): Unit = {
    logging.info(this, s"Shutting down ContainerDbInfoManager.")
    scheduler.shutdown()
    flushCacheToDisk() // 最后一次刷新数据到磁盘
  }

  // 启动定期刷新任务
  private def startPeriodicFlush()(implicit logging: Logging): Unit = {
    scheduler.scheduleAtFixedRate(
      new Runnable {
        override def run(): Unit = Try {
          flushCacheToDisk()
        }.recover {
          case e: Exception =>
            logging.error(this, s"Error flushing dbInfo to disk: ${e.getMessage}")
        }
      },
      flushInterval.toMillis,
      flushInterval.toMillis,
      java.util.concurrent.TimeUnit.MILLISECONDS
    )
  }


  // 启动定期刷新任务
  private def startPeriodicDbSizeRefresh()(implicit logging: Logging): Unit = {
    scheduler.scheduleAtFixedRate(
      new Runnable {
        override def run(): Unit = {
          Try {
            refreshDbSizeLastRecord()
          }.recover {
            case e: Exception =>
              logging.error(this, s"Error refreshing dbSizeLastRecord: ${e.getMessage}")
          }
        }
      },
      dbSizeFlushIntervalSec, // 初始延迟
      dbSizeFlushIntervalSec, // 每次执行间隔
      java.util.concurrent.TimeUnit.SECONDS
    )
  }

  // 刷新dbSize逻辑
  private def refreshDbSizeLastRecord()(implicit logging: Logging): Unit = synchronized {
    // 直接修改现有的 map，而不是创建新的
    cachedDbInfo.foreach { case (containerId, entry) =>
      val dbPath = s"/db/${entry.dbName}"
      val lastDbSize = entry.dbSizeLastRecord

      // 获取当前文件大小
      val currentDbSize = Try {
        val filePath = Paths.get(dbPath)
        // if (Files.exists(filePath)) Files.size(filePath).toDouble else entry.dbSizeLastRecord
        if (Files.exists(filePath)) {
          val sizeInBytes = Files.size(filePath)
          (sizeInBytes.toDouble / (1024 * 1024)) // 转换为MB
        } else {
          entry.dbSizeLastRecord
        }
      }.getOrElse(lastDbSize)

      if (lastDbSize != 0 && entry.state == "loading") {
        // logging.info(this, s"${entry.state} 容器 $containerId 上次记录大小: $lastDbSize, 当前大小: $currentDbSize")
      
        // 计算增长速率
        val growthRate = (currentDbSize - lastDbSize) * 1024 * 1024 / dbSizeFlushIntervalSec

        // 更新增长速率映射
        if (growthRate > 0) {
          dbSizeGrowthRateMap = dbSizeGrowthRateMap.updated(containerId, growthRate)
          logging.info(this, s"容器 $containerId 的增长速率更新为 $growthRate B/s")
        }
      }

      // 只更新 dbSizeLastRecord 字段，保留其他所有字段
      cachedDbInfo = cachedDbInfo.updated(containerId, entry.copy(dbSizeLastRecord = currentDbSize))
    }
  }

  // 刷新内存缓存到磁盘
  private def flushCacheToDisk()(implicit logging: Logging): Unit = synchronized {
    // logging.info(this, s"Flushing dbInfo from memory cache to disk.")
    writeDbInfo()
  }

  // 从磁盘读取 dbInfo
  private def readDbInfo()(implicit logging: Logging): Map[String, ContainerDbInfo] = {
    if (!Files.exists(Paths.get(dbInfoFilePath))) {
      logging.warn(this, s"$dbInfoFilePath does not exist. Returning empty map.")
      Map.empty
    } else {
      val source = scala.io.Source.fromFile(dbInfoFilePath)
      val jsonStr = try source.mkString finally source.close()
      if (jsonStr.trim.isEmpty) {
        logging.warn(this, s"$dbInfoFilePath is empty. Returning empty map.")
        Map.empty
      } else {
        Try(jsonStr.parseJson.convertTo[Map[String, ContainerDbInfo]]).getOrElse {
          logging.error(this, s"Failed to parse JSON from $dbInfoFilePath. Returning empty map.")
          Map.empty
        }
      }
    }
  }

  // 将内存缓存写入磁盘
  private def writeDbInfo()(implicit logging: Logging): Unit = {
    val jsonStr = cachedDbInfo.toJson.prettyPrint
    val writer = new PrintWriter(new File(dbInfoFilePath))
    try {
      writer.write(jsonStr)
    } catch {
      case e: Exception =>
        logging.error(this, s"Failed to write dbInfo to $dbInfoFilePath: ${e.getMessage}")
    } finally {
      writer.close()
    }
  }

  // 提供的业务方法

  // 取内存缓存中的 dbInfo
  def getDbInfo()(implicit logging: Logging): Map[String, ContainerDbInfo] = synchronized {
    // logging.info(this, "geting dbInfo from memory cache.")
    cachedDbInfo
  }

  // 创建新的dbInfo条目
  def createDbInfo(creationId: String, dbFile: String, state: String, tables: List[String], predictWorkingTime: Option[Double])(implicit logging: Logging): Unit = synchronized {
    val activationId = " wait "
    val containerId = " wait "
    val updateStateTimestamp: Option[Double] = Some(Instant.now.getEpochSecond)
    val emptyList: List[String] = List()
    val dbSizeLastRecord = 0.0
    val invoker = "invoker0"
    val useCount = 0
    val createTimestamp = Instant.now.getEpochSecond
    cachedDbInfo = cachedDbInfo + (creationId -> ContainerDbInfo(activationId, creationId, containerId, dbFile, tables, state, updateStateTimestamp, predictWorkingTime, dbSizeLastRecord, invoker, useCount, createTimestamp))
    logging.info(this, s"New dbInfo created, new creationId: $creationId")
  }

  // 删除容器信息
  def removeContainerInfo(containerId: String)(implicit logging: Logging): Unit = synchronized {
    logging.info(this, s"Removing containerId: $containerId from memory cache.")
    cachedDbInfo = cachedDbInfo - containerId

    dbSizeGrowthRateMap -= containerId
    // logging.info(this, s"Removed containerId: $containerId, current cache: $cachedDbInfo")

    // 删除 idleStartTimeMap 中的记录，并计算idle时间
    val currentTime = System.currentTimeMillis() / 1000.0
    val idleTime =   idleStartTimeMap.remove(containerId).map { startTime =>
      currentTime - startTime
    }.getOrElse(0.0)  // 如果map中没有该key，则返回0.0
    
    
    val file = new File(idleTimeFilePath)
    if (!file.exists()) {
      // 文件不存在，创建新文件并写入表头和数据
      val writer = new PrintWriter(file)
      try {
        writer.println("container_id,idle_time(s)")
        writer.println(s"$containerId,$idleTime")
        logging.info(this, s"创建新文件 ${file.getName}")
      } finally {
        writer.close()
      }
    } else {
      // 文件存在，追加数据
      val writer = new PrintWriter(new FileWriter(file, true))
      try {
        writer.println(s"$containerId,$idleTime")
      } finally {
        writer.close()
      }
    }
  }

  // 根据 containerId 查找 dbName
  def findDbNameByContainerId(containerId: String)(implicit logging: Logging): Option[String] = synchronized {
    logging.info(this, s"Looking for dbName associated with containerId: $containerId")

    cachedDbInfo.get(containerId) match {
      case Some(containerDbInfo) =>
        logging.info(this, s"Found dbName: ${containerDbInfo.dbName} for containerId: $containerId")
        Some(containerDbInfo.dbName)
      case None =>
        logging.error(this, s"No dbName found for containerId: $containerId, dbInfo: $cachedDbInfo")
        None
    }
  }

  // 当需要锁定某容器时，将 containerId 对应的容器态置为 locked
  def updateStateToLocked(containerId: String)(implicit logging: Logging): Unit = synchronized {
    logging.info(this, s"Updating state to locked for containerId: $containerId.")
    cachedDbInfo = cachedDbInfo.get(containerId).map { info =>
      val updatedInfo = info.copy(state = "locked", updateStateTimestamp = None, predictWorkingTime = None)
      cachedDbInfo + (containerId -> updatedInfo)
    }.getOrElse {
      logging.error(this, s"ContainerId: $containerId not found in cache: $cachedDbInfo")
      cachedDbInfo
    }
  }
  
  // 现在系统置某条目为loading时，使用的是 updateDbInfoByDbName 函数
  // // 将 containerId 对应的容器态置为 loading
  // def updateStateToLoading(containerId: String, tablesNeeded: List[String], newPredictWorkingTime: Option[Double])(implicit logging: Logging): Unit = synchronized {
  //   logging.info(this, s"Updating state to loading for containerId: $containerId.")
  //   cachedDbInfo = cachedDbInfo.get(containerId).map { info =>
  //     // 合并原有的 tables 和新的 tablesNeeded，去重后形成并集
  //     val updatedTables = (info.tables ++ tablesNeeded).distinct
  //     val updatedInfo = info.copy(state = "loading", tables = updatedTables, updateStateTimestamp = Some(Instant.now.getEpochSecond), predictWorkingTime = newPredictWorkingTime, useCount = info.useCount + 1)
  //     cachedDbInfo + (containerId -> updatedInfo)
  //   }.getOrElse {
  //     logging.error(this, s"未能将容器 ContainerId: $containerId 置为 loading, 因为 not found in cachedDbInfo: $cachedDbInfo.")
  //     cachedDbInfo
  //   }
  //   // logging.info(this, s"Updated state to 'loading', current cache: $cachedDbInfo")
  // }

  // 更新容器状态为 warm
  def updateStateToWarm(containerId: String)(implicit logging: Logging): Unit = synchronized {
    // 记录容器变为warm的时间戳    
    val currentTime = System.currentTimeMillis() / 1000.0  // 转换为秒
    idleStartTimeMap(containerId) = currentTime
    logging.info(this, s"Container $containerId started warm at $currentTime")

    cachedDbInfo = cachedDbInfo.get(containerId).map { info =>
      val updatedInfo = info.copy(state = "warm", updateStateTimestamp = Some(Instant.now.getEpochSecond), predictWorkingTime = None)
      logging.info(this, s"Updated state to warm for containerId: $containerId.")
      cachedDbInfo + (containerId -> updatedInfo)
    }.getOrElse {
      logging.error(this, s"未能将容器 ContainerId: $containerId 置为 warm, 因为 not found in cachedDbInfo: $cachedDbInfo.")
      cachedDbInfo
    }
  }

  // 更新 containerId 对应的条目中的 predictWorkingTime
  def updatePredictWorkingTime(containerId: String, newPredictWorkingTime: Option[Double])(implicit logging: Logging): Unit = synchronized {
    logging.info(this, s"Updating predictWorkingTime for containerId: $containerId.")
    cachedDbInfo = cachedDbInfo.get(containerId) match {
      case Some(info) =>
        val updatedInfo = info.copy(predictWorkingTime = newPredictWorkingTime)
        cachedDbInfo + (containerId -> updatedInfo)
      case None =>
        logging.error(this, s"ContainerId: $containerId not found in cache.")
        cachedDbInfo
    }
    // logging.info(this, s"Updated predictWorkingTime, current cache: $cachedDbInfo")
  }

  // 根据 activationId 更新 creationId 和 containerId
  def updateActivationIdAndContainerId(
    activationIdToCreationIdMap: Map[String, String],
    activationIdToContainerIdMap: Map[String, String],
    activationId: String)(implicit logging: Logging): Unit = synchronized {
    logging.info(this, s"Updating activationId and containerId for activationId: $activationId.")
    val maybeCreationId = activationIdToCreationIdMap.get(activationId)
    val maybeContainerId = activationIdToContainerIdMap.get(activationId)

    (maybeCreationId, maybeContainerId) match {
      case (Some(creationId), Some(containerId)) =>
        cachedDbInfo = cachedDbInfo.get(creationId) match {
          case Some(existingInfo) =>
            val updatedInfo = existingInfo.copy(activationId = activationId, containerId = containerId)
            logging.info(this, s"activationId: $activationId, creationId: $creationId, containerId: $containerId")
            cachedDbInfo - creationId + (containerId -> updatedInfo) // 删除旧条目并添加新条目
          case None =>
            logging.info(this, s"No dbInfo entry for creationId: $creationId. Maybe warm start?")
            cachedDbInfo
        }
        // logging.info(this, s"Updated activationId and containerId, current cache: $cachedDbInfo")
      case _ =>
        logging.error(this, s"ActivationId: $activationId does not have corresponding creationId or containerId in the provided maps.")
    }
  }

  // 定义方法：更新 cachedDbInfo 的 tables
  def updateDbInfoTables(containerId: String, newTableList: List[String])(implicit logging: Logging): Unit = synchronized {
    cachedDbInfo.get(containerId) match {
      case Some(containerInfo) =>
        // 计算并集并更新 tables
        val updatedTables = (containerInfo.tables ++ newTableList).distinct
        val updatedContainerInfo = containerInfo.copy(tables = updatedTables)
        cachedDbInfo += (containerId -> updatedContainerInfo)

        logging.info(this, s"Updated tables for containerId $containerId. New tables: $updatedTables")
      case None =>
        logging.error(this, s"No entry found in cachedDbInfo for containerId: $containerId")
    }
  }


  def updateDbInfoByDbName(dbName: String, state: String, updateStateTimestamp: Double)(implicit logging: Logging): Unit = synchronized {
    
    // 查找匹配的容器条目
    val matchingEntry = cachedDbInfo.find { case (_, info) => info.dbName == dbName }
    
    matchingEntry match {
      case Some((containerId, containerInfo)) =>
        if (containerInfo.state == "prewarm") {
          // 对预热状态的容器，不更新其状态，因为loading状态的容器调度时有时不考虑
          logging.info(this, s"容器 $containerId 为预热状态，不更新状态")
          return
        }

        // 计算新的使用次数
        val newUseCount = if (state == "loading") {
          // 如果状态更新为loading，则增加使用次数
          containerInfo.useCount + 1
        } else {
          // 其他状态变更不改变使用次数
          containerInfo.useCount
        }

        // 更新条目
        val updatedInfo = containerInfo.copy(
          state = state,
          updateStateTimestamp = Some(updateStateTimestamp),
          useCount = newUseCount
        )
        cachedDbInfo += (containerId -> updatedInfo)
        logging.info(this, s"Updated containerId: $containerId dbName: $dbName state: $state updateStateTimestamp: $updateStateTimestamp")
      case None =>
        logging.error(this, s"No container found with dbName: $dbName")
    }
  }

  // 根据 containerId 查找 tables
  def findTablesByContainerId(containerId: String)(implicit logging: Logging): Option[List[String]] = synchronized {
    // logging.info(this, s"Looking for tables associated with containerId: $containerId")
    cachedDbInfo.get(containerId) match {
      case Some(containerDbInfo) =>
        // logging.info(this, s"Found tables: ${containerDbInfo.tables} for containerId: $containerId")
        Some(containerDbInfo.tables)
      case None =>
        logging.error(this, s"No tables found for containerId: $containerId, dbInfo: $cachedDbInfo")
        None
    }
  }

  // 返回指定状态的容器数
  def countEntriesByState(state: String): Int = {
    cachedDbInfo.values.count(_.state == state)
  }

}



// 定义状态计数的数据结构
case class ContainerStateStats(
  timestamp: String,    // 时间戳，格式：yyyy-MM-dd HH:mm:ss
  total: Int,          // 总容器数
  warm: Int,           // warm状态的容器数
  working: Int,        // working状态的容器数
  loading: Int,        // loading状态的容器数
  locked: Int,         // locked状态的容器数
  prewarm: Int         // prewarm状态的容器数
)

import java.io.{File, FileWriter, PrintWriter}

// 容器状态统计记录器
object ContainerStatsRecorder {
  private val statsFilePath = "/db/container_stats.csv"
  private val scheduler = Executors.newSingleThreadScheduledExecutor()
  private val recordInterval = 0.5.seconds
  private val header = "time,total,warm,working,loading,locked,prewarm"
  private val dateTimeFormatter = java.time.format.DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss").withZone(java.time.ZoneId.systemDefault())

  // 使用 volatile 保证多线程可见性
  @volatile private var lastStats: Option[ContainerStateStats] = None

  def init()(implicit logging: Logging): Unit = {
    logging.info(this, "正在初始化容器状态记录器...")
    
    val file = new File(statsFilePath)  
          
    // 如果文件存在，检查其内容
    if (file.exists()) {
      var source: scala.io.Source = null
      try {
        // 读取并检查文件内容
        source = scala.io.Source.fromFile(file)
        val lines = source.getLines().toList
        
        if (lines.length <= 1) {
          // 文件为空或只有表头，直接删除
          logging.info(this, s"检测到空文件或只有表头，删除文件: ${file.getName}")
          file.delete()
        } else {
          // 文件有实际数据，执行重命名操作
          val dir = file.getParentFile
          val baseOldFileName = "old_container_stats_"

          // 查找现有的old文件，确定新的序号
          val oldFiles = dir.listFiles((_, name) => name.startsWith(baseOldFileName) && name.endsWith(".csv"))
          val maxNumber = if (oldFiles == null || oldFiles.isEmpty) {
            0
          } else {
            oldFiles.map { f =>
              val numStr = f.getName.stripPrefix(baseOldFileName).stripSuffix(".csv")
              try {
                numStr.toInt
              } catch {
                case _: NumberFormatException => 0
              }
            }.max
          }
          
          // 生成新的old文件名
          val newNumber = maxNumber + 1
          val oldFile = new File(dir, s"${baseOldFileName}${newNumber}.csv")
          
          // 重命名现有文件
          if (file.renameTo(oldFile)) {
            logging.info(this, s"已将包含数据的文件重命名为: ${oldFile.getName}")
          } else {
            logging.error(this, s"无法重命名文件 ${file.getName} 到 ${oldFile.getName}")
          }
        }
      } catch {
        case e: Exception => 
          logging.error(this, s"处理已存在的统计文件时发生错误: ${e.getMessage}")
      } finally {
        if (source != null) {
          source.close()
        }
      }
    }
    
    // 创建新的统计文件
    val writer = new PrintWriter(file)
    try {
      writer.println(header)
      logging.info(this, s"已创建新的统计文件: ${file.getName}")
    } finally {
      writer.close()
    }

    startPeriodicRecording()
  }

  def shutdown(): Unit = {
    scheduler.shutdown()
  }

  private def startPeriodicRecording()(implicit logging: Logging): Unit = {
    scheduler.scheduleAtFixedRate(
      new Runnable {
        override def run(): Unit = {
          try {
            recordCurrentStats()
          } catch {
            case e: Exception =>
              logging.error(this, s"记录容器状态统计信息时发生错误: ${e.getMessage}")
          }
        }
      },
      0,
      recordInterval.toMillis,
      java.util.concurrent.TimeUnit.MILLISECONDS
    )
  }

  // 统计当前状态 - 直接访问 cachedDbInfo
  private def computeCurrentStats()(implicit logging: Logging): ContainerStateStats = {
    val instant = Instant.now()
    val formattedTimestamp = dateTimeFormatter.format(instant)
    
    // 使用一次遍历完成所有状态的计数
    val cachedDbInfo = ContainerDbInfoManager.getDbInfo()
    val counts = cachedDbInfo.values.foldLeft((0, 0, 0, 0, 0)) {
      case ((warm, working, loading, locked, prewarm), info) =>
        info.state match {
          case "warm" => (warm + 1, working, loading, locked, prewarm)
          case "working" => (warm, working + 1, loading, locked, prewarm)
          case "loading" => (warm, working, loading + 1, locked, prewarm)
          case "locked" => (warm, working, loading, locked + 1, prewarm)
          case "prewarm" => (warm, working, loading, locked, prewarm + 1)
          case _ => (warm, working, loading, locked, prewarm)
        }
    }

    // 计算总容器数
    val total = counts._1 + counts._2 + counts._3 + counts._4 + counts._5

    ContainerStateStats(
      formattedTimestamp,
      total,
      counts._1, // warm
      counts._2, // working
      counts._3, // loading
      counts._4, // locked
      counts._5  // prewarm
    )
  }

  // 记录当前状态统计信息
  private def recordCurrentStats()(implicit logging: Logging): Unit = {
    // 计算新的统计数据
    val stats = computeCurrentStats()
    
    // 只在总数不为0时记录
    if (stats.total > 0) {
      // 生成CSV行数据
      val statsLine = s"${stats.timestamp},${stats.total},${stats.warm},${stats.working},${stats.loading},${stats.locked},${stats.prewarm}"
      
      // 追加写入文件
      val writer = new FileWriter(statsFilePath, true)
      try {
        writer.write(s"$statsLine\n")
      } finally {
        writer.close()
      }
    }
  }

}