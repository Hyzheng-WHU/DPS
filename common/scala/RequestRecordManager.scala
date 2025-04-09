package org.apache.openwhisk.core.scheduler.container

import org.apache.openwhisk.common._
import java.time.Instant
import scala.concurrent.duration._
import java.time.temporal.ChronoUnit
import java.util.concurrent.ConcurrentHashMap
import scala.collection.JavaConverters._
import scala.concurrent.ExecutionContext.Implicits.global
import akka.http.scaladsl.Http
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.model._
import scala.util.{Failure, Success}
import akka.actor.ActorSystem
import java.util.concurrent.{Executors, ScheduledExecutorService, TimeUnit}
import scala.util.Try
import com.typesafe.config.ConfigFactory
import spray.json._
import scala.util.control.Breaks._
import scala.collection.mutable

import java.io.{FileWriter, BufferedWriter}
import java.util.concurrent.Executors
import scala.concurrent.duration._

// 请求记录数据结构
case class RequestRecord(
  receiveTime: Instant,
  tablesNeeded: List[String]
)

// 请求记录工具函数
object RequestRecordManager {
  // 用于主动移除容器时读取
  val containerCheckIntervalInSec = 10

  // var validStatesWhenKM = List("warm")
  val validStatesWhenKM = List("warm", "working")

  val removeStrategy = "none"
  // "none" 、 "best" 、 "random" 、 "redundancy"

  // 按分钟分桶存储请求记录，用于记录每个新到请求的表情况
  private val requestBuckets = new ConcurrentHashMap[Long, List[RequestRecord]]().asScala
  // 按分钟分桶存储请求记录，用于记录每个新到且被调度为等待或冷启动的请求
  private val waitAndColdRequestBuckets = new ConcurrentHashMap[Long, List[RequestRecord]]().asScala

  // 已经记录过表情况的请求，用于过滤重新调度的请求
  private val recordTableCreationId: mutable.Set[String] = mutable.Set()
  // 已经记录过调度情况的请求，用于过滤重新调度的请求
  private val recordCreationId: mutable.Set[String] = mutable.Set()

  private val request_record: mutable.Map[String, (Instant, List[String], Option[String])] = mutable.Map.empty
  private val request_record_path = "/db/request_record.txt"

  // 用于定期刷新缓存到磁盘的线程池
  private val scheduler = Executors.newSingleThreadScheduledExecutor()

  // 启动定期刷新任务
  def startFlush()(implicit logging: Logging): Unit = {
    scheduler.scheduleAtFixedRate(
      new Runnable {
        override def run(): Unit = Try {
          saveToFile()
        }.recover {
          case e: Exception =>
            logging.error(this, s"Error flushing dbInfo to disk: ${e.getMessage}")
        }
      },
      60,
      60,
      java.util.concurrent.TimeUnit.SECONDS
    )
  }

  // 添加请求记录
  def addRequestRecord(creationId: String, tablesNeeded: List[String])(implicit logging: Logging): Unit = {
    if(recordTableCreationId.contains(creationId)){
      logging.info(this, s"已存在creationId: $creationId 的请求记录，跳过")
      return
    }
    
    recordTableCreationId.add(creationId)
    val receiveTime = Instant.now()
    logging.info(this, s"新请求记录: tablesNeeded: $tablesNeeded at $receiveTime")

    val record = RequestRecord(receiveTime, tablesNeeded)
    val bucketKey = receiveTime.getEpochSecond / 60 // 将时间戳转换为分钟粒度

    requestBuckets.synchronized {
      val updatedList = requestBuckets.getOrElse(bucketKey, List()) :+ record
      requestBuckets.update(bucketKey, updatedList)
    }

    // 定期清理过期记录（可指定清理时间窗口）
    cleanUpOldRecords(30) // 默认清理超过60分钟的记录

    val timestamp = Instant.now() // 获取当前时间戳
    request_record(creationId) = (timestamp, tablesNeeded, None)
  
  }

  def addWaitAndColdRequestRecord(creationId: String, tablesNeeded: List[String], startMode: String)(implicit logging: Logging): Unit = {
    if(recordCreationId.contains(creationId)){
      logging.info(this, s"已存在creationId: $creationId 的请求记录，跳过")
      return
    }
    
    recordCreationId.add(creationId)
    val receiveTime = Instant.now()
    logging.info(this, s"新WaitAndCold请求记录: tablesNeeded: $tablesNeeded at $receiveTime")

    val record = RequestRecord(receiveTime, tablesNeeded)
    val bucketKey = receiveTime.getEpochSecond / 60 // 将时间戳转换为分钟粒度

    waitAndColdRequestBuckets.synchronized {
      val updatedList = waitAndColdRequestBuckets.getOrElse(bucketKey, List()) :+ record
      waitAndColdRequestBuckets.update(bucketKey, updatedList)
    }

    // 定期清理过期记录（可指定清理时间窗口）
    cleanUpOldRecords(30) // 默认清理超过60分钟的记录

    request_record.get(creationId) match {
      case Some((timestamp, tablesNeeded, _)) =>
        request_record(creationId) = (timestamp, tablesNeeded, Some(startMode)) // 更新记录
      case _ =>
    }
  }

  def saveToFile(): Unit = {
    val writer = new BufferedWriter(new FileWriter(request_record_path))
    try {
      request_record.foreach { case (creationId, (timestamp, tablesNeeded, appendedStr)) =>
        val tablesStr = tablesNeeded.mkString(",")
        val appended = appendedStr.getOrElse("warm")
        writer.write(s"$creationId|$timestamp|$tablesStr|$appended")
        writer.newLine()
      }
    } finally {
      writer.close()
    }
  }
  
  // 获取给定时间段内的请求数
  def getRequestCountWithinDuration(endTime: Instant, durationInSeconds: Long)(implicit logging: Logging): Int = {
    val startTime = endTime.minus(durationInSeconds, ChronoUnit.SECONDS)
    val startTimeKey = startTime.getEpochSecond / 60
    val endTimeKey = endTime.getEpochSecond / 60

    val count = requestBuckets.synchronized {
      requestBuckets
        .filter { case (bucketKey, _) => bucketKey >= startTimeKey && bucketKey <= endTimeKey }
        .foldLeft(0) { case (totalCount, (bucketKey, records)) =>
          if (bucketKey == startTimeKey || bucketKey == endTimeKey) {
            totalCount + records.count(record =>
              record.receiveTime.compareTo(startTime) >= 0 && record.receiveTime.compareTo(endTime) < 0
            )
          } else {
            totalCount + records.size
          }
        }
    }

    // logging.info(this, s"从 ${startTime} 到 ${endTime} 的请求共: $count 个")
    count
  }

  // 获取给定时间段内的请求数
  def getWaitAndColdRequestCountWithinDuration(endTime: Instant, durationInSeconds: Long)(implicit logging: Logging): Int = {
    val startTime = endTime.minus(durationInSeconds, ChronoUnit.SECONDS)
    val startTimeKey = startTime.getEpochSecond / 60
    val endTimeKey = endTime.getEpochSecond / 60

    val count = waitAndColdRequestBuckets.synchronized {
      waitAndColdRequestBuckets
        .filter { case (bucketKey, _) => bucketKey >= startTimeKey && bucketKey <= endTimeKey }
        .foldLeft(0) { case (totalCount, (bucketKey, records)) =>
          if (bucketKey == startTimeKey || bucketKey == endTimeKey) {
            totalCount + records.count(record =>
              record.receiveTime.compareTo(startTime) >= 0 && record.receiveTime.compareTo(endTime) < 0
            )
          } else {
            totalCount + records.size
          }
        }
    }

    // logging.info(this, s"从 ${startTime} 到 ${endTime} 的请求共: $count 个")
    count
  }

  // 获取给定时间段内每个请求访问的表的情况
  def getRequestTablesWithinDuration(endTime: Instant, durationInSeconds: Long)(implicit logging: Logging): List[List[String]] = {
    val startTime = endTime.minus(durationInSeconds, ChronoUnit.SECONDS)
    val startTimeKey = startTime.getEpochSecond / 60
    val endTimeKey = endTime.getEpochSecond / 60

    val tableLists = requestBuckets.synchronized {
      requestBuckets
        .filterKeys(_ >= startTimeKey) // 筛选可能相关的桶
        .flatMap { case (bucketKey, records) =>
          if (bucketKey > startTimeKey && bucketKey < endTimeKey) {
            // 完全包含的桶，所有记录都相关
            records
          } else {
            // 开始和结束的桶，逐条检查时间戳
            records.filter(record =>
              record.receiveTime.isAfter(startTime) && record.receiveTime.isBefore(endTime)
            )
          }
        }
        .map(_.tablesNeeded) // 提取每个请求的 tablesNeeded
        .toList
    }

    logging.info(this, s"从 ${startTime} 到 ${endTime} 每个请求访问的表: $tableLists")
    tableLists
  }

  // 从时间窗口里获取请求表并采样
  def getSampleRequestTablesWithinDuration(endTime: Instant, durationInSeconds: Long, sampleRate: Double)(implicit logging: Logging): List[List[String]] = {
    // 获取指定时间窗口内的所有表访问记录
    val allTables = getRequestTablesWithinDuration(endTime, durationInSeconds)
    
    if (allTables.isEmpty) return List.empty
    
    // 计算需要采样的数量，向上取整以确保至少有一个样本
    val sampleSize = (allTables.size * sampleRate).ceil.toInt
    
    // 随机打乱并取前 sampleSize 个元素
    val sampledTables = scala.util.Random.shuffle(allTables).take(sampleSize)
    
    logging.info(this, s"时间窗口 [${endTime.minus(durationInSeconds, ChronoUnit.SECONDS)} -> $endTime] " +
      s"采样率 $sampleRate: 原始数据量 ${allTables.size}, 采样后数量 ${sampledTables.size}")
    logging.info(this, s"采样后的表访问情况: $sampledTables")
    sampledTables
  }

  // 清理超出指定时间窗口的记录（单位为分钟）
  def cleanUpOldRecords(maxAgeInMinutes: Long)(implicit logging: Logging): Unit = {
    val currentTime = Instant.now()
    val expirationKey = currentTime.minus(maxAgeInMinutes, ChronoUnit.MINUTES).getEpochSecond / 60

    requestBuckets.synchronized {
      val initialSize = requestBuckets.size
      requestBuckets.retain((key, _) => key >= expirationKey)
      val finalSize = requestBuckets.size
      if (finalSize < initialSize){
      logging.info(this, s"清理早期请求记录. 记录桶数 from $initialSize to $finalSize.")
      }
    }
    waitAndColdRequestBuckets.synchronized {
      val initialSize = requestBuckets.size
      requestBuckets.retain((key, _) => key >= expirationKey)
      val finalSize = requestBuckets.size
      if (finalSize < initialSize){
      logging.info(this, s"清理早期请求记录. 记录桶数 from $initialSize to $finalSize.")
      }
    }
  }
}

// JSON 格式支持
object InvokerJsonProtocol extends DefaultJsonProtocol {
  implicit val invokerFormat = jsonFormat1(InvokerIP)
}

case class InvokerIP(invoker0IP: String)
// 容器管理
// 如果放在WarmContainerManager.scala中，因为它属于invoker，会无法与调度器通信
object ContainerRemoveManager {
  import InvokerJsonProtocol._
    
  // 必须新建使用独立的 Akka 配置，否则日子中报错端口被占用
  // 配置独立的 Akka 系统配置
  val customConfig = ConfigFactory.parseString(
    """
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

  implicit val system: ActorSystem = ActorSystem("WarmContainerManagerHttp", customConfig)

  // 定期任务的调度器
  private val scheduler: ScheduledExecutorService = Executors.newSingleThreadScheduledExecutor()
  // 可以使用 docker inspect -f '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}' invoker0 检查 invoker0 的ip

  // 初始化 invoker0IP
  private var invoker0IP = ""
  
  
  // 定期检查间隔（Duration：秒）
  private var containerCheckIntervalInSec = RequestRecordManager.containerCheckIntervalInSec
  private var validStates = RequestRecordManager.validStatesWhenKM
  private val checkInterval = containerCheckIntervalInSec.seconds

  // 定时启动定期检查任务
  def init()(implicit logging: Logging): Unit = {
    ServerForReceiveInvokerIP()

    // 启动定期检查任务
    scheduler.scheduleAtFixedRate(
      new Runnable {
        override def run(): Unit = Try {
          checkRequestTrends()
        }.recover {
          case e: Exception =>
            logging.error(this, s"Error during periodic request trend check: ${e.getMessage}")
        }
      },
      checkInterval.toMillis, // 初始延迟
      checkInterval.toMillis, // 周期时间
      TimeUnit.MILLISECONDS
    )
  }

  // 启动服务，接收并记录由invoker传来的invoker IP
  private def ServerForReceiveInvokerIP()(implicit logging: Logging): Unit = {
    // 定义路由
    val route =
      path("updateInvokerIP") {
        post {
          entity(as[String]) { jsonBody =>
            try {
              val invokerIP = jsonBody.parseJson.convertTo[InvokerIP]
              invoker0IP = invokerIP.invoker0IP // 更新全局变量
              logging.info(this, s"Received invoker0IP: $invoker0IP")
              complete(HttpResponse(StatusCodes.OK, entity = "Invoker IP updated successfully"))
            } catch {
              case ex: Exception =>
                logging.error(this, s"Failed to parse invoker0IP: ${ex.getMessage}")
                complete(HttpResponse(StatusCodes.BadRequest, entity = "Invalid JSON format"))
            }
          }
        }
      }

    // 启动 HTTP 服务器
    Http().newServerAt("0.0.0.0", 6788).bind(route).map { binding =>
      logging.info(this, s"Server started at ${binding.localAddress}")
    }.recover {
      case ex: Exception =>
        logging.error(this, "Failed to start server: ${ex.getMessage}")
    }
  }

  // 检查前段时间窗口内的请求，计算请求趋势
  private def checkRequestTrends()(implicit logging: Logging): Unit = {
    val now = Instant.now()

    val requestCountLastxSecs = RequestRecordManager.getRequestCountWithinDuration(now, containerCheckIntervalInSec)
    val requestCountLastxto2xSecsAgo = RequestRecordManager.getRequestCountWithinDuration(now.minus(containerCheckIntervalInSec, ChronoUnit.SECONDS), containerCheckIntervalInSec)
    val requestCountLast2xto3xSecsAgo = RequestRecordManager.getRequestCountWithinDuration(now.minus(2 * containerCheckIntervalInSec, ChronoUnit.SECONDS), containerCheckIntervalInSec)
    if (requestCountLastxSecs + requestCountLastxto2xSecsAgo + requestCountLast2xto3xSecsAgo == 0) {
      logging.info(this, "过去一段时间内没有请求，跳过容器检查")
      return
    }
    logging.info(this, s"过去 $containerCheckIntervalInSec s的请求数量: $requestCountLastxSecs; " +
      s"过去 $containerCheckIntervalInSec 到 ${2 * containerCheckIntervalInSec} s的请求数量: $requestCountLastxto2xSecsAgo; " +
      s"过去 ${2 * containerCheckIntervalInSec} 到 ${3 * containerCheckIntervalInSec} s的请求数量: $requestCountLast2xto3xSecsAgo")

    val waitAndColdRequestCountLastxSecs = RequestRecordManager.getWaitAndColdRequestCountWithinDuration(now, containerCheckIntervalInSec)
    val waitAndColdRequestCountLastxto2xSecsAgo = RequestRecordManager.getWaitAndColdRequestCountWithinDuration(now.minus(containerCheckIntervalInSec, ChronoUnit.SECONDS), containerCheckIntervalInSec)
    val waitAndColdRequestCountLast2xto3xSecsAgo = RequestRecordManager.getWaitAndColdRequestCountWithinDuration(now.minus(2 * containerCheckIntervalInSec, ChronoUnit.SECONDS), containerCheckIntervalInSec)

    if (waitAndColdRequestCountLastxSecs > 0 || waitAndColdRequestCountLastxto2xSecsAgo > 0 || waitAndColdRequestCountLast2xto3xSecsAgo > 0){
      // 全 0 情况不打印日志
      logging.info(this, s"过去 $containerCheckIntervalInSec s的等待和冷启动请求数量: $waitAndColdRequestCountLastxSecs")
      logging.info(this, s"过去 $containerCheckIntervalInSec 到 ${2 * containerCheckIntervalInSec} s的等待和冷启动请求数量: $waitAndColdRequestCountLastxto2xSecsAgo")
      logging.info(this, s"过去 ${2 * containerCheckIntervalInSec} 到 ${3 * containerCheckIntervalInSec} s的等待和冷启动请求数量: $waitAndColdRequestCountLast2xto3xSecsAgo")

      // val requestTablesLastxSecs = RequestRecordManager.getRequestTablesWithinDuration(now, containerCheckIntervalInSec)
      // logging.info(this, s"过去 $containerCheckIntervalInSec s的表访问情况: $requestTablesLastxSecs")

      // 分别从三个时间窗口采样并合并结果
      val recentTables = RequestRecordManager.getSampleRequestTablesWithinDuration(now, containerCheckIntervalInSec, 0.7)
      val middleTables = RequestRecordManager.getSampleRequestTablesWithinDuration(now.minus(containerCheckIntervalInSec, ChronoUnit.SECONDS),containerCheckIntervalInSec,0.2)
      val oldestTables = RequestRecordManager.getSampleRequestTablesWithinDuration(now.minus(2 * containerCheckIntervalInSec, ChronoUnit.SECONDS),containerCheckIntervalInSec,0.1)
      
      // 合并三个时间窗口的采样结果
      val allSampledTables = recentTables ++ middleTables ++ oldestTables
      logging.info(this, s"三个时间窗口采样后的表访问情况: $allSampledTables")

      val warmContainerCount = ContainerDbInfoManager.countEntriesByState("warm")
      val workingContainerCount = ContainerDbInfoManager.countEntriesByState("working")
      val prewarmContainerCount = ContainerDbInfoManager.countEntriesByState("prewarm")
      
      logging.info(this, s"当前系统中的容器状态: warm: $warmContainerCount, working: $workingContainerCount, prewarm: $prewarmContainerCount")
      if (waitAndColdRequestCountLastxSecs >= waitAndColdRequestCountLastxto2xSecsAgo && waitAndColdRequestCountLastxto2xSecsAgo >= waitAndColdRequestCountLast2xto3xSecsAgo && warmContainerCount + prewarmContainerCount + workingContainerCount < waitAndColdRequestCountLastxSecs) {
        // 前x秒的请求 > 2x-3x的请求，并且x-2x的请求在中间（允许相等）
        logging.info(this, "到达的请求数呈上升趋势, 使用热度较高的表预热一些容器...")
        // preWarmSomeContainers(requestTablesLastxSecs)
      } else if (warmContainerCount + prewarmContainerCount > requestCountLastxSecs && warmContainerCount + prewarmContainerCount > requestCountLastxto2xSecsAgo && warmContainerCount + prewarmContainerCount > requestCountLast2xto3xSecsAgo){
        logging.info(this, "系统中存在较多热容器, 提前释放一些热容器...")
        RequestRecordManager.removeStrategy.toLowerCase() match {
          case "best" => removeSomeWarmContainers(allSampledTables)
          case "random" => removeRandomWarmContainers(allSampledTables)
          case "redundancy" => removeWarmContainersByRedundancy(allSampledTables)
          case "none" => logging.info(this, "不执行容器移除策略")
          case _ =>  throw new IllegalArgumentException(s"未知的容器移除策略: ${RequestRecordManager.removeStrategy}")
        }
      } else {
        logging.info(this, "到达的请求数波动持平")
      }
    }
  }

  private def preWarmSomeContainers(recentRequestTables: List[List[String]])(implicit logging: Logging): Unit = {
    if (recentRequestTables.isEmpty) {
      logging.warn(this, "近期的请求记录为空，跳过预热")
      return
    }

    logging.info(this, s"尝试预热一些新容器, recentRequestTables: $recentRequestTables")
    val dbInfo = ContainerDbInfoManager.getDbInfo()
    val containers = dbInfo.values.filter(info => validStates.contains(info.state)).toList
    
    // 构造代价矩阵
    val (costMatrix, containerOrder) = KM.buildCostMatrix(recentRequestTables, containers)
    
    // 运行 KM 算法，获取匹配结果
    val (matchedPairs, _) = KM.kuhnMunkres(costMatrix, recentRequestTables, containerOrder)
    
    // 找出未匹配的请求
    val unmatchedRequests = (0 until recentRequestTables.length).filter(!matchedPairs.contains(_)).toList
    
    logging.info(this, s"KM算法匹配结果 - 匹配成功: $matchedPairs, 未匹配请求索引: $unmatchedRequests")

    // 处理未匹配的请求
    if (unmatchedRequests.nonEmpty) {
      // 为每个未匹配的请求找出最大的表并预热
      unmatchedRequests.foreach { requestIdx =>
        val largestTable = findLargestTableFromRequest(requestIdx, recentRequestTables)
        logging.info(this, s"为未匹配请求 $requestIdx 预热新容器，使用表: $largestTable")
        PrewarmManager.sendPrewarmRequest(List(largestTable))
      }
    } else {
      logging.info(this, "所有请求都已匹配到现有容器，无需预热新容器")
    }

  }

  /**
   * 从单个请求中找出最大的表
   */
  private def findLargestTableFromRequest(requestIdx: Int, recentRequestTables: List[List[String]])(implicit logging: Logging): String = {
    val tables = recentRequestTables(requestIdx)
    val largestTable = tables.maxBy(table => TimePredictor.TargetBenchmark.getOrElse(table, 0L))
    logging.info(this, s"请求 $requestIdx 的表: [${tables.mkString(", ")}], 选择最大的表: $largestTable (大小: ${TimePredictor.TargetBenchmark.getOrElse(largestTable, 0L)})")
    largestTable
  }

  // 移除一些热容器
  private def removeSomeWarmContainers(recentRequestTables: List[List[String]])(implicit logging: Logging): Unit = {
    logging.info(this, s"尝试移除一些热容器, recentRequestTables: $recentRequestTables")
    val dbInfo = ContainerDbInfoManager.getDbInfo()
    val containers = dbInfo.values.filter(info => validStates.contains(info.state)).toList

    // 如果没有请求或容器，直接返回
    if (recentRequestTables.isEmpty || containers.isEmpty) {
      logging.warn(this, s"容器列表 $containers 或请求列表 $recentRequestTables 为空！")
      return
    }

    // 构造代价矩阵
    val (costMatrix, containerOrder) = KM.buildCostMatrix(recentRequestTables, containers)

    // 运行 Kuhn-Munkres 算法，获取匹配结果和未匹配的容器索引
    val (matchedPairs, unmatchedContainers) = KM.kuhnMunkres(costMatrix, recentRequestTables, containerOrder)
    logging.info(this, s"匹配成功: $matchedPairs, 未匹配的容器: $unmatchedContainers")

    // 过滤冗余的容器，仅保留状态为 "warm" 的容器
    val warmContainersToRemove = unmatchedContainers
      .map(containerOrder) // 将索引转换为容器ID
      .flatMap(containerId => containers.find(_.containerId == containerId)) // 根据ID查找容器信息
      .filter(_.state == "warm") // 仅保留 warm 状态的容器

    // 输出一些日志，包括容器内表的信息等
    {
      // 输出所有容器的信息，方便调试
      logging.info(this, "==== Debug Information: 所有容器 ====")
      containers.foreach { container =>
        val tablesInfo = container.tables.mkString(", ")
        logging.info(this, s"Container ID: ${container.containerId}, State: ${container.state}, Tables: [$tablesInfo]")
      }

      // 输出决定移除的容器的信息
      if (warmContainersToRemove.nonEmpty) {
        logging.info(this, "==== Debug Information: 容器决策 ====")
        warmContainersToRemove.foreach { container =>
          val tablesInfo = container.tables.mkString(", ")
          logging.info(this, s"移除 -> Container ID: ${container.containerId}, Tables: [$tablesInfo]")
        }

        // 输出未移除的容器的信息
        val remainingContainers = containers.filterNot(c => warmContainersToRemove.exists(_.containerId == c.containerId))
        remainingContainers.foreach { container =>
          val tablesInfo = container.tables.mkString(", ")
          logging.info(this, s"保留 -> Container ID: ${container.containerId}, State: ${container.state}, Tables: [$tablesInfo]")
        }
        logging.info(this, "=====================================")
      }
    }
    // 如果没有冗余的 warm 容器需要移除，记录日志并返回
    if (warmContainersToRemove.isEmpty) {
      logging.warn(this, "没有冗余的 warm 容器需要移除")
      return
    }

    // 移除 warm 容器并记录日志
    warmContainersToRemove.foreach { container =>
      logging.info(this, s"正在移除热容器: ${container.containerId}")
      removeContainerById(container.containerId)
    }

    // 记录完成信息
    logging.info(this, s"移除了 ${warmContainersToRemove.size} 个热容器")
  }

  // 随机移除热容器（用于对照实验）
  private def removeRandomWarmContainers(recentRequestTables: List[List[String]])(implicit logging: Logging): Unit = {
    val dbInfo = ContainerDbInfoManager.getDbInfo()
    val containers = dbInfo.values.filter(info => validStates.contains(info.state)).toList

    // 如果没有请求或容器，直接返回
    if (recentRequestTables.isEmpty || containers.isEmpty) {
      logging.warn(this, s"容器列表 $containers 或请求列表 $recentRequestTables 为空！")
      return
    }

    logging.info(this, s"尝试随机移除热容器, recentRequestTables: $recentRequestTables")
    // 仅获取warm状态的容器
    val warmContainers = dbInfo.values.filter(_.state == "warm").toList

    // 如果没有warm容器，直接返回
    if (warmContainers.isEmpty) {
      logging.warn(this, "没有warm容器可供移除")
      return
    }

    // 首先运行原始算法来获取要移除的容器数量
    val dbInfoCopy = dbInfo.values.filter(info => info.state == "warm" || info.state == "working").toList
    val (costMatrix, containerOrder) = KM.buildCostMatrix(recentRequestTables, dbInfoCopy)
    val (matchedPairs, unmatchedContainers) = KM.kuhnMunkres(costMatrix, recentRequestTables, containerOrder)
    val originalRemovalCount = unmatchedContainers
      .map(containerOrder)
      .flatMap(containerId => dbInfoCopy.find(_.containerId == containerId))
      .count(_.state == "warm")

    // 如果原算法不需要移除任何容器，我们也直接返回
    if (originalRemovalCount == 0) {
      logging.warn(this, "原算法无需移除容器，随机算法也不执行移除")
      return
    }

    // 随机选择相同数量的warm容器
    val shuffledContainers = scala.util.Random.shuffle(warmContainers)
    val containersToRemove = shuffledContainers.take(originalRemovalCount)

    // 输出调试信息
    {
      logging.info(this, "==== Debug Information: 随机移除决策 ====")
      logging.info(this, s"原算法决定移除的容器数量: $originalRemovalCount")
      
      // 输出所有warm容器信息
      logging.info(this, "当前所有warm容器:")
      warmContainers.foreach { container =>
        val tablesInfo = container.tables.mkString(", ")
        logging.info(this, s"Container ID: ${container.containerId}, Tables: [$tablesInfo]")
      }

      // 输出将要移除的容器信息
      logging.info(this, "即将随机移除的容器:")
      containersToRemove.foreach { container =>
        val tablesInfo = container.tables.mkString(", ")
        logging.info(this, s"移除 -> Container ID: ${container.containerId}, Tables: [$tablesInfo]")
      }

      // 输出将要保留的容器信息
      val remainingContainers = warmContainers.filterNot(c => containersToRemove.exists(_.containerId == c.containerId))
      logging.info(this, "保留的容器:")
      remainingContainers.foreach { container =>
        val tablesInfo = container.tables.mkString(", ")
        logging.info(this, s"保留 -> Container ID: ${container.containerId}, Tables: [$tablesInfo]")
      }
      logging.info(this, "=====================================")
    }

    // 执行移除操作
    containersToRemove.foreach { container =>
      logging.info(this, s"正在移除热容器: ${container.containerId}")
      removeContainerById(container.containerId)
    }

    // 记录完成信息
    logging.info(this, s"随机移除了 ${containersToRemove.size} 个热容器")
  }

  // 按照容器冗余表数计算并移除（用于对照实验）
  private def removeWarmContainersByRedundancy(recentRequestTables: List[List[String]])(implicit logging: Logging): Unit = {
    val dbInfo = ContainerDbInfoManager.getDbInfo()
    val containers = dbInfo.values.filter(info => validStates.contains(info.state)).toList

    if (recentRequestTables.isEmpty || containers.isEmpty) {
      logging.warn(this, s"空容器列表 $containers 或请求列表 $recentRequestTables 为空！")
      return
    }

    val requestTableCounts = recentRequestTables.flatten.groupBy(identity).map { case (k, v) => k -> v.size }
    val containerTableCounts = containers.flatMap(_.tables).groupBy(identity).map { case (k, v) => k -> v.size }

    val redundantTableCounts = containerTableCounts.map { case (table, count) =>
      table -> (count - requestTableCounts.getOrElse(table, 0)).max(0)
    }

    val warmContainersWithScore = containers
      .filter(_.state == "warm")
      .map { container =>
        container -> container.tables.map(table => redundantTableCounts.getOrElse(table, 0)).sum
      }
      .sortBy(-_._2)

    val (costMatrix, containerOrder) = KM.buildCostMatrix(recentRequestTables, containers)
    val (_, unmatchedContainers) = KM.kuhnMunkres(costMatrix, recentRequestTables, containerOrder)
    val originalRemovalCount = unmatchedContainers
      .map(containerOrder)
      .flatMap(containerId => containers.find(_.containerId == containerId))
      .count(_.state == "warm")

    val warmContainersToRemove = warmContainersWithScore.take(originalRemovalCount).map(_._1)

    if (warmContainersToRemove.nonEmpty) {
      logging.info(this, "==== Debug Information: 容器决策 ====")
      warmContainersToRemove.foreach { container =>
        logging.info(this, s"移除 -> Container ID: ${container.containerId}, Tables: [${container.tables.mkString(", ")}]")
      }

      val remainingContainers = containers.filterNot(c => warmContainersToRemove.exists(_.containerId == c.containerId))
      remainingContainers.foreach { container =>
        logging.info(this, s"保留 -> Container ID: ${container.containerId}, State: ${container.state}, Tables: [${container.tables.mkString(", ")}]")
      }
      logging.info(this, "=====================================")
    } else {
      logging.warn(this, "没有冗余的 warm 容器需要移除")
      return
    }

    warmContainersToRemove.foreach { container =>
      logging.info(this, s"正在移除热容器: ${container.containerId}")
      removeContainerById(container.containerId)
    }

    logging.info(this, s"移除了 ${warmContainersToRemove.size} 个热容器")
  }

  // 移除指定id的容器
  private def removeContainerById(containerIdToRemove: String)(implicit logging: Logging): Unit = {
    // 锁定该容器
    ContainerDbInfoManager.updateStateToLocked(containerIdToRemove)

    val removeRequest = s"""{"containerId": "$containerIdToRemove"}"""
    val httpRequest = HttpRequest(
      method = HttpMethods.POST,
      uri = s"http://$invoker0IP:5678/warmContainerManager/removeById",
      entity = HttpEntity(ContentTypes.`application/json`, removeRequest)
    )

    Http().singleRequest(httpRequest).onComplete {
      case Success(response) =>
        logging.info(this, s"成功发送移除请求, 容器ID: $containerIdToRemove")
      case Failure(exception) =>
        logging.error(this, s"发送移除请求失败, 容器ID: $containerIdToRemove, 错误信息: ${exception.getMessage}")
    }
  }

}

// Kuhn-Munkres算法
object KM{
  /**
   * 生成代价矩阵
   *
   * @param requests 请求列表，每个请求为需要的表集合
   * @param containers 容器信息列表
   * @return (代价矩阵, 容器ID列表)
   */
  def buildCostMatrix(requests: List[List[String]], containers: List[ContainerDbInfo])(implicit logging: Logging): (Array[Array[Double]], List[String]) = {
    val n = requests.length // 请求数
    val m = containers.length // 容器数
    logging.info(this, s"正在构造代价矩阵，请求数: $n 总容器数: $m")

    val containerOrder = containers.map(_.containerId) // 容器ID的顺序
    val validContainers = scala.collection.mutable.ArrayBuffer[String]() // 执行期间未变化的容器ID列表

    // 初始化代价矩阵
    val costMatrix = Array.ofDim[Double](n, m)

    // 遍历每个请求，调用 TimePredictor 计算代价
    for (i <- requests.indices) {
      val tablesNeeded = requests(i)
      val costs = TimePredictor.predictWaitTime(tablesNeeded) // 调用预测时间
      logging.info(this, s"Request $i costs: $costs")

      // 填充代价矩阵
      for ((containerId, timeCost, _) <- costs) {
        val containerIndex = containerOrder.indexOf(containerId)
        if (containerIndex != -1) {
          costMatrix(i)(containerIndex) = timeCost
          // 只记录未变化的容器
          if (!validContainers.contains(containerId)) {
            validContainers += containerId
          }
        }else {
        logging.warn(this, s"Container $containerId not found in system containers.")
        }
      }
    }

    // 根据有效容器重新生成代价矩阵
    val finalMatrix = Array.ofDim[Double](n, validContainers.length)
    for (i <- requests.indices) {
      for (j <- validContainers.indices) {
        // 复制有效列的数据
        val originalIndex = containerOrder.indexOf(validContainers(j))
        finalMatrix(i)(j) = costMatrix(i)(originalIndex)
      }
    }
    
    // 打印完整的代价矩阵
    logging.info(this, s"Cost matrix:")
    finalMatrix.foreach(row => logging.info(this, row.mkString(" ")))

    (finalMatrix, validContainers.toList)
  }

  /**
   * Kuhn-Munkres算法
   */
  def kuhnMunkres(costMatrix: Array[Array[Double]], requests: List[List[String]], containerOrder: List[String])(implicit logging: Logging): (Map[Int, Int], Set[Int]) = {
    val n = costMatrix.length    
    val m = costMatrix(0).length 
    
    logging.info(this, s"开始KM算法匹配, 请求数:$n, 容器数:$m")

    // 处理边界情况
    if (m == 0) {
      // 没有容器时，所有请求均未匹配
      logging.info(this, "系统中没有可用容器，所有请求均未匹配")
      return (Map.empty[Int, Int], Set.empty[Int])
    }
    
    if (n == 0) {
      // 没有请求时，所有容器均未匹配
      logging.info(this, "没有待处理的请求，所有容器均未匹配")
      return (Map.empty[Int, Int], (0 until m).toSet)
    }
    
    // 输出请求和容器的详细信息
    logging.info(this, "请求信息:")
    requests.zipWithIndex.foreach { case (tables, idx) =>
      logging.info(this, s"  请求 $idx: 需要表 [${tables.mkString(", ")}]")
    }
    logging.info(this, "容器信息:")
    containerOrder.zipWithIndex.foreach { case (containerId, idx) =>
      logging.info(this, s"  容器 $idx: ID: $containerId tables: [${ContainerDbInfoManager.findTablesByContainerId(containerId).mkString(", ")}]")
    }
    
    // 转换代价矩阵以处理最小化问题
    val maxCost = costMatrix.map(_.max).max
    val transformedCostMatrix = Array.ofDim[Double](n, m)
    for (i <- costMatrix.indices; j <- costMatrix(i).indices) {
      transformedCostMatrix(i)(j) = maxCost - costMatrix(i)(j)
    }
    
    // 初始化标号
    val labelX = Array.fill(n)(0.0)
    val labelY = Array.fill(m)(0.0)
    
    // 初始化顶标
    for (i <- 0 until n) {
      labelX(i) = transformedCostMatrix(i).max
    }
    
    // 初始化匹配数组
    val matchY = Array.fill(m)(-1)   
    
    // 为每个请求寻找匹配
    for (i <- 0 until n) {
      logging.info(this, s"正在为请求 $i (表: [${requests(i).mkString(", ")}]) 寻找匹配")
      
      var visited = Array.fill(m)(false)
      var iterationCount = 0
      val maxIterations = n * m * 2  // 设置最大迭代次数
      
      // 为当前请求寻找增广路径
      def findAugmentingPath(i: Int): Boolean = {
        for (j <- 0 until m if !visited(j)) {
          if (math.abs(labelX(i) + labelY(j) - transformedCostMatrix(i)(j)) < 1e-10) {
            visited(j) = true
            
            if (matchY(j) == -1 || findAugmentingPath(matchY(j))) {
              matchY(j) = i
              return true
            }
          }
        }
        false
      }
      
      breakable {
        // 当找不到增广路径时，调整标号
        while (!findAugmentingPath(i)) {
          iterationCount += 1
          if (iterationCount > maxIterations) {
            logging.warn(this, s"请求 $i 超过最大迭代次数 $maxIterations，跳过该请求")
            break
          }
          
          var delta = Double.MaxValue
          
          // 计算调整量
          for (j <- 0 until m if !visited(j)) {
            val currentDelta = labelX(i) + labelY(j) - transformedCostMatrix(i)(j)
            if (!currentDelta.isInfinite && currentDelta < delta) {
              delta = currentDelta
            }
          }
          
          // 检查delta的有效性
          if (delta.isInfinite || delta == Double.MaxValue) {
            logging.warn(this, s"请求 $i 无法找到有效的调整量，跳过该请求")
            break
          }
          
          if (delta < 1e-10) {
            logging.warn(this, s"请求 $i 的调整量 $delta 太小，可能导致无效调整，跳过该请求")
            break
          }
          
          // 记录调整前的值
          val oldLabelX = labelX(i)
          
          // 调整标号
          labelX(i) -= delta
          for (j <- 0 until m if visited(j)) {
            labelY(j) += delta
          }
          
          // 检查调整是否有效
          if (math.abs(oldLabelX - labelX(i)) < 1e-10) {
            logging.warn(this, s"请求 $i 的标号调整无效，跳过该请求")
            break
          }
          
          // 重置访问标记
          visited = Array.fill(m)(false)
        }
      }
    }
    
    // 构造返回结果并添加详细的匹配信息
    val matchedPairs = matchY.zipWithIndex
      .filter(_._1 != -1)
      .map(p => (p._1, p._2))
      .toMap
    
    val unmatchedContainers = (0 until m)
      .filter(j => matchY(j) == -1)
      .toSet
    
    // 输出更详细的调试信息
    logging.info(this, s"标号X: ${labelX.mkString(", ")}")
    logging.info(this, s"标号Y: ${labelY.mkString(", ")}")
    logging.info(this, s"匹配结果Y: ${matchY.mkString(", ")}")
    logging.info(this, "最终匹配详情:")
    matchedPairs.foreach { case (reqIdx, contIdx) =>
      logging.info(this, s"  请求 $reqIdx (表: [${requests(reqIdx).mkString(", ")}]) -> 容器 $contIdx (ID: ${containerOrder(contIdx)})")
    }
    logging.info(this, "未匹配容器详情:")
    unmatchedContainers.foreach { contIdx =>
      logging.info(this, s"  容器 $contIdx (ID: ${containerOrder(contIdx)})")
    }
    
    (matchedPairs, unmatchedContainers)
  }
}