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
  // 采样时间窗口参数
  val containerCheckIntervalInSec =3
  val totalTimeWindowInSec = 9

  // 判定请求趋势时，滑动时间窗口参数
  val slidingWindowSizeInSec = 60  // 时间窗口大小（单位：秒）
  val slidingStepSizeInSec = 5  // 滑动步长（单位：秒）
  val triggerThreshold = 12  // 触发条件：连续多少个滑动窗口下降或持平

  // 定义采样比例数组
  val samplingRatios = Array(0.7, 0.2, 0.1)

  // var validStatesWhenKM = List("warm")
  val validStatesWhenKM = List("warm", "working")

  val removeStrategy = "km"
  // "none" 、 "km" 、 "random" 、 "redundancy" 、 "total_voc" 、 "voc" 、 "svoc"

  // 最低保留热容器数
  val leastSaveContainers = 7

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
  private var removeStrategy = RequestRecordManager.removeStrategy
  private var containerCheckIntervalInSec = RequestRecordManager.containerCheckIntervalInSec
  private var totalTimeWindowInSec = RequestRecordManager.totalTimeWindowInSec 
  private var samplingRatios = RequestRecordManager.samplingRatios
  private var validStates = RequestRecordManager.validStatesWhenKM
  private var leastSaveContainers = RequestRecordManager.leastSaveContainers
  private var slidingWindowSizeInSec = RequestRecordManager.slidingWindowSizeInSec 
  private var slidingStepSizeInSec = RequestRecordManager.slidingStepSizeInSec
  private var triggerThreshold = RequestRecordManager.triggerThreshold

  private val checkInterval = containerCheckIntervalInSec.seconds

  // 总主动移除容器数
  private var proactiveRemovedContainerCount: Int = 0

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

    // 向日志中记录本次的参数配置
    logging.warn(this, s"主动回收策略: ${removeStrategy}")
    logging.warn(this, s"采样比例数组: ${samplingRatios.mkString(", ")}")
    logging.warn(this, s"容器检查间隔: $containerCheckIntervalInSec s")
    logging.warn(this, s"总时间窗口大小: $totalTimeWindowInSec s")
    logging.warn(this, s"最低保留热容器数: $leastSaveContainers")
    logging.warn(this, s"滑动窗口大小: $slidingWindowSizeInSec s, 滑动步长: $slidingStepSizeInSec s, 触发阈值: $triggerThreshold")

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
    val dbInfo = ContainerDbInfoManager.getDbInfo()
    val warmContainers = dbInfo.values.filter(_.state == "warm").toList

    // 热容器数小于最低保留热容器数，直接返回
    if (warmContainers.length <= leastSaveContainers) {
      logging.debug(this, s"热容器数量 ${warmContainers.length} 小于等于最低保留热容器数 ${leastSaveContainers}，跳过移除操作")
      return
    }

    val now = Instant.now()
    // 保持原有的固定时间窗口逻辑
    val windowSize = containerCheckIntervalInSec
    val numWindows = Math.max(1, totalTimeWindowInSec / windowSize)
    logging.info(this, s"总时间窗口大小: $totalTimeWindowInSec s, 时间窗口大小: $windowSize s, 时间窗口数量: $numWindows")
    
    // 验证采样比例数组长度是否与窗口数量匹配
    if (samplingRatios.length != numWindows) {
      val errorMsg = s"采样比例数组长度(${samplingRatios.length})与窗口数量($numWindows)不匹配"
      logging.error(this, errorMsg)
      throw new IllegalArgumentException(errorMsg)
    }

    // 收集各固定时间窗口的请求数据（用于条件1判断）
    val requestCounts = new Array[Int](numWindows)

    // 收集每个窗口的请求数据
    for (i <- 0 until numWindows) {
      val windowStart = now.minus(i * windowSize, ChronoUnit.SECONDS)
      requestCounts(i) = RequestRecordManager.getRequestCountWithinDuration(windowStart, windowSize)
    }
    
    // 检查是否有请求
    if (requestCounts.sum == 0) {
      logging.info(this, "过去一段时间内没有请求，跳过容器检查")
      return
    }

    // 输出每个时间窗口的请求信息
    for (i <- 0 until numWindows) {
      val startTime = i * windowSize
      val endTime = (i + 1) * windowSize
      logging.info(this, s"过去 $startTime 到 $endTime s的请求数量: ${requestCounts(i)}")
    }

    // 使用新的采样比例数组进行采样
    val sampledTables = Array.ofDim[List[List[String]]](numWindows)
    
    // 对每个窗口进行采样
    for (i <- 0 until numWindows) {
      // 计算窗口结束时间（当前时间减去窗口开始时间）
      val windowEnd = now.minus(i * windowSize, ChronoUnit.SECONDS)
      sampledTables(i) = RequestRecordManager.getSampleRequestTablesWithinDuration(
        windowEnd, windowSize, samplingRatios(i))
    }
    
    // 合并所有采样结果
    val allSampledTables = sampledTables.flatten.toList
    logging.info(this, s"采样后的表访问情况: $allSampledTables")

    // ===== 滑动时间窗口逻辑（仅用于条件2的趋势判断） =====
    logging.info(this, s"滑动窗口大小: $slidingWindowSizeInSec s, 滑动步长: $slidingStepSizeInSec s, 触发阈值: $triggerThreshold")
    
    // 定义滑动窗口数量 = triggerThreshold
    val numSlidingWindows = triggerThreshold
    
    // 收集各滑动时间窗口的请求数据
    val slidingRequestCounts = new Array[Int](numSlidingWindows)
    
    // 收集每个滑动窗口的请求数据
    for (i <- 0 until numSlidingWindows) {
      // 计算窗口的起始时间：现在减去(窗口起始偏移)
      val windowStartOffset = i * slidingStepSizeInSec
      val windowStart = now.minus(windowStartOffset + slidingWindowSizeInSec, ChronoUnit.SECONDS)
      
      // 获取该滑动窗口内的请求数量
      slidingRequestCounts(i) = RequestRecordManager.getRequestCountWithinDuration(windowStart, slidingWindowSizeInSec)
    }

    // 输出每个滑动窗口的请求信息
    for (i <- 0 until numSlidingWindows) {
      val startTime = i * slidingStepSizeInSec
      val endTime = startTime + slidingWindowSizeInSec
      logging.info(this, s"滑动窗口 $i (过去 $startTime 到 $endTime s) 的请求数量: ${slidingRequestCounts(i)}")
    }

    val warmContainerCount = ContainerDbInfoManager.countEntriesByState("warm")
    val workingContainerCount = ContainerDbInfoManager.countEntriesByState("working")
    val prewarmContainerCount = ContainerDbInfoManager.countEntriesByState("prewarm")
    
    logging.warn(this, s"当前系统中的容器状态: warm: $warmContainerCount, working: $workingContainerCount, prewarm: $prewarmContainerCount")

    // 判断是否存在过多的热容器
    // 条件1：所有时间窗口的请求量都小于热容器数量加预热容器数量（保持原逻辑不变）
    val allWindowsHaveExcessContainers = requestCounts.forall(count => warmContainerCount + prewarmContainerCount > count)

    // 条件2：滑动窗口的请求趋势判断（新增的滑动窗口逻辑）
    // 检查所有相邻窗口对是否满足：前一个窗口(较新)的请求量不大于后一个窗口(较旧)的请求量
    var hasDecreasingTrend = true
    for (i <- 0 until slidingRequestCounts.length - 1) {
      // 比较相邻窗口：slidingRequestCounts(i)为较新窗口，slidingRequestCounts(i+1)为较旧窗口
      if (slidingRequestCounts(i) > slidingRequestCounts(i + 1)) {
        hasDecreasingTrend = false
      }
    }

    // // 条件2：使用线性回归模型拟合请求趋势，斜率小于等于-1才触发
    // // 准备数据用于线性回归：x为窗口索引，y为请求数量
    // val xValues = (0 until numSlidingWindows).map(_.toDouble).toArray
    // val yValues = slidingRequestCounts.map(_.toDouble)
    // // 计算线性回归的斜率
    // // 使用最小二乘法计算斜率：slope = (n*sum(xy) - sum(x)*sum(y)) / (n*sum(x^2) - (sum(x))^2)
    // val n = xValues.length
    // val sumX = xValues.sum
    // val sumY = yValues.sum
    // val sumXY = (xValues zip yValues).map { case (x, y) => x * y }.sum
    // val sumXSquare = xValues.map(x => x * x).sum
    // val slope = (n * sumXY - sumX * sumY) / (n * sumXSquare - sumX * sumX)
    // // 记录斜率值
    // logging.info(this, s"请求趋势的线性拟合斜率: $slope")
    // // 只有当斜率小于等于-1时才认为有明显的下降趋势
    // val hasDecreasingTrend = slope <= -1.0


    // 条件3：最近的时间窗口中没有冷启动或等待启动的请求（保持原逻辑不变）
    val waitAndColdRequestCountLastxSecs = RequestRecordManager.getWaitAndColdRequestCountWithinDuration(now, containerCheckIntervalInSec)
    val noRecentColdOrWaitingStarts = waitAndColdRequestCountLastxSecs == 0

    // 同时满足三个条件
    val hasExcessContainers = allWindowsHaveExcessContainers && hasDecreasingTrend && noRecentColdOrWaitingStarts
    
    // 记录判断条件的日志
    logging.info(this, s"判断条件: 容器数量过多=$allWindowsHaveExcessContainers, 请求下降趋势=$hasDecreasingTrend, 无冷启动请求=$noRecentColdOrWaitingStarts")
    
    // 如果有过多的热容器，则释放一些
    if (hasExcessContainers) {
      logging.info(this, "系统中存在较多热容器, 提前释放一些热容器...")
      val removedCount = RequestRecordManager.removeStrategy.toLowerCase() match {
        case "km" => removeSomeWarmContainersByKM(allSampledTables)
        case "random" => removeRandomWarmContainers(allSampledTables)
        case "redundancy" => removeWarmContainersByRedundancy(allSampledTables)
        case "total_voc" => removeWarmContainersByTotalVoC(allSampledTables)
        case "voc" => removeWarmContainersByVoC(allSampledTables)
        case "svoc" => removeWarmContainersByStrictVoC(allSampledTables)
        case "none" => 
          logging.info(this, "不执行容器移除策略")
          0
        case _ => throw new IllegalArgumentException(s"未知的容器移除策略: ${RequestRecordManager.removeStrategy}")
      }
      logging.warn(this, s"由${RequestRecordManager.removeStrategy}策略移除 $removedCount 个热容器，当前热容器数量: ${warmContainers.length - removedCount}")
      // 更新全局计数器
      proactiveRemovedContainerCount += removedCount
      logging.info(this, s"当前累计主动移除的容器数量: $proactiveRemovedContainerCount")
    } else {
      logging.info(this, "当前热容器数量合理")
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
    
    // 构建容器大小映射和列表
    val containerSizesMap = containers.map(container => 
      container.containerId -> Option(container.dbSizeLastRecord).getOrElse(0.0)
    ).toMap

    val containerSizes = containerOrder.map(containerId => 
      containerSizesMap.getOrElse(containerId, 0.0)
    )

    // 调用 KM 算法
    val (matchedPairs, unmatchedContainers) = KM.kuhnMunkres(
      costMatrix, 
      recentRequestTables, 
      containerOrder,
      containerSizes
    )
    
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

  // 处理请求数为0的情况：保留leastSaveContainers个dbSize最大的容器，移除其他容器
  private def removeWarmContainersBySize(warmContainers: List[ContainerDbInfo])(implicit logging: Logging): Unit = {
    // 按照dbSizeLastRecord降序排序warm容器
    val sortedBySize = warmContainers.sortBy(-_.dbSizeLastRecord)
    
    // 选择前k个保留，其余移除
    val containersToKeep = sortedBySize.take(leastSaveContainers)
    val containersToRemove = warmContainers.filterNot(c => containersToKeep.exists(_.containerId == c.containerId))
    
    // 输出调试信息
    logging.info(this, "==== Debug Information: 请求记录为空时的特殊处理 ====")
    logging.info(this, s"保留dbSizeLastRecord最大的 $leastSaveContainers 个容器，移除其他 ${containersToRemove.size} 个容器")
    
    // 输出要保留的容器信息
    logging.info(this, "保留的容器:")
    containersToKeep.foreach { container =>
      val tablesInfo = container.tables.mkString(", ")
      logging.info(this, s"保留 -> Container ID: ${container.containerId}, dbSize: ${container.dbSizeLastRecord}, useCount: ${container.useCount}, Tables: [$tablesInfo]")
    }
    
    // 输出要移除的容器信息
    logging.info(this, "即将移除的容器:")
    containersToRemove.foreach { container =>
      val tablesInfo = container.tables.mkString(", ")
      logging.info(this, s"移除 -> Container ID: ${container.containerId}, dbSize: ${container.dbSizeLastRecord}, useCount: ${container.useCount}, Tables: [$tablesInfo]")
    }
    
    // 执行移除操作
    containersToRemove.foreach { container =>
      logging.info(this, s"正在移除热容器: ${container.containerId}")
      removeContainerById(container.containerId)
    }
    
    // 记录完成信息
    logging.info(this, s"特殊处理：移除了 ${containersToRemove.size} 个热容器，保留了 ${containersToKeep.size} 个热容器")
  }

  // 移除一些热容器
  /**
   * 根据Kuhn-Munkres算法移除部分热容器，同时确保系统中至少保留全局变量leastSaveContainers指定数量的热容器
   *
   * @param recentRequestTables 最近的请求表列表
   * @param logging 日志记录实例
   * @return 成功移除的热容器数量
   */
  private def removeSomeWarmContainersByKM(recentRequestTables: List[List[String]])(implicit logging: Logging): Int = {
    logging.info(this, s"尝试移除一些热容器, recentRequestTables: $recentRequestTables, 至少保留热容器: $leastSaveContainers")
    val dbInfo = ContainerDbInfoManager.getDbInfo()
    val containers = dbInfo.values.filter(info => validStates.contains(info.state)).toList

    // 如果没有请求或容器，直接返回
    if (recentRequestTables.isEmpty || containers.isEmpty) {
      logging.warn(this, s"容器列表 $containers 或请求列表 $recentRequestTables 为空！")
      return 0
    }

    // 构造代价矩阵
    val (costMatrix, containerOrder) = KM.buildCostMatrix(recentRequestTables, containers)

    // 构建容器大小映射和列表
    val containerSizesMap = containers.map(container => 
      container.containerId -> Option(container.dbSizeLastRecord).getOrElse(0.0)
    ).toMap

    val containerSizes = containerOrder.map(containerId => 
      containerSizesMap.getOrElse(containerId, 0.0)
    )

    // 调用 KM 算法
    val (matchedPairs, unmatchedContainers) = KM.kuhnMunkres(
      costMatrix, 
      recentRequestTables, 
      containerOrder,
      containerSizes
    )
    logging.info(this, s"匹配成功: $matchedPairs, 未匹配的容器: $unmatchedContainers")

    // 获取所有warm容器
    val allWarmContainers = containers.filter(_.state == "warm")
    logging.info(this, s"当前系统中共有 ${containers.size} 个容器，其中热容器数量: ${allWarmContainers.size}")
    
    // 获取匹配成功的warm容器
    val matchedWarmContainers = matchedPairs
      .map(_._2) // 获取匹配的容器索引
      .map(containerOrder) // 将索引转换为容器ID
      .flatMap(containerId => containers.find(_.containerId == containerId)) // 根据ID查找容器信息
      .filter(_.state == "warm") // 仅保留warm状态的容器
    
    // 计算还需保留的warm容器数量
    val matchedWarmCount = matchedWarmContainers.size
    val additionalWarmToKeep = Math.max(0, leastSaveContainers - matchedWarmCount)
    
    // 安全检查：确保我们不会试图保留比未匹配容器更多的容器
    val actualAdditionalToKeep = Math.min(additionalWarmToKeep, unmatchedContainers.size)
    
    logging.info(this, s"匹配的warm容器数: $matchedWarmCount, 需额外保留的warm容器数: $actualAdditionalToKeep (原始计算: $additionalWarmToKeep)")

    // 从未匹配容器中找出warm容器
    val unmatchedWarmContainers = unmatchedContainers
      .map(containerOrder) // 将索引转换为容器ID
      .flatMap(containerId => containers.find(_.containerId == containerId)) // 根据ID查找容器信息
      .filter(_.state == "warm") // 仅保留warm状态的容器
    
    // 如果需要额外保留容器，按dbSizeLastRecord排序，保留较大的容器
    val warmContainersToKeep = if (actualAdditionalToKeep > 0 && unmatchedWarmContainers.nonEmpty) {
      // 按dbSizeLastRecord降序排序，保留较大的容器
      unmatchedWarmContainers.toList
        .sortBy(container => {
          // 如果dbSizeLastRecord为null，则将其视为0.0
          val dbSize = Option(container.dbSizeLastRecord).getOrElse(0.0)
          // 使用负值来实现降序排序
          -dbSize
        })
        .take(actualAdditionalToKeep)
    } else {
      List.empty
    }
    
    // 最终确定要移除的warm容器
    val warmContainersToRemove = unmatchedWarmContainers
      .filterNot(container => warmContainersToKeep.exists(_.containerId == container.containerId))

    // 输出一些日志，包括容器内表的信息等
    {
      // 输出所有容器的信息，方便调试
      logging.info(this, "==== Debug Information: 所有容器 ====")
      containers.foreach { container =>
        val tablesInfo = container.tables.mkString(", ")
        val dbSizeInfo = Option(container.dbSizeLastRecord).getOrElse(0L)
        logging.info(this, s"Container ID: ${container.containerId}, State: ${container.state}, DbSize: $dbSizeInfo, Tables: [$tablesInfo]")
      }

      // 输出决定移除的容器的信息
      if (warmContainersToRemove.nonEmpty) {
        logging.info(this, "==== Debug Information: 容器决策 ====")
        
        // 输出匹配成功的容器
        matchedWarmContainers.foreach { container =>
          val tablesInfo = container.tables.mkString(", ")
          val dbSizeInfo = Option(container.dbSizeLastRecord).getOrElse(0L)
          logging.info(this, s"匹配保留 -> Container ID: ${container.containerId}, DbSize: $dbSizeInfo, Tables: [$tablesInfo]")
        }
        
        // 输出因大小而额外保留的容器
        warmContainersToKeep.foreach { container =>
          val tablesInfo = container.tables.mkString(", ")
          val dbSizeInfo = Option(container.dbSizeLastRecord).getOrElse(0L)
          logging.info(this, s"大小保留 -> Container ID: ${container.containerId}, DbSize: $dbSizeInfo, Tables: [$tablesInfo]")
        }
        
        // 输出将被移除的容器
        warmContainersToRemove.foreach { container =>
          val tablesInfo = container.tables.mkString(", ")
          val dbSizeInfo = Option(container.dbSizeLastRecord).getOrElse(0L)
          logging.info(this, s"移除 -> Container ID: ${container.containerId}, DbSize: $dbSizeInfo, Tables: [$tablesInfo]")
        }

        // 输出未移除的非warm容器的信息
        val nonWarmContainers = containers.filter(_.state != "warm")
        nonWarmContainers.foreach { container =>
          val tablesInfo = container.tables.mkString(", ")
          logging.info(this, s"非warm -> Container ID: ${container.containerId}, State: ${container.state}, Tables: [$tablesInfo]")
        }
        
        logging.info(this, s"总结: 总热容器数: ${allWarmContainers.size}, 移除后剩余热容器数: ${allWarmContainers.size - warmContainersToRemove.size}")
        logging.info(this, "=====================================")
      }
    }
    
    // 如果没有冗余的 warm 容器需要移除，记录日志并返回
    if (warmContainersToRemove.isEmpty) {
      logging.warn(this, "没有冗余的 warm 容器需要移除，可能是因为所有热容器都匹配成功或需要保留的数量已达到最小阈值")
      return 0
    }

    // 移除 warm 容器并记录日志
    warmContainersToRemove.foreach { container =>
      logging.info(this, s"正在移除热容器: ${container.containerId}")
      removeContainerById(container.containerId)
    }

    // 记录完成信息
    val removedCount = warmContainersToRemove.size
    logging.info(this, s"KM 移除了 ${removedCount} 个热容器, 剩余热容器数: ${allWarmContainers.size - removedCount}")
    removedCount
  }

  // 根据km选择容器数，随机移除热容器（用于对照实验）
  private def goodRemoveRandomWarmContainers(recentRequestTables: List[List[String]])(implicit logging: Logging): Int = {
    val dbInfo = ContainerDbInfoManager.getDbInfo()
    val containers = dbInfo.values.filter(info => validStates.contains(info.state)).toList

    // 如果没有请求或容器，直接返回
    if (recentRequestTables.isEmpty || containers.isEmpty) {
      logging.warn(this, s"容器列表 $containers 或请求列表 $recentRequestTables 为空！")
      return 0
    }

    logging.info(this, s"尝试随机移除热容器, recentRequestTables: $recentRequestTables")
    // 仅获取warm状态的容器
    val warmContainers = dbInfo.values.filter(_.state == "warm").toList
    logging.info(this, s"当前系统中共有 ${containers.size} 个容器，其中热容器数量: ${warmContainers.size}")

    // 如果没有warm容器，直接返回
    if (warmContainers.isEmpty) {
      logging.warn(this, "没有warm容器可供移除")
      return 0
    }

    // 首先运行原始算法来获取要移除的容器数量
    val dbInfoCopy = dbInfo.values.filter(info => info.state == "warm" || info.state == "working").toList
    val (costMatrix, containerOrder) = KM.buildCostMatrix(recentRequestTables, dbInfoCopy)
    // 构建容器大小映射和列表
    val containerSizesMap = containers.map(container => 
      container.containerId -> Option(container.dbSizeLastRecord).getOrElse(0.0)
    ).toMap

    val containerSizes = containerOrder.map(containerId => 
      containerSizesMap.getOrElse(containerId, 0.0)
    )

    // 调用 KM 算法
    val (matchedPairs, unmatchedContainers) = KM.kuhnMunkres(
      costMatrix, 
      recentRequestTables, 
      containerOrder,
      containerSizes
    )
    val originalRemovalCount = unmatchedContainers
      .map(containerOrder)
      .flatMap(containerId => dbInfoCopy.find(_.containerId == containerId))
      .count(_.state == "warm")

    // 如果原算法不需要移除任何容器，我们也直接返回
    if (originalRemovalCount == 0) {
      logging.warn(this, "原算法无需移除容器，随机算法也不执行移除")
      return 0
    }

    // 计算可以移除的最大容器数量，确保剩余热容器数不低于leastSaveContainers
    val maxRemovalCount = Math.max(0, warmContainers.size - leastSaveContainers)
    
    // 确定最终要移除的容器数量，取原始计划和最大可移除数中的较小值
    val finalRemovalCount = Math.min(originalRemovalCount, maxRemovalCount)
    
    // 如果没有容器可以移除，直接返回
    if (finalRemovalCount <= 0) {
      logging.warn(this, s"热容器数量 (${warmContainers.size}) 已经达到或低于最小阈值 ($leastSaveContainers)，不执行移除操作")
      return 0
    }
    
    // 随机选择确定数量的warm容器
    val shuffledContainers = scala.util.Random.shuffle(warmContainers)
    val containersToRemove = shuffledContainers.take(finalRemovalCount)

    // 输出调试信息
    {
      logging.info(this, "==== Debug Information: 随机移除决策 ====")
      logging.info(this, s"原算法决定移除的容器数量: $originalRemovalCount")
      logging.info(this, s"考虑最小热容器阈值 ($leastSaveContainers) 后，实际决定移除: $finalRemovalCount")
      
      // 输出所有warm容器信息
      logging.info(this, "当前所有warm容器:")
      warmContainers.foreach { container =>
        val tablesInfo = container.tables.mkString(", ")
        val dbSizeInfo = Option(container.dbSizeLastRecord).getOrElse(0.0)
        logging.info(this, s"Container ID: ${container.containerId}, DbSize: $dbSizeInfo, Tables: [$tablesInfo]")
      }

      // 输出将要移除的容器信息
      logging.info(this, "即将随机移除的容器:")
      containersToRemove.foreach { container =>
        val tablesInfo = container.tables.mkString(", ")
        val dbSizeInfo = Option(container.dbSizeLastRecord).getOrElse(0.0)
        logging.info(this, s"移除 -> Container ID: ${container.containerId}, DbSize: $dbSizeInfo, Tables: [$tablesInfo]")
      }

      // 输出将要保留的容器信息
      val remainingContainers = warmContainers.filterNot(c => containersToRemove.exists(_.containerId == c.containerId))
      logging.info(this, "保留的容器:")
      remainingContainers.foreach { container =>
        val tablesInfo = container.tables.mkString(", ")
        val dbSizeInfo = Option(container.dbSizeLastRecord).getOrElse(0.0)
        logging.info(this, s"保留 -> Container ID: ${container.containerId}, DbSize: $dbSizeInfo, Tables: [$tablesInfo]")
      }
      
      logging.info(this, s"总结: 总热容器数: ${warmContainers.size}, 移除后剩余热容器数: ${warmContainers.size - finalRemovalCount}")
      logging.info(this, "=====================================")
    }

    // 执行移除操作
    containersToRemove.foreach { container =>
      logging.info(this, s"正在移除热容器: ${container.containerId}")
      removeContainerById(container.containerId)
    }

    val removedCount = containersToRemove.size
    logging.info(this, s"随机算法移除了 ${removedCount} 个热容器，剩余热容器数: ${warmContainers.size - removedCount}")
    
    // 返回移除的容器数量
    removedCount
  }

  // 根据请求数选择容器数，随机移除热容器（用于对照实验）
  private def limit_removeRandomWarmContainers(recentRequestTables: List[List[String]])(implicit logging: Logging): Int = {
    val dbInfo = ContainerDbInfoManager.getDbInfo()
    val containers = dbInfo.values.filter(info => validStates.contains(info.state)).toList

    // 如果没有请求或容器，直接返回
    if (containers.isEmpty) {
      logging.warn(this, s"容器列表为空！")
      return 0
    }

    // 仅获取warm状态的容器
    val warmContainers = dbInfo.values.filter(_.state == "warm").toList
    logging.info(this, s"当前系统中共有 ${containers.size} 个容器，其中热容器数量: ${warmContainers.size}")

    // 如果没有warm容器，直接返回
    if (warmContainers.isEmpty) {
      logging.warn(this, "没有warm容器可供移除")
      return 0
    }

    // 计算目标热容器数量（等于recentRequestTables的长度）
    val targetWarmContainers = Math.max(recentRequestTables.size, leastSaveContainers)
    logging.info(this, s"目标热容器数量: $targetWarmContainers (请求表长度: ${recentRequestTables.size}, 最低保留数: $leastSaveContainers)")

    // 如果当前热容器数量小于等于目标数量，不需要移除
    if (warmContainers.size <= targetWarmContainers) {
      logging.info(this, s"当前热容器数量 (${warmContainers.size}) 已经小于等于目标数量 ($targetWarmContainers)，不需要移除")
      return 0
    }

    // 计算需要移除的容器数量
    val removalCount = warmContainers.size - targetWarmContainers
    logging.info(this, s"计划移除 $removalCount 个热容器，移除后剩余热容器数将为 $targetWarmContainers")

    // 随机选择确定数量的warm容器进行移除
    val shuffledContainers = scala.util.Random.shuffle(warmContainers)
    val containersToRemove = shuffledContainers.take(removalCount)

    // 输出调试信息
    {
      logging.info(this, "==== Debug Information: 热容器移除决策 ====")
      logging.info(this, s"目标: 保持热容器数量 = max(请求表长度 ${recentRequestTables.size}, 最低保留数 $leastSaveContainers)")
      logging.info(this, s"当前热容器数量: ${warmContainers.size}, 目标热容器数量: $targetWarmContainers")
      logging.info(this, s"需要移除的热容器数量: $removalCount")
      
      // 输出所有warm容器信息
      logging.info(this, "当前所有warm容器:")
      warmContainers.foreach { container =>
        val tablesInfo = container.tables.mkString(", ")
        val dbSizeInfo = Option(container.dbSizeLastRecord).getOrElse(0.0)
        logging.info(this, s"Container ID: ${container.containerId}, DbSize: $dbSizeInfo, Tables: [$tablesInfo]")
      }

      // 输出将要移除的容器信息
      logging.info(this, "即将移除的容器:")
      containersToRemove.foreach { container =>
        val tablesInfo = container.tables.mkString(", ")
        val dbSizeInfo = Option(container.dbSizeLastRecord).getOrElse(0.0)
        logging.info(this, s"移除 -> Container ID: ${container.containerId}, DbSize: $dbSizeInfo, Tables: [$tablesInfo]")
      }

      // 输出将要保留的容器信息
      val remainingContainers = warmContainers.filterNot(c => containersToRemove.exists(_.containerId == c.containerId))
      logging.info(this, "保留的容器:")
      remainingContainers.foreach { container =>
        val tablesInfo = container.tables.mkString(", ")
        val dbSizeInfo = Option(container.dbSizeLastRecord).getOrElse(0.0)
        logging.info(this, s"保留 -> Container ID: ${container.containerId}, DbSize: $dbSizeInfo, Tables: [$tablesInfo]")
      }
      
      logging.info(this, s"总结: 总热容器数: ${warmContainers.size}, 移除后剩余热容器数: $targetWarmContainers")
      logging.info(this, "=====================================")
    }

    // 执行移除操作
    containersToRemove.foreach { container =>
      logging.info(this, s"正在移除热容器: ${container.containerId}")
      removeContainerById(container.containerId)
    }

    // 再次检查是否移除成功
    val removedCount = containersToRemove.size
    logging.info(this, s"成功移除了 ${removedCount} 个热容器，剩余热容器数应为: $targetWarmContainers")
    
    // 返回移除的容器数量
    removedCount
  }

  // 根据请求数选择容器数，随机移除热容器（用于对照实验（无最低限制）
  private def removeRandomWarmContainers(recentRequestTables: List[List[String]])(implicit logging: Logging): Int = {
    val dbInfo = ContainerDbInfoManager.getDbInfo()
    val containers = dbInfo.values.filter(info => validStates.contains(info.state)).toList

    // 如果没有请求或容器，直接返回
    if (containers.isEmpty) {
      logging.warn(this, s"容器列表为空！")
      return 0
    }

    // 仅获取warm状态的容器
    val warmContainers = dbInfo.values.filter(_.state == "warm").toList
    logging.info(this, s"当前系统中共有 ${containers.size} 个容器，其中热容器数量: ${warmContainers.size}")

    // 如果没有warm容器，直接返回
    if (warmContainers.isEmpty) {
      logging.warn(this, "没有warm容器可供移除")
      return 0
    }

    // 计算目标热容器数量（等于recentRequestTables的长度）
    val targetWarmContainers = recentRequestTables.size
    logging.info(this, s"目标热容器数量: $targetWarmContainers (请求表长度: ${recentRequestTables.size})")

    // 如果当前热容器数量小于等于目标数量，不需要移除
    if (warmContainers.size <= targetWarmContainers) {
      logging.info(this, s"当前热容器数量 (${warmContainers.size}) 已经小于等于目标数量 ($targetWarmContainers)，不需要移除")
      return 0
    }

    // 计算需要移除的容器数量
    val removalCount = warmContainers.size - targetWarmContainers
    logging.info(this, s"计划移除 $removalCount 个热容器，移除后剩余热容器数将为 $targetWarmContainers")

    // 随机选择确定数量的warm容器进行移除
    val shuffledContainers = scala.util.Random.shuffle(warmContainers)
    val containersToRemove = shuffledContainers.take(removalCount)

    // 输出调试信息
    {
      logging.info(this, "==== Debug Information: 热容器移除决策 ====")
      logging.info(this, s"目标: 保持热容器数量 = 请求表长度 ${recentRequestTables.size}")
      logging.info(this, s"当前热容器数量: ${warmContainers.size}, 目标热容器数量: $targetWarmContainers")
      logging.info(this, s"需要移除的热容器数量: $removalCount")
      
      // 输出所有warm容器信息
      logging.info(this, "当前所有warm容器:")
      warmContainers.foreach { container =>
        val tablesInfo = container.tables.mkString(", ")
        val dbSizeInfo = Option(container.dbSizeLastRecord).getOrElse(0.0)
        logging.info(this, s"Container ID: ${container.containerId}, DbSize: $dbSizeInfo, Tables: [$tablesInfo]")
      }

      // 输出将要移除的容器信息
      logging.info(this, "即将移除的容器:")
      containersToRemove.foreach { container =>
        val tablesInfo = container.tables.mkString(", ")
        val dbSizeInfo = Option(container.dbSizeLastRecord).getOrElse(0.0)
        logging.info(this, s"移除 -> Container ID: ${container.containerId}, DbSize: $dbSizeInfo, Tables: [$tablesInfo]")
      }

      // 输出将要保留的容器信息
      val remainingContainers = warmContainers.filterNot(c => containersToRemove.exists(_.containerId == c.containerId))
      logging.info(this, "保留的容器:")
      remainingContainers.foreach { container =>
        val tablesInfo = container.tables.mkString(", ")
        val dbSizeInfo = Option(container.dbSizeLastRecord).getOrElse(0.0)
        logging.info(this, s"保留 -> Container ID: ${container.containerId}, DbSize: $dbSizeInfo, Tables: [$tablesInfo]")
      }
      
      logging.info(this, s"总结: 总热容器数: ${warmContainers.size}, 移除后剩余热容器数: $targetWarmContainers")
      logging.info(this, "=====================================")
    }

    // 执行移除操作
    containersToRemove.foreach { container =>
      logging.info(this, s"正在移除热容器: ${container.containerId}")
      removeContainerById(container.containerId)
    }

    // 再次检查是否移除成功
    val removedCount = containersToRemove.size
    logging.info(this, s"成功移除了 ${removedCount} 个热容器，剩余热容器数应为: $targetWarmContainers")
    
    // 返回移除的容器数量
    removedCount
  }

  // 按照容器冗余表数计算并移除（移除数等于容器列表的长度减去请求列表的长度，用于对照实验）
  private def removeWarmContainersByRedundancy(recentRequestTables: List[List[String]])(implicit logging: Logging): Int = {    
    val dbInfo = ContainerDbInfoManager.getDbInfo()
    val containers = dbInfo.values.filter(info => validStates.contains(info.state)).toList

    // 如果容器列表的长度 <= 请求列表的长度，则不进行移除操作
    if (recentRequestTables.isEmpty || containers.isEmpty || containers.length <= recentRequestTables.length) {
      logging.warn(this, s"容器列表长度 ${containers.length} 小于等于 请求列表长度 ${recentRequestTables.length}，跳过移除操作")
      return 0
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

    // 计算需要移除的容器数量，数量等于容器列表的长度减去请求列表的长度
    val originalRemovalCount = containers.length - recentRequestTables.length

    // 选取冗余分数最高的容器进行移除
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
      return 0
    }

    warmContainersToRemove.foreach { container =>
      logging.info(this, s"正在移除热容器: ${container.containerId}")
      removeContainerById(container.containerId)
    }

    val removedCount = warmContainersToRemove.size
    logging.info(this, s"KM 移除了 ${removedCount} 个热容器")
    
    // 返回移除的容器数量
    removedCount
  }

  // 使用总VoC分数策略移除热容器（计算活动次数而不是单位时间活动次数）
  private def removeWarmContainersByTotalVoC(recentRequestTables: List[List[String]])(implicit logging: Logging): Int = {
    val dbInfo = ContainerDbInfoManager.getDbInfo()
    val containers = dbInfo.values.filter(info => validStates.contains(info.state)).toList

    // 如果没有请求或容器，直接返回
    if (containers.isEmpty) {
      logging.warn(this, s"容器列表 $containers 为空！")
      return 0
    }

    logging.info(this, s"尝试使用Total VoC策略移除热容器, recentRequestTables: $recentRequestTables")
    // 仅获取warm状态的容器
    val warmContainers = dbInfo.values.filter(_.state == "warm").toList
    logging.info(this, s"当前系统中共有 ${containers.size} 个容器，其中热容器数量: ${warmContainers.size}")

    if (recentRequestTables.isEmpty) {
      logging.warn(this, "请求列表为空，转为使用容器大小策略移除")
      removeWarmContainersBySize(warmContainers)
      return 0
    }

    // 首先运行原始算法来获取要移除的容器数量
    val dbInfoCopy = dbInfo.values.filter(info => info.state == "warm" || info.state == "working").toList
    val (costMatrix, containerOrder) = KM.buildCostMatrix(recentRequestTables, dbInfoCopy)
    // 构建容器大小映射和列表
    val containerSizesMap = containers.map(container => 
      container.containerId -> Option(container.dbSizeLastRecord).getOrElse(0.0)
    ).toMap

    val containerSizes = containerOrder.map(containerId => 
      containerSizesMap.getOrElse(containerId, 0.0)
    )

    // 调用 KM 算法
    val (matchedPairs, unmatchedContainers) = KM.kuhnMunkres(
      costMatrix, 
      recentRequestTables, 
      containerOrder,
      containerSizes
    )
    val originalRemovalCount = unmatchedContainers
      .map(containerOrder)
      .flatMap(containerId => dbInfoCopy.find(_.containerId == containerId))
      .count(_.state == "warm")

    // 如果原算法不需要移除任何容器，我们也直接返回
    if (originalRemovalCount == 0) {
      logging.warn(this, "原算法无需移除容器，Total VoC算法也不执行移除")
      return 0
    }

    // 计算可以移除的最大容器数量，确保剩余热容器数不低于leastSaveContainers
    val maxRemovalCount = Math.max(0, warmContainers.size - leastSaveContainers)
    
    // 确定最终要移除的容器数量，取原始计划和最大可移除数中的较小值
    val finalRemovalCount = Math.min(originalRemovalCount, maxRemovalCount)
    
    // 如果没有容器可以移除，直接返回
    if (finalRemovalCount <= 0) {
      logging.warn(this, s"热容器数量 (${warmContainers.size}) 已经达到或低于最小阈值 ($leastSaveContainers)，不执行移除操作")
      return 0
    }

    // 计算每个warm容器的Total VoC分数 (dbSizeLastRecord * useCount)
    val containersWithVoC = warmContainers.map { container =>
      // 安全处理dbSizeLastRecord可能为null的情况
      val dbSize = Option(container.dbSizeLastRecord).getOrElse(0.0)
      val vocScore = dbSize * container.useCount
      (container, vocScore)
    }

    // 按VoC分数升序排序，选择分数最低的x个容器
    val sortedContainers = containersWithVoC.sortBy(_._2)
    val containersToRemove = sortedContainers.take(finalRemovalCount).map(_._1)

    // 输出调试信息
    {
      logging.info(this, "==== Debug Information: Total VoC策略移除决策 ====")
      logging.info(this, s"原算法决定移除的容器数量: $originalRemovalCount")
      logging.info(this, s"考虑最小热容器阈值 ($leastSaveContainers) 后，实际决定移除: $finalRemovalCount")
      
      // 输出所有warm容器信息及其VoC分数
      logging.info(this, "当前所有warm容器及Total VoC分数:")
      containersWithVoC.foreach { case (container, vocScore) =>
        val tablesInfo = container.tables.mkString(", ")
        val dbSize = Option(container.dbSizeLastRecord).getOrElse(0.0)
        logging.info(this, s"Container ID: ${container.containerId}, Total VoC分数: $vocScore, dbSize: $dbSize, useCount: ${container.useCount}, Tables: [$tablesInfo]")
      }

      // 输出将要移除的容器信息
      logging.info(this, "即将基于Total VoC分数移除的容器:")
      containersToRemove.foreach { container =>
        val vocIndex = containersWithVoC.indexWhere(_._1.containerId == container.containerId)
        val vocScore = if (vocIndex >= 0) containersWithVoC(vocIndex)._2 else 0.0
        val tablesInfo = container.tables.mkString(", ")
        logging.info(this, s"移除 -> Container ID: ${container.containerId}, Total VoC分数: $vocScore, Tables: [$tablesInfo]")
      }

      // 输出将要保留的容器信息
      val remainingContainers = warmContainers.filterNot(c => containersToRemove.exists(_.containerId == c.containerId))
      logging.info(this, "保留的容器:")
      remainingContainers.foreach { container =>
        val vocIndex = containersWithVoC.indexWhere(_._1.containerId == container.containerId)
        val vocScore = if (vocIndex >= 0) containersWithVoC(vocIndex)._2 else 0.0
        val tablesInfo = container.tables.mkString(", ")
        logging.info(this, s"保留 -> Container ID: ${container.containerId}, Total VoC分数: $vocScore, Tables: [$tablesInfo]")
      }
      
      logging.info(this, s"总结: 总热容器数: ${warmContainers.size}, 移除后剩余热容器数: ${warmContainers.size - finalRemovalCount}")
      logging.info(this, "=====================================")
    }

    // 执行移除操作
    containersToRemove.foreach { container =>
      logging.info(this, s"正在移除热容器: ${container.containerId}")
      removeContainerById(container.containerId)
    }

    val removedCount = containersToRemove.size
    logging.info(this, s"Total VoC算法移除了 ${removedCount} 个热容器，剩余热容器数: ${warmContainers.size - removedCount}")
    
    // 返回移除的容器数量
    removedCount
  }

  // 使用VoC分数策略移除热容器（计算单位时间活动次数）
  private def removeWarmContainersByVoC(recentRequestTables: List[List[String]])(implicit logging: Logging): Int = {
    val dbInfo = ContainerDbInfoManager.getDbInfo()
    val containers = dbInfo.values.filter(info => validStates.contains(info.state)).toList

    // 如果没有请求或容器，直接返回
    if (containers.isEmpty) {
      logging.warn(this, s"容器列表 $containers 为空！")
      return 0
    }

    logging.info(this, s"尝试使用VoC策略移除热容器, recentRequestTables: $recentRequestTables")
    // 仅获取warm状态的容器
    val warmContainers = dbInfo.values.filter(_.state == "warm").toList
    logging.info(this, s"当前系统中共有 ${containers.size} 个容器，其中热容器数量: ${warmContainers.size}")

    if (recentRequestTables.isEmpty) {
      logging.warn(this, "请求列表为空，转为使用容器大小策略移除")
      removeWarmContainersBySize(warmContainers)
      return 0
    }

    // 首先运行原始算法来获取要移除的容器数量
    val dbInfoCopy = dbInfo.values.filter(info => info.state == "warm" || info.state == "working").toList
    val (costMatrix, containerOrder) = KM.buildCostMatrix(recentRequestTables, dbInfoCopy)
    // 构建容器大小映射和列表
    val containerSizesMap = containers.map(container => 
      container.containerId -> Option(container.dbSizeLastRecord).getOrElse(0.0)
    ).toMap

    val containerSizes = containerOrder.map(containerId => 
      containerSizesMap.getOrElse(containerId, 0.0)
    )

    // 调用 KM 算法
    val (matchedPairs, unmatchedContainers) = KM.kuhnMunkres(
      costMatrix, 
      recentRequestTables, 
      containerOrder,
      containerSizes
    )
    val originalRemovalCount = unmatchedContainers
      .map(containerOrder)
      .flatMap(containerId => dbInfoCopy.find(_.containerId == containerId))
      .count(_.state == "warm")

    // 如果原算法不需要移除任何容器，我们也直接返回
    if (originalRemovalCount == 0) {
      logging.warn(this, "原算法无需移除容器，VoC算法也不执行移除")
      return 0
    }

    // 计算可以移除的最大容器数量，确保剩余热容器数不低于leastSaveContainers
    val maxRemovalCount = Math.max(0, warmContainers.size - leastSaveContainers)
    
    // 确定最终要移除的容器数量，取原始计划和最大可移除数中的较小值
    val finalRemovalCount = Math.min(originalRemovalCount, maxRemovalCount)
    
    // 如果没有容器可以移除，直接返回
    if (finalRemovalCount <= 0) {
      logging.warn(this, s"热容器数量 (${warmContainers.size}) 已经达到或低于最小阈值 ($leastSaveContainers)，不执行移除操作")
      return 0
    }

    // 计算当前时间
    val currentTime = Instant.now.getEpochSecond
    
    // 计算每个warm容器的VoC分数 (dbSizeLastRecord * (useCount / 存活时间))
    val containersWithVoC = warmContainers.map { container =>
      // 计算容器存活时间（单位：秒）
      val lifeTimeInSeconds = currentTime - container.createTimestamp
      // 防止除以零，如果存活时间为0，则设为1
      val safeLifeTime = if (lifeTimeInSeconds <= 0) 1L else lifeTimeInSeconds
      // 计算单位时间的使用频率
      val usageRate = container.useCount.toDouble / safeLifeTime
      // 计算VoC分数，安全处理dbSizeLastRecord可能为null的情况
      val dbSize = Option(container.dbSizeLastRecord).getOrElse(0.0)
      val vocScore = dbSize * usageRate
      (container, vocScore)
    }

    // 按VoC分数升序排序，选择分数最低的x个容器
    val sortedContainers = containersWithVoC.sortBy(_._2)
    val containersToRemove = sortedContainers.take(finalRemovalCount).map(_._1)

    // 输出调试信息
    {
      logging.info(this, "==== Debug Information: VoC策略移除决策 ====")
      logging.info(this, s"原算法决定移除的容器数量: $originalRemovalCount")
      logging.info(this, s"考虑最小热容器阈值 ($leastSaveContainers) 后，实际决定移除: $finalRemovalCount")
      
      // 输出所有warm容器信息及其VoC分数
      logging.info(this, "当前所有warm容器及VoC分数:")
      containersWithVoC.foreach { case (container, vocScore) =>
        val tablesInfo = container.tables.mkString(", ")
        val dbSize = Option(container.dbSizeLastRecord).getOrElse(0.0)
        logging.info(this, s"Container ID: ${container.containerId}, VoC分数: $vocScore, dbSize: $dbSize, useCount: ${container.useCount}, Tables: [$tablesInfo]")
      }

      // 输出将要移除的容器信息
      logging.info(this, "即将基于VoC分数移除的容器:")
      containersToRemove.foreach { container =>
        val vocIndex = containersWithVoC.indexWhere(_._1.containerId == container.containerId)
        val vocScore = if (vocIndex >= 0) containersWithVoC(vocIndex)._2 else 0.0
        val tablesInfo = container.tables.mkString(", ")
        logging.info(this, s"移除 -> Container ID: ${container.containerId}, VoC分数: $vocScore, Tables: [$tablesInfo]")
      }

      // 输出将要保留的容器信息
      val remainingContainers = warmContainers.filterNot(c => containersToRemove.exists(_.containerId == c.containerId))
      logging.info(this, "保留的容器:")
      remainingContainers.foreach { container =>
        val vocIndex = containersWithVoC.indexWhere(_._1.containerId == container.containerId)
        val vocScore = if (vocIndex >= 0) containersWithVoC(vocIndex)._2 else 0.0
        val tablesInfo = container.tables.mkString(", ")
        logging.info(this, s"保留 -> Container ID: ${container.containerId}, VoC分数: $vocScore, Tables: [$tablesInfo]")
      }
      
      logging.info(this, s"总结: 总热容器数: ${warmContainers.size}, 移除后剩余热容器数: ${warmContainers.size - finalRemovalCount}")
      logging.info(this, "=====================================")
    }

    // 执行移除操作
    containersToRemove.foreach { container =>
      logging.info(this, s"正在移除热容器: ${container.containerId}")
      removeContainerById(container.containerId)
    }

    val removedCount = containersToRemove.size
    logging.info(this, s"VoC算法移除了 ${removedCount} 个热容器，剩余热容器数: ${warmContainers.size - removedCount}")
    
    // 返回移除的容器数量
    removedCount
  }

  // 使用严格VoC分数策略移除热容器（因为每个容器的资源数相同，只计算单位时间使用次数）（使用km算法计算移除数，有保底）
  private def ori_removeWarmContainersByStrictVoC(recentRequestTables: List[List[String]])(implicit logging: Logging): Int = {
    val dbInfo = ContainerDbInfoManager.getDbInfo()
    val containers = dbInfo.values.filter(info => validStates.contains(info.state)).toList

    // 如果没有请求或容器，直接返回
    if (containers.isEmpty) {
      logging.warn(this, s"容器列表 $containers 为空！")
      return 0
    }

    logging.info(this, s"尝试使用单位时间使用频率策略移除热容器, recentRequestTables: $recentRequestTables")
    // 仅获取warm状态的容器
    val warmContainers = dbInfo.values.filter(_.state == "warm").toList
    logging.info(this, s"当前系统中共有 ${containers.size} 个容器，其中热容器数量: ${warmContainers.size}")

    if (recentRequestTables.isEmpty) {
      logging.warn(this, "请求列表为空，转为使用容器大小策略移除")
      removeWarmContainersBySize(warmContainers)
      return 0
    }

    // 首先运行原始算法来获取要移除的容器数量
    val dbInfoCopy = dbInfo.values.filter(info => info.state == "warm" || info.state == "working").toList
    val (costMatrix, containerOrder) = KM.buildCostMatrix(recentRequestTables, dbInfoCopy)
    // 构建容器大小映射和列表
    val containerSizesMap = containers.map(container => 
      container.containerId -> Option(container.dbSizeLastRecord).getOrElse(0.0)
    ).toMap

    val containerSizes = containerOrder.map(containerId => 
      containerSizesMap.getOrElse(containerId, 0.0)
    )

    // 调用 KM 算法
    val (matchedPairs, unmatchedContainers) = KM.kuhnMunkres(
      costMatrix, 
      recentRequestTables, 
      containerOrder,
      containerSizes
    )
    val originalRemovalCount = unmatchedContainers
      .map(containerOrder)
      .flatMap(containerId => dbInfoCopy.find(_.containerId == containerId))
      .count(_.state == "warm")

    // 如果原算法不需要移除任何容器，我们也直接返回
    if (originalRemovalCount == 0) {
      logging.warn(this, "原算法无需移除容器，单位时间使用频率算法也不执行移除")
      return 0
    }

    // 计算可以移除的最大容器数量，确保剩余热容器数不低于leastSaveContainers
    val maxRemovalCount = Math.max(0, warmContainers.size - leastSaveContainers)
    
    // 确定最终要移除的容器数量，取原始计划和最大可移除数中的较小值
    val finalRemovalCount = Math.min(originalRemovalCount, maxRemovalCount)
    
    // 如果没有容器可以移除，直接返回
    if (finalRemovalCount <= 0) {
      logging.warn(this, s"热容器数量 (${warmContainers.size}) 已经达到或低于最小阈值 ($leastSaveContainers)，不执行移除操作")
      return 0
    }

    // 计算当前时间
    val currentTime = Instant.now.getEpochSecond
    
    // 计算每个warm容器的使用频率分数 (useCount / 存活时间)
    val containersWithUsageRate = warmContainers.map { container =>
      // 计算容器存活时间（单位：秒）
      val lifeTimeInSeconds = currentTime - container.createTimestamp
      // 防止除以零，如果存活时间为0，则设为1
      val safeLifeTime = if (lifeTimeInSeconds <= 0) 1L else lifeTimeInSeconds
      // 计算单位时间的使用频率
      val usageRate = container.useCount.toDouble / safeLifeTime
      (container, usageRate)
    }

    // 按使用频率升序排序，选择使用频率最低的x个容器
    val sortedContainers = containersWithUsageRate.sortBy(_._2)
    val containersToRemove = sortedContainers.take(finalRemovalCount).map(_._1)

    // 输出调试信息
    {
      logging.info(this, "==== Debug Information: 单位时间使用频率策略移除决策 ====")
      logging.info(this, s"原算法决定移除的容器数量: $originalRemovalCount")
      logging.info(this, s"考虑最小热容器阈值 ($leastSaveContainers) 后，实际决定移除: $finalRemovalCount")
      
      // 输出所有warm容器信息及其使用频率
      logging.info(this, "当前所有warm容器及单位时间使用频率:")
      containersWithUsageRate.foreach { case (container, usageRate) =>
        val tablesInfo = container.tables.mkString(", ")
        logging.info(this, s"Container ID: ${container.containerId}, 使用频率: $usageRate, useCount: ${container.useCount}, Tables: [$tablesInfo]")
      }

      // 输出将要移除的容器信息
      logging.info(this, "即将基于单位时间使用频率移除的容器:")
      containersToRemove.foreach { container =>
        val rateIndex = containersWithUsageRate.indexWhere(_._1.containerId == container.containerId)
        val usageRate = if (rateIndex >= 0) containersWithUsageRate(rateIndex)._2 else 0.0
        val tablesInfo = container.tables.mkString(", ")
        logging.info(this, s"移除 -> Container ID: ${container.containerId}, 使用频率: $usageRate, Tables: [$tablesInfo]")
      }

      // 输出将要保留的容器信息
      val remainingContainers = warmContainers.filterNot(c => containersToRemove.exists(_.containerId == c.containerId))
      logging.info(this, "保留的容器:")
      remainingContainers.foreach { container =>
        val rateIndex = containersWithUsageRate.indexWhere(_._1.containerId == container.containerId)
        val usageRate = if (rateIndex >= 0) containersWithUsageRate(rateIndex)._2 else 0.0
        val tablesInfo = container.tables.mkString(", ")
        logging.info(this, s"保留 -> Container ID: ${container.containerId}, 使用频率: $usageRate, Tables: [$tablesInfo]")
      }
      
      logging.info(this, s"总结: 总热容器数: ${warmContainers.size}, 移除后剩余热容器数: ${warmContainers.size - finalRemovalCount}")
      logging.info(this, "=====================================")
    }

    // 执行移除操作
    containersToRemove.foreach { container =>
      logging.info(this, s"正在移除热容器: ${container.containerId}")
      removeContainerById(container.containerId)
    }

    val removedCount = containersToRemove.size
    logging.info(this, s"单位时间使用频率算法移除了 ${removedCount} 个热容器，剩余热容器数: ${warmContainers.size - removedCount}")
    
    // 返回移除的容器数量
    removedCount
  }

  // 使用严格VoC分数策略移除热容器（因为每个容器的资源数相同，只计算单位时间使用次数）（自己计算移除数，无保底）
  private def removeWarmContainersByStrictVoC(recentRequestTables: List[List[String]])(implicit logging: Logging): Int = {
    val dbInfo = ContainerDbInfoManager.getDbInfo()
    val containers = dbInfo.values.filter(info => validStates.contains(info.state)).toList

    // 如果没有请求或容器，直接返回
    if (containers.isEmpty) {
      logging.warn(this, s"容器列表 $containers 为空！")
      return 0
    }

    logging.info(this, s"尝试使用单位时间使用频率策略移除热容器, recentRequestTables: $recentRequestTables")
    // 仅获取warm状态的容器
    val warmContainers = dbInfo.values.filter(_.state == "warm").toList
    logging.info(this, s"当前系统中共有 ${containers.size} 个容器，其中热容器数量: ${warmContainers.size}")

    // 计算需要移除的容器数量
    val targetWarmContainers = recentRequestTables.size
    val finalRemovalCount = warmContainers.size - targetWarmContainers
    
    // 如果没有容器可以移除，直接返回
    if (finalRemovalCount <= 0) {
      logging.warn(this, s"热容器数量 (${warmContainers.size}) 已经小于或等于目标数量 ($targetWarmContainers)，不执行移除操作")
      return 0
    }

    // 计算当前时间
    val currentTime = Instant.now.getEpochSecond
    
    // 计算每个warm容器的使用频率分数 (useCount / 存活时间)
    val containersWithUsageRate = warmContainers.map { container =>
      // 计算容器存活时间（单位：秒）
      val lifeTimeInSeconds = currentTime - container.createTimestamp
      // 防止除以零，如果存活时间为0，则设为1
      val safeLifeTime = if (lifeTimeInSeconds <= 0) 1L else lifeTimeInSeconds
      // 计算单位时间的使用频率
      val usageRate = container.useCount.toDouble / safeLifeTime
      (container, usageRate)
    }

    // 按使用频率升序排序，选择使用频率最低的x个容器
    val sortedContainers = containersWithUsageRate.sortBy(_._2)
    val containersToRemove = sortedContainers.take(finalRemovalCount).map(_._1)

    // 输出调试信息
    {
      logging.info(this, "==== Debug Information: 单位时间使用频率策略移除决策 ====")
      logging.info(this, s"根据目标热容器数量 ($targetWarmContainers)，决定移除: $finalRemovalCount")
      
      // 输出所有warm容器信息及其使用频率
      logging.info(this, "当前所有warm容器及单位时间使用频率:")
      containersWithUsageRate.foreach { case (container, usageRate) =>
        val tablesInfo = container.tables.mkString(", ")
        logging.info(this, s"Container ID: ${container.containerId}, 使用频率: $usageRate, useCount: ${container.useCount}, Tables: [$tablesInfo]")
      }

      // 输出将要移除的容器信息
      logging.info(this, "即将基于单位时间使用频率移除的容器:")
      containersToRemove.foreach { container =>
        val rateIndex = containersWithUsageRate.indexWhere(_._1.containerId == container.containerId)
        val usageRate = if (rateIndex >= 0) containersWithUsageRate(rateIndex)._2 else 0.0
        val tablesInfo = container.tables.mkString(", ")
        logging.info(this, s"移除 -> Container ID: ${container.containerId}, 使用频率: $usageRate, Tables: [$tablesInfo]")
      }

      // 输出将要保留的容器信息
      val remainingContainers = warmContainers.filterNot(c => containersToRemove.exists(_.containerId == c.containerId))
      logging.info(this, "保留的容器:")
      remainingContainers.foreach { container =>
        val rateIndex = containersWithUsageRate.indexWhere(_._1.containerId == container.containerId)
        val usageRate = if (rateIndex >= 0) containersWithUsageRate(rateIndex)._2 else 0.0
        val tablesInfo = container.tables.mkString(", ")
        logging.info(this, s"保留 -> Container ID: ${container.containerId}, 使用频率: $usageRate, Tables: [$tablesInfo]")
      }
      
      logging.info(this, s"总结: 总热容器数: ${warmContainers.size}, 移除后剩余热容器数: ${warmContainers.size - finalRemovalCount}")
      logging.info(this, "=====================================")
    }

    // 执行移除操作
    containersToRemove.foreach { container =>
      logging.info(this, s"正在移除热容器: ${container.containerId}")
      removeContainerById(container.containerId)
    }

    val removedCount = containersToRemove.size
    logging.info(this, s"单位时间使用频率算法移除了 ${removedCount} 个热容器，剩余热容器数: ${warmContainers.size - removedCount}")
    
    // 返回移除的容器数量
    removedCount
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
        logging.info(this, s"发送移除请求失败, 容器ID: $containerIdToRemove, 错误信息: ${exception.getMessage}")
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
    logging.info(this, s"原始 Cost matrix(值为代价，越小越匹配):")
    finalMatrix.foreach(row => logging.info(this, row.mkString(" ")))

    (finalMatrix, validContainers.toList)
  }

  /**
   * Kuhn-Munkres算法
   */
   def kuhnMunkres(costMatrix: Array[Array[Double]], requests: List[List[String]], containerOrder: List[String],containerSizes: List[Double])(implicit logging: Logging): (Map[Int, Int], Set[Int]) = {
    val n = costMatrix.length    
    val m = costMatrix(0).length 
    
    // 处理边界情况
    if (m == 0) {
      logging.info(this, "系统中没有可用容器，所有请求均未匹配")
      return (Map.empty[Int, Int], Set.empty[Int])
    }
    
    if (n == 0) {
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
    
    // 转换代价矩阵以处理最小化问题，并引入容器大小权重
    val maxCost = costMatrix.map(_.max).max
    val maxSize = containerSizes.max
    val transformedCostMatrix = Array.ofDim[Double](n, m)
    
    for (i <- costMatrix.indices; j <- costMatrix(i).indices) {
      // 引入容器大小的微小权重
      // 对于代价相同的容器，大小越大的容器权重越大（因为转换为最大化问题）
      val sizeWeight = (containerSizes(j) / maxSize) * 0.1
      // val sizeWeight = (1.0 - containerSizes(j) / maxSize) * 0.001
      transformedCostMatrix(i)(j) = maxCost - costMatrix(i)(j) + sizeWeight

      // 为第一个请求的所有容器输出详细权重计算
      if (i == 0) {
        logging.info(this, s"请求0-容器$j: 原始代价=${costMatrix(i)(j)}, 容器大小=${containerSizes(j)}, " +
          s"容器大小权重=${sizeWeight}, 转换后代价=${transformedCostMatrix(i)(j)}")
      }
    }

    // 打印转换后的代价矩阵
    logging.info(this, "转换后的代价矩阵(加入容器大小权重)(值为收益，越大越匹配):")
    transformedCostMatrix.foreach(row => logging.info(this, row.map(v => f"$v%.6f").mkString(" ")))
  
    
    // 初始化标号
    val labelX = Array.fill(n)(0.0)
    val labelY = Array.fill(m)(0.0)
    
    // 初始化顶标
    for (i <- 0 until n) {
      labelX(i) = transformedCostMatrix(i).max
    }
    
    // 初始化匹配数组
    val matchY = Array.fill(m)(-1)   
    
    // 为每个请求寻找匹配（保持原有匹配逻辑）
    for (i <- 0 until n) {
      logging.info(this, s"正在为请求 $i (表: [${requests(i).mkString(", ")}]) 寻找匹配")
      
      var visited = Array.fill(m)(false)
      var iterationCount = 0
      val maxIterations = n * m * 2
      
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
        while (!findAugmentingPath(i)) {
          iterationCount += 1
          if (iterationCount > maxIterations) {
            logging.warn(this, s"请求 $i 超过最大迭代次数 $maxIterations，跳过该请求")
            break
          }
          
          var delta = Double.MaxValue
          
          for (j <- 0 until m if !visited(j)) {
            val currentDelta = labelX(i) + labelY(j) - transformedCostMatrix(i)(j)
            if (!currentDelta.isInfinite && currentDelta < delta) {
              delta = currentDelta
            }
          }
          
          if (delta.isInfinite || delta == Double.MaxValue) {
            logging.warn(this, s"请求 $i 无法找到有效的调整量，跳过该请求")
            break
          }
          
          if (delta < 1e-10) {
            logging.warn(this, s"请求 $i 的调整量 $delta 太小，可能导致无效调整，跳过该请求")
            break
          }
          
          val oldLabelX = labelX(i)
          
          labelX(i) -= delta
          for (j <- 0 until m if visited(j)) {
            labelY(j) += delta
          }
          
          if (math.abs(oldLabelX - labelX(i)) < 1e-10) {
            logging.warn(this, s"请求 $i 的标号调整无效，跳过该请求")
            break
          }
          
          visited = Array.fill(m)(false)
        }
      }
    }
    
    // 构造返回结果
    val matchedPairs = matchY.zipWithIndex
      .filter(_._1 != -1)
      .map(p => (p._1, p._2))
      .toMap
    
    val unmatchedContainers = (0 until m)
      .filter(j => matchY(j) == -1)
      .toSet
    
    // 输出更详细的调试信息（保持原有日志）
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