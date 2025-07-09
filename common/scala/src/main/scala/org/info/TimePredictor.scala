package org.apache.openwhisk.core.scheduler.container

import org.apache.openwhisk.common._
import scala.collection.mutable
import java.time.Instant
import java.nio.file.{Files, Paths}
import scala.collection.mutable
import scala.collection.concurrent.TrieMap
import java.io.{File, FileWriter, PrintWriter}


object TimePredictor {
  // 调度器考虑的容器状态列表
  val ConsideredStates: List[String] = List("warm", "prewarm")
  // List() // 只有冷启动
  // List("warm", "prewarm") // 只有冷热启动
  // List("warm", "working", "loading", "prewarm") // 完全wait


  // 记录activationId和creationId的对应关系
  private val creationIdToActivationIdMap = TrieMap[String, String]()

  // 记录waiting请求初次决定waiting的时间戳，用于计算waiting的总时间
  private val waitingStartTimeMap = TrieMap[String, Double]()
  private val waitingTimeFilePath = "/db/waiting_times.csv"

  // 正在被waiting请求等待的容器，这些容器不能再次被用来预测和分配
  var waitingContainerIds: Set[String] = Set.empty[String]

  // 等待的容器已经返回的请求，这些请求通过这个Map来锁定容器
  val waitingCreationIdToContainerIds = mutable.Map[String, String]()

  // 假设冷启动的时间成本(s)
  val coldStartContainerInitTime = 2.660
  val containerReturnTimeCost = 0.5

  // 假设每个实例的带宽为定值(MByte/s)
  val defaultBandwidthBps: Long = 14596101L

  // 定义表大小的Map，单位为Byte
  // val Tpch1gTableSize: Map[String, Long] = Map(
  //   "customer" -> 24196144L,
  //   "lineitem" -> 753862072L,
  //   "nation"   -> 2199L,
  //   "orders"   -> 170452161L,
  //   "partsupp" -> 118184616L,
  //   "part"     -> 23935125L,
  //   "region"   -> 384L,
  //   "supplier" -> 1399184L
  // )

  // val Tpcds1gTableSize: Map[String, Long] = Map(
  //   "call_center" -> 1888L,
  //   "catalog_page" -> 1633796L,
  //   "catalog_returns" -> 31750651L,
  //   "catalog_sales" -> 294263819L,
  //   "customer_address" -> 5392526L,
  //   "customer_demographics" -> 80660096L,
  //   "customer" -> 13203896L,
  //   "date_dim" -> 10317438L,
  //   "dbgen_version" -> 111L,
  //   "household_demographics" -> 151653L,
  //   "income_band" -> 328L,
  //   "inventory" -> 236495557L,
  //   "item" -> 5108290L,
  //   "promotion" -> 37135L,
  //   "reason" -> 1339L,
  //   "ship_mode" -> 1113L,
  //   "store_returns" -> 81074942L,
  //   "store_sales" -> 386079402L,
  //   "store" -> 3162L,
  //   "time_dim" -> 5107780L,
  //   "warehouse" -> 586L,
  //   "web_page" -> 5759L,
  //   "web_returns" -> 19470799L,
  //   "web_sales" -> 145326548L,
  //   "web_site" -> 8726L
  // )

  val Tpch1gParquetSize : Map[String, Long] = Map(
    "customer" -> 12642616L,
    "lineitem" -> 270461256L,
    "nation" -> 2319L,
    "orders" -> 60876870L,
    "partsupp" -> 45379198L,
    "part" -> 6945026L,
    "region" -> 1005L,
    "supplier" -> 805781L,
  )

  val Tpch3gParquetSize : Map[String, Long] = Map(
    "customer" -> 37908904L,
    "lineitem" -> 813502596L,
    "nation"   -> 2319L,
    "orders"   -> 184526603L,
    "partsupp" -> 136046901L,
    "part"     -> 20846723L,
    "region"   -> 1005L,
    "supplier" -> 2411872L,
  )

  val Tpch5gParquetSize: Map[String, Long] = Map(
    "customer" -> 63174135L,
    "lineitem" -> 1356111893L,
    "nation"   -> 2319L,
    "orders"   -> 308705060L,
    "partsupp" -> 226757651L,
    "part"     -> 34740363L,
    "region"   -> 1005L,
    "supplier" -> 4021119L,
  )

  val TargetBenchmark = Tpch5gParquetSize

  def initWaitingTimeRecorder()(implicit logging: Logging): Unit = {
    logging.warn(this, s"调度器考虑的容器状态列表: $ConsideredStates")
    logging.info(this, "正在初始化等待时间记录器...")
    
    val file = new File(waitingTimeFilePath)
    
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
          val baseOldFileName = "old_waiting_time_"
          
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
          logging.error(this, s"处理已存在的等待时间文件时发生错误: ${e.getMessage}")
      } finally {
        if (source != null) {
          source.close()
        }
      }
    }
    
    // 创建新的等待时间文件
    val writer = new PrintWriter(file)
    try {
      writer.println("activation_id,wait_schedule_time")
      logging.info(this, s"已创建新的等待时间文件: ${file.getName}")
    } finally {
      writer.close()
    }
  }
  
  def updateCreationIdToActivationId(creationId: String, activationId: String): Unit = {
    creationIdToActivationIdMap.put(creationId, activationId)
  }

  // 记录请求初次决定waiting的时间
  private def recordWaitingStartTime(creationId: String)(implicit logging: Logging): Unit = {
    if (!waitingStartTimeMap.contains(creationId)) {
      val currentTime = System.currentTimeMillis() / 1000.0  // 转换为秒
      waitingStartTimeMap.put(creationId, currentTime)
      logging.info(this, s"Request $creationId started waiting at $currentTime")
    }
  }

  // 计算waiting总时间，并清理记录
  def calculateAndCleanWaitingTime(creationId: String)(implicit logging: Logging): Unit = {
    try {
      val maybeActivationId = creationIdToActivationIdMap.get(creationId)
      
      val waitingTime = waitingStartTimeMap.get(creationId) match {
        case Some(startTime) =>
          val currentTime = System.currentTimeMillis() / 1000.0
          val timeDiff = currentTime - startTime
          waitingStartTimeMap.remove(creationId)
          creationIdToActivationIdMap.remove(creationId)
          logging.info(this, s"请求creationId: $creationId (activationId: ${maybeActivationId.getOrElse("unknown")}) 等待了 $timeDiff s")
          timeDiff
        case None =>
          0.0
      }
      
      maybeActivationId.foreach { activationId =>
        val file = new File(waitingTimeFilePath)
        
        if (!file.exists()) {
          // 文件不存在，创建新文件并写入表头和数据
          val writer = new PrintWriter(file)
          try {
            writer.println("activation_id,wait_schedule_time")
            writer.println(s"$activationId,$waitingTime")
            logging.info(this, s"创建新文件 ${file.getName}")
          } finally {
            writer.close()
          }
        } else {
          // 文件存在，追加数据
          val writer = new PrintWriter(new FileWriter(file, true))
          try {
            writer.println(s"$activationId,$waitingTime")
          } finally {
            writer.close()
          }
        }
      }
    } catch {
      case e: Exception =>
        logging.error(this, s"Error while processing waiting time: ${e.getMessage}")
    }
  }

  /**
   * 预测下载时间的函数
   * @param neededTables 需要的表列表
   * @param existingTables 已有的表列表
   * @return 缺失表的下载总时间，单位为秒
   */
  def predictDownloadTime(neededTables: List[String], existingTables: List[String], containerId: String = "coldStart")(implicit logging: Logging): Double = {
    // 添加默认带宽值
    // val defaultBandwidth: Long = 5000000L // 5MB/s as default

    // val bandwidthBps: Long = ContainerDbInfoManager.dbSizeGrowthRateMap
    //   .get(containerId)
    //   .map(_.toLong) // 转换为 Long 类型
    //   .getOrElse((ContainerDbInfoManager.dbSizeGrowthRateMap.values.sum / ContainerDbInfoManager.dbSizeGrowthRateMap.size).toLong)

    // logging.info(this, s"容器 $containerId 的带宽为 $bandwidthBps Byte/s")

    // val bandwidthBps: Long = (ContainerDbInfoManager.dbSizeGrowthRateMap.values.sum / ContainerDbInfoManager.dbSizeGrowthRateMap.size).toLong

    val bandwidthBps: Long = {
      val calculatedBandwidth = if (ContainerDbInfoManager.dbSizeGrowthRateMap.isEmpty) {
        TimePredictor.defaultBandwidthBps
      } else {
        (ContainerDbInfoManager.dbSizeGrowthRateMap.values.sum / ContainerDbInfoManager.dbSizeGrowthRateMap.size).toLong
      }
      // 确保带宽至少有一个最小值
      math.max(calculatedBandwidth, TimePredictor.defaultBandwidthBps)
    }

    // 找出缺失的表
    val missingTables = neededTables.diff(existingTables)
    
    // 计算缺失表的总大小（字节），并转换为bit
    val totalSizeBytes = missingTables.flatMap(TargetBenchmark.get).sum

    // 计算并返回下载时间（秒）
    totalSizeBytes.toDouble / bandwidthBps
  }

  // 为了兼容更新与查询的功能，需要一个返回值
  def updateWaitingContainerIds(newWaitingContainerIds: Set[String])(implicit logging: Logging): Unit = {
    waitingContainerIds = newWaitingContainerIds
    logging.info(this, s"更新waitingContainerIds, 长度为: ${waitingContainerIds.size}, 新值为: $waitingContainerIds")
  }

  // 添加waitingContainerIds
  def addWaitingContainerIds(newWaitingContainerId: String)(implicit logging: Logging): Unit = {
    waitingContainerIds += newWaitingContainerId
    logging.info(this, s"新增waitingContainerId: $newWaitingContainerId, 长度为: ${waitingContainerIds.size}, 新值为: $waitingContainerIds")
  }

  // 将containerId从被等待的集合中移除，否则重新调度时会跳过它
  def removeWaitingContainerIds(newWaitingContainerId: String)(implicit logging: Logging): Unit = {
    waitingContainerIds -= newWaitingContainerId
    logging.info(this, s"减少waitingContainerId: $newWaitingContainerId, 长度为: ${waitingContainerIds.size}, 新值为: $waitingContainerIds")
  }

  // 添加元素的函数
  def addWaitingCreationIdToContainerIds(creationId: String, containerId: String)(implicit logging: Logging): Unit = {
    waitingCreationIdToContainerIds += (creationId -> containerId)
    logging.info(this, s"新增waitingCreationIdToContainerIds, 更新后值为: $waitingCreationIdToContainerIds")
  }

  // 删除元素的函数
  def removeWaitingCreationIdToContainerIds(creationId: String)(implicit logging: Logging): Option[String] = {
    waitingCreationIdToContainerIds.remove(creationId)
    // 如果不存在，会直接返回None
  }

  def predictWaitTimeAndGetOptimal(creationId: String, tablesNeeded: List[String], waitingAWarmContainer: Boolean = false)(implicit logging: Logging): (String, Double, Boolean) = synchronized {
    logging.info(this, s"正在为creationId: $creationId 选择最佳容器")
    val coldStartDownloadTime = predictDownloadTime(tablesNeeded, List.empty[String])
    val coldStartTotalTime = coldStartDownloadTime + coldStartContainerInitTime
    logging.info(this, s"Predicted total wait time for cold start: $coldStartTotalTime = coldStartDownloadTime: $coldStartDownloadTime + coldStartContainerInitTime: $coldStartContainerInitTime")

    // 如果 creationId 已经在 waitingCreationIdToContainerIds 中，说明该请求已经在等待某个已返回的热容器，将这个热容器变为可用
    waitingCreationIdToContainerIds.remove(creationId).foreach { containerId =>
      removeWaitingContainerIds(containerId)
    }

    val containerWaitTimes = predictWaitTime(tablesNeeded, waitingAWarmContainer)
    
    // 构建候选策略列表，包括冷启动的总时间
    val candidateStrategies = if (waitingAWarmContainer) {
      containerWaitTimes.map { case (id, time, isWarm) => (id, time, isWarm) }
    } else {
      containerWaitTimes.map { case (id, time, isWarm) => (id, time, isWarm) } :+ (("coldStart", coldStartTotalTime, false))
    }
    logging.info(this, s"Candidate strategies (containerId, totalTime): $candidateStrategies")

    val dbInfo = ContainerDbInfoManager.getDbInfo()
    val filteredStrategies = candidateStrategies.filterNot { case (id, _, _) => 
      // 过滤掉冷启动策略
      id == "coldStart" ||
      // 过滤掉预热状态的容器
      containerWaitTimes.exists { case (containerId, _, _) => 
        containerId == id && dbInfo.get(containerId).exists(_.state == "prewarm")
      }
    }
  
    logging.info(this, s"Filtered strategies (containerId, totalTime): $filteredStrategies")

    // 如果过滤后还有其他选择（warm或working容器），就使用这些优选策略；否则考虑所有选项
    // val strategiesToConsider = if (filteredStrategies.nonEmpty) filteredStrategies else candidateStrategies
    val strategiesToConsider = candidateStrategies

    // 选择等待时间最短的策略，包括冷启动、warm 容器和 working 容器的等待时间
    if (strategiesToConsider.isEmpty) {
      logging.error(this, s"waitingAWarmContainer: $waitingAWarmContainer , 但是选择了冷启动! No available strategies found! containerWaitTimes: $containerWaitTimes, candidateStrategies: $candidateStrategies, filteredStrategies: $filteredStrategies")
      ("coldStart", coldStartTotalTime, false)
    } else {
      // strategiesToConsider.minBy { case (_, totalTime, _) => totalTime }
        // 找出最短等待时间
      val minWaitTime = strategiesToConsider.map(_._2).min
      
      // 筛选出所有具有最短等待时间的容器
      val shortestWaitTimeStrategies = strategiesToConsider.filter(_._2 == minWaitTime)
      
      // 从最短等待时间的容器中筛选出 warm 状态的容器
      val warmStrategies = shortestWaitTimeStrategies.filter { case (containerId, _, _) =>
        dbInfo.get(containerId).exists(_.state == "warm")
      }

      // 选择最终策略，如果有 warm 状态的容器，从中选择时间戳最新的
      val selectedStrategy = if (warmStrategies.nonEmpty) {
        warmStrategies.maxBy { case (containerId, _, _) => 
          dbInfo.get(containerId).flatMap(_.updateStateTimestamp).getOrElse(0.0)
        }
      } else {
        // 如果没有 warm 状态的容器，从所有最短等待时间的容器中选择时间戳最新的
        shortestWaitTimeStrategies.maxBy { case (containerId, _, _) => 
          dbInfo.get(containerId).flatMap(_.updateStateTimestamp).getOrElse(0.0)
        }
      }

      // 如果选择了warm容器，立即更新状态为locked
      selectedStrategy match {
        case (containerId, time, true) => 
          ContainerDbInfoManager.updateStateToLocked(containerId)
          logging.info(this, s"热容器 $containerId 被选中并立即锁定")
          // 如果选择了warm容器，计算总waiting时间（如果有）
          calculateAndCleanWaitingTime(creationId)
          
        case (containerId, time, false) if containerId != "coldStart" =>
          // 如果选择了working容器（waiting策略），记录开始时间
          recordWaitingStartTime(creationId)

        case ("coldStart", _, false) =>
          // 如果选择了冷启动策略，计算总waiting时间（实际为0）
          calculateAndCleanWaitingTime(creationId)

        case _ => // 其他状态不需要处理
          logging.error(this, s"未知的策略选择: $selectedStrategy")
      }

    selectedStrategy
    }
  }

  // 给newWaitingContainerIds一个默认值，使得当非调度器调用时，可以使用TimePredictor自己持有的值计算更新
  def predictWaitTime(tablesNeeded: List[String], waitingAWarmContainer: Boolean = false)(implicit logging: Logging): Seq[(String, Double, Boolean)] = synchronized {
    // waitingContainerIds = newWaitingContainerIds
    // 尽量后置dbInfo的读取，防止读到更新前的值
    logging.info(this, s"waitingAWarmContainer: $waitingAWarmContainer")
    val dbInfo = ContainerDbInfoManager.getDbInfo()
    // logging.info(this, s"dbInfo contents: $dbInfo")

    // 遍历 dbInfo 中的所有容器
    val containerWaitTimes = dbInfo.flatMap {
      case (containerId, info) =>
        // 检查是否在 waitingQueue 中，如果是则跳过
        if (waitingContainerIds.contains(containerId)) {
          logging.info(this, s"Skipping container $containerId as it is already in waitingQueue.")
          None  // 跳过该容器
        } else {
          // logging.info(this, s"inspect container: $containerId, it is $info.state")
          info.state match {
            case state if state == "warm" && ConsideredStates.contains(state) =>
              // 对于 warm 容器，只计算缺失表的下载时间
              val existingTables = info.tables
              val warmDownloadTime = predictDownloadTime(tablesNeeded, existingTables, containerId)
              // logging.info(this, s"Predicted download time for warm container $containerId: $warmDownloadTime ms")
              Some((containerId, warmDownloadTime, true))

            // 加上if !waitingAWarmContainer，因为如果这个msg等待的容器已返回，则不再为它考虑工作中的容器，避免再次等待，影响公平性
            case state if state == "working" && ConsideredStates.contains(state) && !waitingAWarmContainer =>
              // 对于 working 容器，计算缺失表的下载时间和剩余运行时间
              val existingTables = info.tables
              val lackTableDownloadTime = predictDownloadTime(tablesNeeded, existingTables, containerId)
              
              // 获取并验证 updateStateTimestamp 和 predictWorkingTime
              val elapsedTimeOpt = info.updateStateTimestamp.map { timestamp =>
                (System.currentTimeMillis() / 1000.0) - timestamp
              }
              val predictWorkingTimeOpt = info.predictWorkingTime

              // 计算 remainingWorkTime，如果有 missing 值则跳过
              (for {
                elapsedTime <- elapsedTimeOpt
                predictWorkingTime <- predictWorkingTimeOpt
              } yield {
                val timeLeft = predictWorkingTime - elapsedTime
                // if (timeLeft < - 10000) {
                //   logging.warn(this, s"Remaining work time for container $containerId is negative: $timeLeft ms.")
                // }
                val remainingWorkTime = math.max(timeLeft, 0.0) // 确保剩余时间非负

                // 计算总等待时间并返回
                val workingTotalTime = lackTableDownloadTime + remainingWorkTime + containerReturnTimeCost
                // logging.info(this, s"Predicted total wait time for working container $containerId: $workingTotalTime ms")
                logging.info(this, s"working 态容器: $containerId : 总等待时间($workingTotalTime) = 预计剩余执行时间($predictWorkingTime) + 缺失表加载时间($lackTableDownloadTime) + 容器返回时间($containerReturnTimeCost)")
                (containerId, workingTotalTime, false)
              }).orElse {
              logging.error(this, s"Missing data for container $containerId. dbInfo: $info.")
              None
            }

            case state if state == "prewarm" && ConsideredStates.contains(state) =>
              // 对于预热状态的容器，设置一个略低于冷启动的时间，实际上不应该是空列表
              val existingTables = info.tables
              val prewarmDownloadTime = predictDownloadTime(tablesNeeded, existingTables, containerId)

              val prewarmWaitTime = (coldStartContainerInitTime -1) + prewarmDownloadTime
              Some((containerId, prewarmWaitTime, false))

            // 由于loading时间预测不准确，暂时注释掉不考虑
            // 通过限制containerId的长度来跳过那些刚创建、还没有containerId的条目
            case state if state == "loading" && ConsideredStates.contains(state) && !waitingAWarmContainer && containerId.length == 64 =>

              val existingTables = info.tables
              val currentTime = Instant.now.getEpochSecond

              val dbPath = s"/db/${info.dbName}"

              // 获取当前文件大小
              val currentDbSize = {
                val filePath = Paths.get(dbPath)
                if (Files.exists(filePath)) Files.size(filePath).toDouble else info.dbSizeLastRecord
              }

              // 根据 tables 字段计算目标大小
              val targetDbSize = existingTables
                .map(table => TargetBenchmark.getOrElse(table, 0L)) // 获取表的大小，如果表未定义则默认为 0
                .sum

              if (targetDbSize * 1.2 < currentDbSize){
                logging.warn(this, s"Container $containerId has already loaded more data than needed. targetDbSize: $targetDbSize, currentDbSize: $currentDbSize")
              }

              // 计算还需要加载的大小
              val remainingSize = math.max(targetDbSize - currentDbSize, 0L)
              // logging.info(this, s"loading container: $containerId, remainingSize: $remainingSize = targetDbSize: $targetDbSize - currentDbSize: $currentDbSize")

              // 获取带宽（带宽的单位为字节每秒）
              // val bandwidthBps: Long = ContainerDbInfoManager.dbSizeGrowthRateMap
              //   .get(containerId)
              //   .map(_.toLong) // 转换为 Long 类型
              //   .getOrElse((ContainerDbInfoManager.dbSizeGrowthRateMap.values.sum / ContainerDbInfoManager.dbSizeGrowthRateMap.size).toLong)
              // 修改loading状态的带宽计算，添加保护机制
              val bandwidthBps: Long = {
                val calculatedBandwidth = ContainerDbInfoManager.dbSizeGrowthRateMap
                  .get(containerId)
                  .map(_.toLong)
                  .getOrElse {
                    if (ContainerDbInfoManager.dbSizeGrowthRateMap.nonEmpty) {
                      (ContainerDbInfoManager.dbSizeGrowthRateMap.values.sum / ContainerDbInfoManager.dbSizeGrowthRateMap.size).toLong
                    } else {
                      TimePredictor.defaultBandwidthBps
                    }
                  }
                
                // 确保带宽至少有一个最小值
                math.max(calculatedBandwidth, TimePredictor.defaultBandwidthBps)
              }

              // logging.info(this, s"loading container: $containerId, bandwidth: $bandwidthBps Byte/s")
              // 计算剩余加载时间（秒）
              val remainingLoadTime = remainingSize.toDouble / bandwidthBps
              logging.info(this, s"loading 态容器 $containerId : 剩余加载时间  $remainingLoadTime = remainingSize: $remainingSize / bandwidthBps: $bandwidthBps")
              val lackTableDownloadTime = predictDownloadTime(tablesNeeded, existingTables, containerId)
              // logging.info(this, s"loading container: $containerId, tablesNeeded: $tablesNeeded, existingTables: $existingTables, lackTableDownloadTime: $lackTableDownloadTime")

              // 获取并验证 predictWorkingTime
              val predictWorkingTimeOpt = info.predictWorkingTime

              // 计算 remainingLoadTime 和 totalWaitTime，如果有 missing 值则跳过
              (for {
                predictWorkingTime <- predictWorkingTimeOpt
              } yield {
                // 计算容器释放的总预测时间
                val totalWaitTime = remainingLoadTime + predictWorkingTime + lackTableDownloadTime + containerReturnTimeCost

                logging.info(this, s"loading 态容器 $containerId : 总等待时间($totalWaitTime) = 剩余加载时间($remainingLoadTime) + 预计执行时间($predictWorkingTime) + 缺失表加载时间($lackTableDownloadTime) + 容器返回时间($containerReturnTimeCost)")

                (containerId, totalWaitTime, false)
              }).orElse {
                logging.error(this, s"Missing data for loading container $containerId. dbInfo: $info.")
                None
              }

      case _ =>
        None // 非 warm 或 working 状态的容器跳过 
          }
        }
    }.toSeq
    containerWaitTimes
  }
}