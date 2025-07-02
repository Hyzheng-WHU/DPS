package org.apache.openwhisk.core.containerpool.docker

import akka.actor.ActorSystem
import akka.http.scaladsl.Http          // 新增
import akka.http.scaladsl.model._       // 新增
import akka.stream.Materializer         // 新增
import scala.concurrent.ExecutionContext
import scala.concurrent.duration._
import org.apache.openwhisk.common.Logging
import scala.util.matching.Regex
import spray.json._                     // 新增
import scala.util.{Success, Failure}    // 新增

// 定义发送到ContainerDbInfoManager的数据格式
case class MemoryUsageRequest(containerMemoryMap: Map[String, Double], timestamp: String)

object MemoryUsageJsonProtocol extends DefaultJsonProtocol {
  implicit val memoryUsageRequestFormat = jsonFormat2(MemoryUsageRequest)
}

object DockerContainerMemoryMonitor {
  import MemoryUsageJsonProtocol._

  private val StatsCmd = Seq(
    "docker", "stats", "--no-stream",
    "--format", "{{.Container}}\t{{.Name}}\t{{.MemUsage}}"
  )

  private val dockerIPConfigPath = "/db/dockerIP.json"
  private def getSchedulerIP(logging: Logging): String = {
    try {
      val source = scala.io.Source.fromFile(dockerIPConfigPath)
      val jsonContent = try source.mkString finally source.close()
      val json = jsonContent.parseJson.asJsObject
      val schedulerIP = json.fields("scheduler0IP").convertTo[String]
      logging.info(this, s"Read scheduler IP from config: $schedulerIP")
      schedulerIP
    } catch {
      case ex: Exception =>
        logging.warn(this, s"Failed to read scheduler IP from $dockerIPConfigPath: ${ex.getMessage}. Using localhost as fallback")
        "localhost" // 回退到localhost
    }
  }

  /** 
    * 启动定时内存监控，每5秒采集一次docker容器内存并处理
    * @param logging 项目的Logging对象
    */
  def start(logging: Logging)(implicit system: ActorSystem, ec: ExecutionContext): Unit = {
    implicit val materializer: Materializer = Materializer(system)  // 新增
    
    // 读取scheduler IP地址
    val schedulerIP = getSchedulerIP(logging)
    val dbInfoManagerUrl = s"http://$schedulerIP:6789/container/memoryUsage"
    logging.info(this, s"ContainerDbInfoManager URL: $dbInfoManagerUrl")
    
    system.scheduler.scheduleAtFixedRate(0.seconds, 2.seconds) { () =>
      try {
        import scala.sys.process._
        val output = StatsCmd.!!
        val timestamp = java.time.Instant.now().toString
        val guestContainers = output
          .split("\n")
          .filter { line =>
            val fields = line.split('\t')
            fields.length >= 3 && fields(1).contains("guest")
          }
          .flatMap { line =>
            val fields = line.split('\t')
            if (fields.length >= 3) {
              val id = fields(0)
              val name = fields(1)
              val memUsage = fields(2)
              parseMemory(memUsage).map(mem => id -> mem)
            } else None
          }
          .toMap
          
        if (guestContainers.nonEmpty) {
          logging.warn(this, s"[$timestamp] guest容器内存Map: $guestContainers")
          sendToDbInfoManager(guestContainers, timestamp, dbInfoManagerUrl, logging)  // 修改：传入URL
        } else {
          logging.debug(this, s"[$timestamp] No guest containers found")  // 新增
        }
      } catch {
        case ex: Exception =>
          logging.warn(this, s"Failed to collect docker memory stats: ${ex.getMessage}")
      }
    }
    logging.info(this, "Started Docker guest container memory monitor (every 5s)")
  }

  /** 解析内存占用字符串，返回MiB数值 */
  private def parseMemory(memField: String): Option[Double] = {
    // memField格式如："4.73MiB / 512MiB"
    val memPart = memField.split('/').head.trim
    val memPattern: Regex = """([\d.]+)([KMG]iB)""".r
    memPart match {
      case memPattern(num, unit) =>
        val value = num.toDouble
        val mib = unit match {
          case "KiB" => value / 1024
          case "MiB" => value
          case "GiB" => value * 1024
          case _     => value
        }
        Some(mib)
      case _ => None
    }
  }

  private def sendToDbInfoManager(containerMap: Map[String, Double], timestamp: String, dbInfoManagerUrl: String, logging: Logging)
                                 (implicit system: ActorSystem, ec: ExecutionContext, materializer: Materializer): Unit = {
    
    val request = MemoryUsageRequest(containerMap, timestamp)
    val jsonEntity = HttpEntity(ContentTypes.`application/json`, request.toJson.prettyPrint)
    
    val httpRequest = HttpRequest(
      method = HttpMethods.POST,
      uri = dbInfoManagerUrl,
      entity = jsonEntity
    )
    
    // Fire-and-forget: 发送请求但不等待响应
    Http().singleRequest(httpRequest).onComplete {
      case Success(response) =>
        if (response.status.isSuccess()) {
          logging.debug(this, s"Memory usage data sent successfully for ${containerMap.size} containers to $dbInfoManagerUrl")
        } else {
          logging.warn(this, s"Failed to send memory usage data. Status: ${response.status}, URL: $dbInfoManagerUrl")
        }
        // 释放响应实体以避免内存泄漏
        response.discardEntityBytes()
        
      case Failure(ex) =>
        ex match {
          case _: java.net.ConnectException =>
            logging.warn(this, s"Cannot connect to ContainerDbInfoManager at $dbInfoManagerUrl. Is the scheduler service running?")
          case _ =>
            logging.warn(this, s"Failed to send HTTP request to $dbInfoManagerUrl: ${ex.getClass.getSimpleName}: ${ex.getMessage}")
        }
    }
  }

  /** 发送给scheduler的接口，实际项目请替换为真实实现 */
  def readyForScheduler(containerMap: Map[String, Double]): Unit = {
    // TODO: 替换为实际发送逻辑
    // 示例：println 或集成到消息队列、Akka消息等
  }
}