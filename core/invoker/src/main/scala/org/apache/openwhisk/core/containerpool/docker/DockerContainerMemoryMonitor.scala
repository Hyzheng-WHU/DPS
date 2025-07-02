package org.apache.openwhisk.core.containerpool.docker

import akka.actor.ActorSystem
import scala.concurrent.ExecutionContext
import scala.concurrent.duration._
import org.apache.openwhisk.common.Logging
import scala.util.matching.Regex

object DockerContainerMemoryMonitor {
  private val StatsCmd = Seq(
    "docker", "stats", "--no-stream",
    "--format", "{{.Container}}\t{{.Name}}\t{{.MemUsage}}"
  )

  /** 
    * 启动定时内存监控，每5秒采集一次docker容器内存并处理
    * @param logging 项目的Logging对象
    */
  def start(logging: Logging)(implicit system: ActorSystem, ec: ExecutionContext): Unit = {
    system.scheduler.scheduleAtFixedRate(0.seconds, 5.seconds) { () =>
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
        logging.warn(this, s"[$timestamp] guest容器内存Map: $guestContainers")
        readyForScheduler(guestContainers)
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

  /** 发送给scheduler的接口，实际项目请替换为真实实现 */
  def readyForScheduler(containerMap: Map[String, Double]): Unit = {
    // TODO: 替换为实际发送逻辑
    // 示例：println 或集成到消息队列、Akka消息等
  }
}