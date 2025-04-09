// 测试发现，似乎只有跟FunctionPullingContainerPool.scala在同一个package中才能发送Remove消息
package org.apache.openwhisk.core.containerpool.v2

import org.apache.openwhisk.common._
import akka.actor.{ActorRef, ActorSystem}
import akka.http.scaladsl.Http
import akka.http.scaladsl.model._
import akka.http.scaladsl.server.Directives._
import akka.stream. Materializer
import com.typesafe.config.ConfigFactory
import spray.json._

import scala.collection.concurrent.TrieMap
import scala.concurrent.ExecutionContextExecutor
import scala.util.{Failure, Success, Try}

import scala.sys.process._
import akka.http.scaladsl.unmarshalling.Unmarshal

case object Remove

// JSON Protocol for serializing/deserializing HTTP requests and responses
object WarmContainerJsonProtocol extends DefaultJsonProtocol {
  case class RemoveRequest(containerId: String)
  case class RemoveResponse(status: String, message: String)

  implicit val removeRequestFormat: RootJsonFormat[RemoveRequest] = jsonFormat1(RemoveRequest)
  implicit val removeResponseFormat: RootJsonFormat[RemoveResponse] = jsonFormat2(RemoveResponse)
}

object WarmContainerManagerHttp {
  import WarmContainerJsonProtocol._

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
  implicit val materializer: Materializer = Materializer(system)
  implicit val executionContext: ExecutionContextExecutor = system.dispatcher

  private val containerIdToActorRef: TrieMap[String, ActorRef] = TrieMap()

  var scheduler0IP: Option[String] = None
  var invoker0IP: Option[String] = None

  def init()(implicit logging: Logging): Unit = {
    startHttpServer()
    getAndSendIP()
  }

  private def startHttpServer()(implicit logging: Logging): Unit = {
    logging.info(this, "启动 WarmContainerManagerHttp 服务")
    val route =
      path("warmContainerManager" / "removeById") { // API 路径
        post {
          entity(as[String]) { jsonBody => // 接收 JSON 请求体
            val parsedRequest = Try(jsonBody.parseJson.convertTo[RemoveRequest]) // 转换为数据模型
            parsedRequest match {
              case Success(request) =>
                logging.info(this, s"收到请求: $parsedRequest")
                val containerId = request.containerId
                if (containerId.nonEmpty) {
                  WarmContainerManager.sendRemoveMessage(containerId)
                  val response = RemoveResponse("success", s"Container with ID $containerId removed successfully.")
                  complete(HttpEntity(ContentTypes.`application/json`, response.toJson.prettyPrint))
                } else {
                  val response = RemoveResponse("error", "Missing containerId.")
                  complete(HttpEntity(ContentTypes.`application/json`, response.toJson.prettyPrint))
                }

              case Failure(ex) =>
                val response = RemoveResponse("error", s"Invalid request: ${ex.getMessage}")
                complete(HttpEntity(ContentTypes.`application/json`, response.toJson.prettyPrint))
            }
          }
        }
      }

    // 监听端口 5678
    Http().newServerAt("0.0.0.0", 5678).bind(route).onComplete {
      case Success(binding) =>
        val address = binding.localAddress
        logging.info(this, s"Server is listening on ${address.getHostString}:${address.getPort}")
      case Failure(ex) =>
        logging.error(this, s"Failed to bind HTTP endpoint: ${ex.getMessage}")
        system.terminate()
    }
  }
  
  private def getContainerIP(containerName: String)(implicit logging: Logging): Option[String] = {
    try {
      // 使用 docker inspect 获取容器的 IP 地址
      val command = Seq("sh", "-c", s"docker inspect -f '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}' $containerName")
      val ip = command.!!.trim // 执行命令
      logging.info(this, s"IP of $containerName is: $ip")
      if (ip.nonEmpty) Some(ip) else None
    } catch {
      case ex: Exception =>
        logging.error(this, s"Error fetching IP for container $containerName: ${ex.getMessage}")
        None
    }
  }

  private def getAndSendIP()(implicit logging: Logging): Unit = {
    scheduler0IP = getContainerIP("scheduler0")
    invoker0IP = getContainerIP("invoker0")
    logging.info(this, s"scheduler0IP: $scheduler0IP")
    logging.info(this, s"invoker0IP: $invoker0IP")

    // 检查 invoker0IP 是否存在
    invoker0IP match {
      case Some(ip) =>
        // 构建 HTTP 请求
        scheduler0IP match {
          case Some(schedulerIP) =>
            val url = s"http://$schedulerIP:6788/updateInvokerIP"
            // s"""{"invoker0IP": "$ip"}"""
            val body = s"""{"invoker0IP": "$ip"}"""
            val requestEntity = HttpEntity(ContentTypes.`application/json`, body)

            val httpRequest = HttpRequest(
              method = HttpMethods.POST,
              uri = url,
              entity = requestEntity
            )

            // 发送 HTTP 请求
            Http().singleRequest(httpRequest).map { response =>
              response.status match {
                case StatusCodes.OK =>
                  logging.info(this, s"Successfully sent invoker0IP to $url")
                case _ =>
                  Unmarshal(response.entity).to[String].map { responseBody =>
                    logging.error(this, s"Failed to send invoker0IP: $responseBody")
                  }
              }
            }.recover {
              case ex: Exception =>
                logging.error(this, s"Error occurred while sending invoker0IP: ${ex.getMessage}")
            }
          case None =>
            logging.error(this, "scheduler0IP is not available")
        }

      case _ =>
        logging.error(this, "invoker0IP is not available")
    }
  }
}

object WarmContainerManager {

  // 使用线程安全的 TrieMap 维护映射关系
  private val containerIdToActorRef: TrieMap[String, ActorRef] = TrieMap()
  
  // 增加一个映射
  def addMapping(containerId: String, actorRef: ActorRef)(implicit logging: Logging): Unit = {
    containerIdToActorRef.put(containerId, actorRef)
    logging.info(this, s"containerIdToActorRef 新增: $containerId -> $actorRef")

    // val test1=RequestRecordManager.getRequestCountWithinDuration(2)
    // logging.info(this, s"test1: $test1")
    // val test2=ContainerDbInfoManager.findDbNameByContainerId(containerId)
    // logging.info(this, s"test2: $test2")
  }

  // 删除一个映射
  private def removeMapping(containerId: String)(implicit logging: Logging): Unit = {
    containerIdToActorRef.remove(containerId)
    logging.info(this, s"containerIdToActorRef 删除容器: $containerId")
  }

  /**
   * 根据 containerId 向对应的 Actor 发送 Remove 消息
   *
   * @param containerId 容器ID
   * @return 是否找到并成功发送消息
   */
  def sendRemoveMessage(containerId: String)(implicit logging: Logging): Unit = {
    containerIdToActorRef.get(containerId) match {
      case Some(actorRef) =>
        logging.info(this, s"向容器 $containerId 对应的 actorRef $actorRef 发送 Remove 消息删除容器")
        actorRef ! Remove // 发送 Remove 消息
      case None =>
        logging.error(this, s"ContainerId $containerId not found in containerIdToActorRef $containerIdToActorRef.")
    }

    removeMapping(containerId)
  }
}