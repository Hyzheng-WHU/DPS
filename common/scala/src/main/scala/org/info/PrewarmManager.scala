package org.apache.openwhisk.core.scheduler.container

import org.apache.openwhisk.common._
import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.model._
import akka.http.scaladsl.model.headers._
import akka.http.scaladsl.unmarshalling.Unmarshal
import akka.stream.Materializer
import scala.concurrent.Future

import com.typesafe.config.ConfigFactory
import java.util.Base64

import akka.http.scaladsl.ConnectionContext
import javax.net.ssl.{SSLContext, TrustManager, X509TrustManager}
import java.security.cert.X509Certificate
import java.security.SecureRandom

object PrewarmManager {
  // 自定义配置
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
        client {
          parsing.illegal-header-warnings = off
          ssl-config {
            loose {
              acceptAnyCertificate = true
              disableHostnameVerification = true
            }
          }
        }
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

  // 创建信任所有证书的 TrustManager
  private val trustAllCerts = Array[TrustManager](new X509TrustManager {
    override def checkClientTrusted(chain: Array[X509Certificate], authType: String): Unit = ()
    override def checkServerTrusted(chain: Array[X509Certificate], authType: String): Unit = ()
    override def getAcceptedIssuers: Array[X509Certificate] = Array.empty
  })

  // 配置 SSLContext
  private val sslContext: SSLContext = {
    val context = SSLContext.getInstance("TLS")
    context.init(null, trustAllCerts, new SecureRandom())
    context
  }

  // 配置 HTTPS connection context
  private val httpsConnectionContext = ConnectionContext.httpsClient { (host, port) =>
    val engine = sslContext.createSSLEngine(host, port)
    engine.setUseClientMode(true)
    // 禁用所有验证
    engine.setEnabledProtocols(Array("TLSv1.2"))
    engine.setSSLParameters {
      val params = engine.getSSLParameters
      params.setEndpointIdentificationAlgorithm(null)  // 禁用主机名验证
      params
    }
    engine
  }

  val apihost = "https://172.17.0.1"
  val authKey = "23bc46b1-71f6-4ed5-8c54-816aa4f8c502:123zO3xZCLrMN6v2BKK1dXYFpXlPkccOFqm12CdAsMgRU4VrNZ9lyGVCGuMDGIwP"
  val encodedAuth = "Basic " + Base64.getEncoder.encodeToString(authKey.getBytes)
  val url = buildUrl("remote_data_to_run_sql")

  def sendPrewarmRequest(tablesList: List[String])(implicit logging: Logging): Unit = {
    logging.info(this, s"尝试预热容器, tables: $tablesList")
    val tables = tablesList.mkString(", ")
    val params = Map(
      "data_file" -> "tpch_1g",
      "db_file" -> "prewarm",
      "sql_query" -> s"SELECT 1 FROM ${tables} WHERE 1=0;"
    )

    val jsonBody = s"""{
      "data_file": "${params("data_file")}",
      "db_file": "${params("db_file")}",
      "sql_query": "${params("sql_query")}",
      "predict_time": 0.0
    }"""

    val request = HttpRequest(
      method = HttpMethods.POST,
      uri = url,
      headers = List(RawHeader("Authorization", encodedAuth)),
      entity = HttpEntity(ContentTypes.`application/json`, jsonBody)
    )

    logging.info(this, s"request: $request")

    val responseFuture: Future[HttpResponse] = Http().singleRequest(request, connectionContext = httpsConnectionContext)

    responseFuture.onComplete {
      case scala.util.Success(response) =>
        if (response.status.isSuccess()) {
          val bodyFuture: Future[String] = Unmarshal(response.entity).to[String]
          bodyFuture.foreach { body =>
            logging.info(this, s"Request accepted: $body")
          }
        } else {
          logging.error(this, s"Request failed with status: ${response.status}")
        }
      case scala.util.Failure(exception) =>
        logging.error(this, s"Request failed: ${exception.getMessage}")
    }
  }

  private def buildUrl(func: String): String = {
    s"$apihost/api/v1/namespaces/_/actions/$func?blocking=false&result=false"
  }

  def terminate(): Unit = {
    system.terminate()
  }
}