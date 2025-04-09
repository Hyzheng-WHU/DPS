/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.openwhisk.core.scheduler.container
import akka.actor.{Actor, ActorRef, ActorRefFactory, ActorSystem, Props}
import akka.event.Logging.InfoLevel
import org.apache.openwhisk.common.InvokerState.{Healthy, Offline, Unhealthy}
import org.apache.openwhisk.common._
import org.apache.openwhisk.core.connector.ContainerCreationError.{
  containerCreationErrorToString,
  NoAvailableInvokersError,
  NoAvailableResourceInvokersError
}
import org.apache.openwhisk.core.connector._
import org.apache.openwhisk.core.entity.size._
import org.apache.openwhisk.core.entity._
import org.apache.openwhisk.core.etcd.EtcdClient
import org.apache.openwhisk.core.etcd.EtcdKV.ContainerKeys.containerPrefix
import org.apache.openwhisk.core.etcd.EtcdKV.{ContainerKeys, InvokerKeys}
import org.apache.openwhisk.core.etcd.EtcdType._
import org.apache.openwhisk.core.scheduler.Scheduler
import org.apache.openwhisk.core.scheduler.container.ContainerManager.{sendState, updateInvokerMemory}
import org.apache.openwhisk.core.scheduler.message._
import org.apache.openwhisk.core.scheduler.queue.{MemoryQueueKey, QueuePool}
import org.apache.openwhisk.core.service._
import org.apache.openwhisk.core.{ConfigKeys, WarmUp, WhiskConfig}
import pureconfig.loadConfigOrThrow
import spray.json.DefaultJsonProtocol._
import pureconfig.generic.auto._

import java.nio.charset.StandardCharsets
import java.util.concurrent.ThreadLocalRandom
import scala.annotation.tailrec
import scala.collection.JavaConverters._
import scala.collection.concurrent.TrieMap
import scala.collection.immutable
import scala.concurrent.{ExecutionContext, ExecutionContextExecutor, Future}
import scala.util.{Failure, Success}

import java.nio.file.{Files, Paths}
import scala.util.matching.Regex
import scala.util.Try
import java.io.{BufferedReader, FileReader, File, PrintWriter}

import spray.json._
import DefaultJsonProtocol._
import scala.io.Source

import org.apache.calcite.sql.parser.SqlParser
import org.apache.calcite.sql.util.SqlBasicVisitor
import org.apache.calcite.sql.SqlIdentifier
import org.apache.calcite.sql.SqlCall
import org.apache.calcite.sql.SqlKind
import org.apache.calcite.sql.SqlSelect

// (implicit logging: Logging)
object TableDownloadPredictor {

  // 定义表大小的Map，单位为Byte
  val Tpch1gTableSize: Map[String, Long] = Map(
    "customer" -> 24196144L,
    "lineitem" -> 753862072L,
    "nation"   -> 2199L,
    "orders"   -> 170452161L,
    "partsupp" -> 118184616L,
    "part"     -> 23935125L,
    "region"   -> 384L,
    "supplier" -> 1399184L
  )

  /**
   * 预测下载时间的函数
   * @param neededTables 需要的表列表
   * @param existingTables 已有的表列表
   * @param bandwidthMbps 网络带宽，单位为Mbps，默认为100
   * @return 缺失表的下载总时间，单位为秒
   */
  def predictDownloadTime(
      neededTables: List[String],
      existingTables: List[String],
      bandwidthMbps: Int = 100
  ): Double = {
    // 找出缺失的表
    val missingTables = neededTables.diff(existingTables)
    
    // 将带宽从Mbps转换为bit/s
    val bandwidthBps = bandwidthMbps * 1000000L

    // 计算缺失表的总大小（字节），并转换为bit
    val totalSizeBits = missingTables.flatMap(Tpch1gTableSize.get).sum * 8

    // 计算并返回下载时间（秒）
    totalSizeBits.toDouble / bandwidthBps
  }
}



object SqlTableExtractor {

  // 预处理函数，用来将 SQLite 风格 SQL 转换为标准 SQL
  def preprocessSQL(sql: String): String = {
    var processedSQL = sql

    // 1. 处理 DATE('YYYY-MM-DD') -> DATE 'YYYY-MM-DD'
    val datePattern = """DATE\('(\d{4}-\d{2}-\d{2})'\)""".r
    processedSQL = datePattern.replaceAllIn(processedSQL, m => s"DATE '${m.group(1)}'")

    // 注释掉了一些暂时没有在测试语句中出现的内容，如果遇到日志中报错解析 “SQL 失败: Encountered ...”，可以取消注释
    // // 2. 处理 DATETIME('YYYY-MM-DD HH:MM:SS') -> TIMESTAMP 'YYYY-MM-DD HH:MM:SS'
    // val datetimePattern = """DATETIME\('(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})'\)""".r
    // processedSQL = datetimePattern.replaceAllIn(processedSQL, m => s"TIMESTAMP '${m.group(1)}'")

    // // 3. 处理字符串拼接 a || b -> CONCAT(a, b)
    // val concatPattern = """(\w+)\s*\|\|\s*(\w+)""".r
    // processedSQL = concatPattern.replaceAllIn(processedSQL, m => s"CONCAT(${m.group(1)}, ${m.group(2)})")

    // // 4. 处理 IFNULL(a, b) -> COALESCE(a, b)
    // val ifNullPattern = """IFNULL\(([^,]+),\s*([^)]+)\)""".r
    // processedSQL = ifNullPattern.replaceAllIn(processedSQL, m => s"COALESCE(${m.group(1)}, ${m.group(2)})")

    // // 5. 处理 INSTR(a, b) -> POSITION(b IN a)
    // val instrPattern = """INSTR\(([^,]+),\s*([^)]+)\)""".r
    // processedSQL = instrPattern.replaceAllIn(processedSQL, m => s"POSITION(${m.group(2)} IN ${m.group(1)})")

    // 6. 移除 SQL 语句末尾的分号（如果有）
    processedSQL = processedSQL.replaceAll(";$", "")

    // 返回预处理后的 SQL
    processedSQL
  }

  // 解析并提取 SQL 语句中的表名
  def extractTables(sql: String)(implicit logging: Logging): List[String] = {
    // 调用 preprocessSQL() 来处理 SQL 语句中的 DATE() 函数
    val processedSQL = preprocessSQL(sql)
    logging.info(this, s"processedSQL: $processedSQL")

    // 初始化 SQL Parser 配置
    val config = SqlParser.Config.DEFAULT
    val parser = SqlParser.create(processedSQL, config)

    try {
      // 解析预处理后的 SQL 语句
      val sqlNode = parser.parseQuery()

      // 使用可变的 ListBuffer 来收集表名
      val tableNames = scala.collection.mutable.ListBuffer[String]()

      // 引入上下文标识，标记当前是否在 FROM 或 JOIN 子句中
      var inFromOrJoinContext = false

      // 定义一个访问者来提取 SQL 中的表名
      val visitor = new SqlBasicVisitor[Unit] {
        override def visit(call: SqlCall): Unit = {
          call.getKind match {
            case SqlKind.SELECT =>
              // 如果是 SELECT 子句，处理它的 FROM 部分
              val select = call.asInstanceOf[SqlSelect]
              if (select.getFrom != null) {
                // logging.info(this, s"FROM clause content: ${select.getFrom.toString}")
                inFromOrJoinContext = true
                select.getFrom.accept(this) // 只处理 FROM 子句
                inFromOrJoinContext = false
              }
            case SqlKind.JOIN =>
              // 如果是 JOIN 子句，递归处理
              inFromOrJoinContext = true
              call.getOperandList.forEach(operand => if (operand != null) operand.accept(this))
              inFromOrJoinContext = false
            case SqlKind.IDENTIFIER =>
              // 处理逗号分隔的多表情况
              call.getOperandList.forEach(operand => if (operand != null) operand.accept(this))
            case _ =>
              super.visit(call) // 继续遍历其他子节点
          }
        }


        override def visit(identifier: SqlIdentifier): Unit = {
          // 只在 FROM 或 JOIN 子句中时，认为该标识符是表名
          if (identifier.names.size == 1 && inFromOrJoinContext) {
            tableNames += identifier.toString
          }
        }
      }

      // 递归访问 SQL AST 节点
      sqlNode.accept(visitor)

      // 转换为不可变的 List 并返回
      tableNames.toList.distinct
    } catch {
      case e: Exception =>
        logging.error(this, s"解析 SQL 失败: ${e.getMessage}")
        List.empty
    }
  }
}

// 定义 ContainerDbInfo 类
case class ContainerDbInfo(activationId: String, creationId: String, containerId: String, dbName: String, tables: List[String])

// 定义 JsonProtocol
object ContainerDbInfoJsonProtocol extends DefaultJsonProtocol {
  // 定义 ContainerDbInfo 的 JsonFormat
  implicit val containerDbInfoFormat: RootJsonFormat[ContainerDbInfo] = jsonFormat5(ContainerDbInfo)
}

object ContainerDbInfoManager {
  val dbInfoFilePath = "/db/container_db_info.json"
  import ContainerDbInfoJsonProtocol._ // 导入 JsonFormat

  // 读取信息文件
  def readDbInfo()(implicit logging: Logging): Map[String, ContainerDbInfo] = {
    logging.info(this, s"reading $dbInfoFilePath")
    // 检查文件是否存在
    if (!Files.exists(Paths.get(dbInfoFilePath))) {
      logging.error(this, s"$dbInfoFilePath not exist!")
      return Map.empty[String, ContainerDbInfo]
    }

    // 读取文件内容
    val source = Source.fromFile(dbInfoFilePath)
    val jsonStr = try source.mkString finally source.close()

    // 检查文件是否为空
    if (jsonStr.trim.isEmpty) {
      logging.error(this, s"$dbInfoFilePath is empty!")
      return Map.empty[String, ContainerDbInfo]
    }

    // 解析 JSON 并捕获异常
    try {
      jsonStr.parseJson.convertTo[Map[String, ContainerDbInfo]]
    } catch {
      case e: Exception =>
        logging.error(this, s"Failed to parse JSON from $dbInfoFilePath: ${e.getMessage}")
        Map.empty[String, ContainerDbInfo]
    }
  }
  
  // 写入信息文件
  def writeDbInfo(dbInfo: Map[String, ContainerDbInfo])(implicit logging: Logging): Unit = {
    logging.info(this, s"writing $dbInfoFilePath")
    val jsonStr = dbInfo.toJson.prettyPrint
    val writer = new PrintWriter(new File(dbInfoFilePath))
    try writer.write(jsonStr) finally writer.close()
  }

  // 引入锁机制来读写
  // 晚点再添加
  // def writeDbInfoWithLock(dbInfo: Map[String, ContainerDbInfo])(implicit logging: Logging): Unit = {
  //   val lockFile = new RandomAccessFile(dbInfoFilePath + ".lock", "rw").getChannel.lock()
  //   try {
  //     writeDbInfo(dbInfo)
  //   } finally {
  //     lockFile.release()
  //   }
  // }


  // 增加信息文件
  def createDbInfo(creationId: String, dbFile: String, tables: List[String])(implicit logging: Logging): Unit = {
    logging.info(this, s"create DbInfo of creationId:$creationId in $dbInfoFilePath")
    val dbInfo = readDbInfo()
    val activationId=" wait "
    val containerId=" wait "
    val updatedInfo = dbInfo + (creationId -> ContainerDbInfo(activationId, creationId, containerId, dbFile, tables))
    writeDbInfo(updatedInfo)
  }

  // 删除容器信息
  def removeContainerInfo(containerId: String)(implicit logging: Logging): Unit = {
    logging.info(this, s"removing $dbInfoFilePath")
    val dbInfo = readDbInfo()
    val updatedInfo = dbInfo - containerId
    writeDbInfo(updatedInfo)
  }
}

object WarmStartInfoManager {

  // 定义文件路径为对象的成员变量
  val warmStartInfoFilePath: String = "/db/warm_start_info.json"

  // 读取 warmStartInfo 文件内容的函数
  def readWarmStartInfo(): JsArray = {
    val source = Source.fromFile(warmStartInfoFilePath)
    try {
      val fileContent = source.getLines().mkString("\n")

      // 解析 JSON 内容
      val json = fileContent.parseJson

      // 确保解析结果是 JsArray
      json match {
        case arr: JsArray => arr
        case _ => throw new RuntimeException("Invalid JSON format: expected a JsArray")
      }
    } finally {
      source.close() // 确保文件资源关闭
    }
  }

  // 写入 warmStartInfo 到文件的函数
  def writeWarmStartInfo(jsonArray: JsArray): Unit = {
    val writer = new PrintWriter(warmStartInfoFilePath)
    try {
      // 将 JSON 数组转换为字符串并写入文件
      writer.write(jsonArray.prettyPrint)
    } finally {
      writer.close() // 确保写入完成后关闭资源
    }
  }

  // 针对热启动情况，更新warmStartInfo 文件
  def updateContainerIdByCreationId(creationId: String, newContainerId: String)(implicit logging: Logging): Unit = {
    // 1. 读取现有的 warmStartInfo 文件
    val warmStartInfoArray = readWarmStartInfo()

    // 日志记录
    logging.info(this, s"warmStartInfoArray: $warmStartInfoArray")
    logging.info(this, s"creationId: $creationId")
    logging.info(this, s"newContainerId: $newContainerId")

    // 2. 读取 dbInfo 文件
    val dbInfo = ContainerDbInfoManager.readDbInfo()

    // 3. 查找并更新匹配的 creationId
    val updatedArray = warmStartInfoArray.elements.map {
      case obj: JsObject if obj.fields.get("creationId").contains(JsString(creationId)) =>
        // 从 dbInfo 中查找匹配的 containerId
        dbInfo.get(newContainerId) match {
          case Some(containerDbInfo) =>
            // 获取 dbName
            val dbName = containerDbInfo.dbName

            // 更新 containerId 和 dbName
            obj.copy(fields = obj.fields + ("containerId" -> JsString(newContainerId), "dbName" -> JsString(dbName)))

          case None =>
            // 如果 dbInfo 中没有匹配的 containerId，只更新 containerId
            obj.copy(fields = obj.fields + ("containerId" -> JsString(newContainerId)))
        }

      case other => other // 保留其他不变的对象
    }

    // 4. 将更新后的数组写回文件
    writeWarmStartInfo(JsArray(updatedArray.toVector))

    logging.info(this, s"update containerId and dbName of creationId: $creationId ")
  }

  // 根据activationId更新containerId
  def updateContainerIdByActivationId(activationId: String, newContainerId: String)(implicit logging: Logging): Unit = {
    val warmStartInfoArray = readWarmStartInfo() // 读取的是JsArray

    val updatedArray = warmStartInfoArray.elements.map { element =>
      element.asJsObject.getFields("activationId", "containerId") match {
        case Seq(JsString(actId), _) if actId == activationId =>
          element.asJsObject.copy(fields = element.asJsObject.fields + ("containerId" -> JsString(newContainerId)))
        case _ => element
      }
    }

    writeWarmStartInfo(JsArray(updatedArray.toVector)) // 写入更新后的数组
    logging.info(this, s"update containerId of ActivationId: $ActivationId ")
  }

    // 根据CreationId更新containerId
  // def updateContainerIdByCreationId(creationId: String, newContainerId: String)(implicit logging: Logging): Unit = {
  //   val warmStartInfoArray = readWarmStartInfo() // 读取的是JsArray

  //   val updatedArray = warmStartInfoArray.elements.map { element =>
  //     element.asJsObject.getFields("creationId", "containerId") match {
  //       case Seq(JsString(creId), _) if creId == creationId =>
  //         element.asJsObject.copy(fields = element.asJsObject.fields + ("containerId" -> JsString(newContainerId)))
  //       case _ => element
  //     }
  //   }

  //   writeWarmStartInfo(JsArray(updatedArray.toVector)) // 写入更新后的数组
  //   logging.info(this, s"update containerId of creationId: $creationId ")
  // }
    

  // 创建新的配对并添加到 warmStartInfo 文件中的函数
  def createWarmStartInfo(creationId: String, activationId: String, containerId: String, dbName: String, startMode: String): Unit = {
    // 1. 创建新的配对对象
    val newWarmStartInfo = JsObject(
      "creationId" -> JsString(creationId),
      "activationId" -> JsString(activationId),
      "containerId" -> JsString(containerId),
      "dbName" -> JsString(dbName),
      "startMode" -> JsString(startMode),
    )

    // 2. 读取现有的 warmStartInfo 数组
    val warmStartInfoArray = readWarmStartInfo()

    // 3. 将新的配对对象添加到数组中
    val updatedArray = warmStartInfoArray.elements :+ newWarmStartInfo

    // 4. 写回文件
    writeWarmStartInfo(JsArray(updatedArray.toVector))
  }
}


case class ScheduledPair(msg: ContainerCreationMessage,
                         invokerId: Option[InvokerInstanceId],
                         err: Option[ContainerCreationError] = None)

case class BlackboxFractionConfig(managedFraction: Double, blackboxFraction: Double)

class ContainerManager(jobManagerFactory: ActorRefFactory => ActorRef,
                       provider: MessagingProvider,
                       schedulerInstanceId: SchedulerInstanceId,
                       etcdClient: EtcdClient,
                       config: WhiskConfig,
                       watcherService: ActorRef)(implicit actorSystem: ActorSystem, logging: Logging)
    extends Actor {
  private implicit val ec: ExecutionContextExecutor = context.dispatcher

  private val warmedContainerStates = TrieMap[String, Boolean]() // Key: 容器id containerId, Value: true if in use, false if idle

  private val creationJobManager = jobManagerFactory(context)

  private val messagingProducer = provider.getProducer(config)

  private var warmedContainers = Set.empty[String]

  private val warmedInvokers = TrieMap[Int, String]()

  private val inProgressWarmedContainers = TrieMap.empty[String, String]

  private val warmKey = ContainerKeys.warmedPrefix
  private val invokerKey = InvokerKeys.prefix
  private val watcherName = s"container-manager"

  watcherService ! WatchEndpoint(warmKey, "", isPrefix = true, watcherName, Set(PutEvent, DeleteEvent))
  watcherService ! WatchEndpoint(invokerKey, "", isPrefix = true, watcherName, Set(PutEvent, DeleteEvent))

  override def receive: Receive = {
    case ContainerCreation(msgs, memory, invocationNamespace) =>
      logging.info(this, s" receive message ContainerCreation, msgs:$msgs")
      createContainer(msgs, memory, invocationNamespace)

    case ContainerDeletion(invocationNamespace, fqn, revision, whiskActionMetaData) =>
      logging.info(this, s" receive message ContainerDeletion : fqn:$fqn")
      getInvokersWithOldContainer(invocationNamespace, fqn, revision)
        .map { invokers =>
          val msg = ContainerDeletionMessage(
            TransactionId.containerDeletion,
            invocationNamespace,
            fqn,
            revision,
            whiskActionMetaData)
          invokers.foreach(sendDeletionContainerToInvoker(messagingProducer, _, msg))

          // 在删除容器的同时删除对应的 .db 文件并更新
          // deleteContainerDbFile(fqn)
        }

    case rescheduling: ReschedulingCreationJob =>
      logging.info(this, s" receive message rescheduling")
      val msg = rescheduling.toCreationMessage(schedulerInstanceId, rescheduling.retry + 1)
      createContainer(
        List(msg),
        rescheduling.actionMetaData.limits.memory.megabytes.MB,
        rescheduling.invocationNamespace)

    case watchMessage @ WatchEndpointInserted(watchKey, key, _, true) =>
      logging.info(this, s"Received message: watchMessage: $watchMessage")
      watchKey match {
        case `warmKey` => 
          logging.info(this, s" receive message WatchEndpointInserted : case `warmKey`, key:$key")
          warmedContainers += key
          logging.info(this, s"warmedContainers after += :$warmedContainers")

          val containerId = key.split("/").last // 获取路径中最后的部分，即 containerId
          warmedContainerStates.update(containerId, false)
        case `invokerKey` =>
          logging.info(this, s" receive message WatchEndpointInserted : case `invokerKey`, key:$key")
          val invoker = InvokerKeys.getInstanceId(key)
          warmedInvokers.getOrElseUpdate(invoker.instance, {
            warmUpInvoker(invoker)
            invoker.toString
          })
      }

    case WatchEndpointRemoved(watchKey, key, _, true) =>
      watchKey match {
        case `warmKey` => 
          logging.info(this, s" receive message WatchEndpointRemoved : case `warmKey`, key:$key")
          warmedContainers -= key
          logging.info(this, s"warmedContainers after -= :$warmedContainers")

          val containerId = key.split("/").last // 获取路径中最后的部分，即 containerId
          warmedContainerStates.get(containerId) match {
          case Some(true) =>
            // 如果状态为 true，说明容器正在使用，因此只从 warmedContainers 中移除，但不删除 db 文件
            logging.info(this, s"Container $key is in use. Removed from warmedContainers but kept DB.")
            
          case Some(false) =>
            // 如果状态为 false，说明容器超时未被重用，因此需要删除 db 文件
            logging.info(this, s"Container $key is idle. Removed and cleaned up DB.")
            deleteContainerDbFile(containerId)

          case None =>
            logging.warn(this, s"Received WatchEndpointRemoved for unknown container $key")
        }
        case `invokerKey` =>
          logging.info(this, s" receive message WatchEndpointRemoved : case `invokerKey`")
          val invoker = InvokerKeys.getInstanceId(key)
          warmedInvokers.remove(invoker.instance)
      }

    case FailedCreationJob(cid, _, _, _, _, _) =>
      inProgressWarmedContainers.remove(cid.asString)

    case SuccessfulCreationJob(cid, _, _, _) =>
      inProgressWarmedContainers.remove(cid.asString)

    case GracefulShutdown =>
      watcherService ! UnwatchEndpoint(warmKey, isPrefix = true, watcherName)
      watcherService ! UnwatchEndpoint(invokerKey, isPrefix = true, watcherName)
      creationJobManager ! GracefulShutdown

    case _ =>
  }

  // 删除 .db 文件并更新 container_db_info.json
  protected[container] def deleteContainerDbFile(containerId: String)(implicit logging: Logging): Unit = {
    logging.info(this, s"try to delete container$containerId, and its .db file")
    // 读取 container_db_info.json 文件
    val dbInfo = ContainerDbInfoManager.readDbInfo()

    // 检查 dbInfo 中是否有对应的容器信息
    dbInfo.get(containerId) match {
      case Some(containerDbInfo) =>
        // 删除 .db 文件
        val dbFilePath = Paths.get(s"/db/${containerDbInfo.dbName}")
        try {
          if (Files.exists(dbFilePath)) {
            Files.delete(dbFilePath)
            logging.info(this, s"Deleted database file: ${s"/db/${containerDbInfo.dbName}"}")
          } else {
            logging.error(this, s"Failed to delete, database file not found: ${s"/db/${containerDbInfo.dbName}"}")
          }
        } catch {
          case e: Exception =>
            logging.error(this, s"Failed to delete database file: ${containerDbInfo.dbName}. Error: ${e.getMessage}")
        }

        // 从 dbInfo 中移除容器信息，并更新文件
        ContainerDbInfoManager.removeContainerInfo(containerId)
        logging.info(this, s"Deleted container information from dbInfo for container: $containerId")

      case None =>
        logging.error(this, s"Failed to delete, No database information found for container: $containerId")
    }
}

  // protected[container] def extractTableNames(sql: String)(implicit logging: Logging): List[String] = {
  //   // 改进后的正则表达式，支持跨行且能够正确处理逗号分隔的多个表
  //   val tableNamePattern: Regex = """(?is)(?:FROM|JOIN)\s+([a-zA-Z_][a-zA-Z0-9_\.]*(?:\s*,\s*[a-zA-Z_][a-zA-Z0-9_\.]*)*)""".r

  //   // 使用正则表达式查找所有匹配的表名组
  //   val matches = tableNamePattern.findAllIn(sql).toList

  //   // 初始化一个空的表名列表
  //   var tables: List[String] = List()

  //   // 遍历每个匹配结果，处理逗号分隔的表名
  //   for (matchStr <- matches) {
  //     // 从每个匹配的字符串中提取逗号分隔的表名
  //     val splitTables = matchStr.split("\\s*,\\s*").toList
  //     // 将表名添加到列表中
  //     tables = tables ++ splitTables.map(_.replaceFirst("(?i)(FROM|JOIN)\\s+", "")) // 去除FROM或JOIN前缀
  //   }

  //   // 返回表名列表
  //   tables.distinct
  // }
  
  // 读取文件内容的函数（适用于 Scala 2.12 及以下版本）
  protected[container] def readSqlFromFile(filePath: String): Option[String] = {
    
    logging.info(this, s"get sqlFile: $filePath")
    val path = Paths.get(filePath)
    if (Files.exists(path)) {
      val reader = new BufferedReader(new FileReader(filePath))
      try {
        // 读取文件内容并返回为字符串
        Some(Iterator.continually(reader.readLine()).takeWhile(_ != null).mkString("\n"))
      } catch {
        case e: Exception => 
          e.printStackTrace()
          None
      } finally {
        // 确保文件关闭
        reader.close()
      }
    } else {
      None
    }
  }

  private def createContainer(msgs: List[ContainerCreationMessage], memory: ByteSize, invocationNamespace: String)(
    implicit logging: Logging): Unit = {
    logging.info(this, s"received ${msgs.size} creation message [${msgs.head.invocationNamespace}:${msgs.head.action}]")
    // 获取当前可用的 Invokers
    ContainerManager
      .getAvailableInvokers(etcdClient, memory, invocationNamespace)
      .recover({
        // 处理错误情况
        case t: Throwable =>
          logging.error(this, s"Unable to get available invokers: ${t.getMessage}.")
          List.empty[InvokerHealth]
      })
      .foreach { invokers =>
        if (invokers.isEmpty) {
          // 处理无可用invoker的情况
          logging.error(this, "there is no available invoker to schedule.")
          msgs.foreach(ContainerManager.sendState(_, NoAvailableInvokersError, NoAvailableInvokersError))
        } else {
          val (coldCreations, warmedCreations) =
            ContainerManager.filterWarmedCreations(warmedContainers, warmedContainerStates, inProgressWarmedContainers, invokers, msgs)
          logging.info(this, s"coldCreations: $coldCreations")
          logging.info(this, s"warmedCreations: $warmedCreations")
          // 处理冷启动创建
          coldCreations.foreach { case (msg, _, _) =>
            logging.info(this, s"cold start msg: $msg")
            val dbFile = s"/db/${msg.creationId.asString}.db"
            val dbPath = Paths.get(dbFile)
            // 提取 sql_query 或 sql_file 中的 SQL
            val sql_query: Option[String] = msg.args.flatMap { args =>
              // 检查是否有 `sql_file` 字段
              args.fields.get("sql_file").flatMap { sqlFile =>
                // 如果有 `sql_file` 字段，读取文件内容
                readSqlFromFile(s"/sql/${sqlFile.convertTo[String].replaceAll("\"", "")}")
              }.orElse {
                // 如果没有 `sql_file` 字段，检查是否有 `sql_query`
                Try(args.fields("sql_query").convertTo[String]).toOption
              }
            }
            logging.info(this, s"get sql_query: $sql_query")
            val tables: List[String] = sql_query match {
              case Some(sql_query) =>
                // 提取 SQL 查询中的表名
                ContainerManager.extractTableNames(sql_query)
                // SqlTableExtractor.extractTables(sql_query)
              case None =>
                // 如果没有找到 `sql_query`，则返回空表名列表
                List.empty
            }
            logging.info(this, s"tables: $tables")

            // 如果数据库文件不存在，则创建一个新文件
            if (!Files.exists(dbPath)) {
              logging.info(this, s"Creating new database file: $dbPath")
              Files.createFile(dbPath)
              // 记录容器与数据库信息
              ContainerDbInfoManager.createDbInfo(msg.creationId.asString, s"${msg.creationId.asString}.db", tables)
              WarmStartInfoManager.createWarmStartInfo(msg.creationId.asString, "wait", "wait", s"${msg.creationId.asString}.db", "cold")
            }
            else {
              logging.error(this, s"dbFile already exists!")
            }
          }
          // handle warmed creation
          val chosenInvokers: immutable.Seq[Option[(Int, ContainerCreationMessage)]] = warmedCreations.map {
            warmedCreation =>
              logging.info(this, s"cold start msg: $warmedCreation")
              // update the in-progress map for warmed containers.
              // even if it is done in the filterWarmedCreations method, it is still necessary to apply the change to the original map.
              warmedCreation._3.foreach(inProgressWarmedContainers.update(warmedCreation._1.creationId.asString, _))

              // send creation message to the target invoker.
              warmedCreation._2 map { chosenInvoker =>
                val msg = warmedCreation._1
                creationJobManager ! RegisterCreationJob(msg)
                sendCreationContainerToInvoker(messagingProducer, chosenInvoker, msg)
                (chosenInvoker, msg)
              }
          }

          // update the resource usage of invokers to apply changes from warmed creations.
          val updatedInvokers = chosenInvokers.foldLeft(invokers) { (invokers, chosenInvoker) =>
            chosenInvoker match {
              case Some((chosenInvoker, msg)) =>
                updateInvokerMemory(chosenInvoker, msg.whiskActionMetaData.limits.memory.megabytes, invokers)
              case err =>
                // this is not supposed to happen.
                logging.error(this, s"warmed creation is scheduled but no invoker is chosen: $err")
                invokers
            }
          }

          // handle cold creations
          if (coldCreations.nonEmpty) {
            ContainerManager
              .schedule(updatedInvokers, coldCreations.map(_._1), memory)
              .map { pair =>
                pair.invokerId match {
                  // an invoker is assigned for the msg
                  case Some(instanceId) =>
                    logging.info(this, s"pair.msg: ${pair.msg}")
                    creationJobManager ! RegisterCreationJob(pair.msg)
                    sendCreationContainerToInvoker(messagingProducer, instanceId.instance, pair.msg)

                  // if a chosen invoker does not exist, it means it failed to find a matching invoker for the msg.
                  case _ =>
                    pair.err.foreach(error => sendState(pair.msg, error, error))
                }
              }
          }
        }
      }
  }

  private def getInvokersWithOldContainer(invocationNamespace: String,
                                          fqn: FullyQualifiedEntityName,
                                          currentRevision: DocRevision): Future[List[Int]] = {
    val namespacePrefix = containerPrefix(ContainerKeys.namespacePrefix, invocationNamespace, fqn)
    val warmedPrefix = containerPrefix(ContainerKeys.warmedPrefix, invocationNamespace, fqn)

    for {
      existing <- etcdClient
        .getPrefix(namespacePrefix)
        .map { res =>
          res.getKvsList.asScala.map { kv =>
            parseExistingContainerKey(namespacePrefix, kv.getKey)
          }
        }
      warmed <- etcdClient
        .getPrefix(warmedPrefix)
        .map { res =>
          res.getKvsList.asScala.map { kv =>
            parseWarmedContainerKey(warmedPrefix, kv.getKey)
          }
        }
    } yield {
      (existing ++ warmed)
        .dropWhile(k => k.revision > currentRevision) // remain latest revision
        .groupBy(k => k.invokerId) // remove duplicated value
        .map(_._2.head.invokerId)
        .toList
    }
  }

  /**
   * existingKey format: {tag}/namespace/{invocationNamespace}/{namespace}/({pkg}/)/{name}/{revision}/invoker{id}/container/{containerId}
   */
  private def parseExistingContainerKey(prefix: String, existingKey: String): ContainerKeyMeta = {
    val keys = existingKey.replace(prefix, "").split("/")
    val revision = DocRevision(keys(0))
    val invokerId = keys(1).replace("invoker", "").toInt
    val containerId = keys(3)
    ContainerKeyMeta(revision, invokerId, containerId)
  }

  /**
   * warmedKey format: {tag}/warmed/{invocationNamespace}/{namespace}/({pkg}/)/{name}/{revision}/invoker/{id}/container/{containerId}
   */
  private def parseWarmedContainerKey(prefix: String, warmedKey: String): ContainerKeyMeta = {
    val keys = warmedKey.replace(prefix, "").split("/")
    val revision = DocRevision(keys(0))
    val invokerId = keys(2).toInt
    val containerId = keys(4)
    ContainerKeyMeta(revision, invokerId, containerId)
  }

  private def sendCreationContainerToInvoker(producer: MessageProducer,
                                             invoker: Int,
                                             msg: ContainerCreationMessage): Future[ResultMetadata] = {
    implicit val transid: TransactionId = msg.transid

    val topic = s"${Scheduler.topicPrefix}invoker$invoker"
    val start = transid.started(this, LoggingMarkers.SCHEDULER_KAFKA, s"posting to $topic")

    producer.send(topic, msg).andThen {
      case Success(status) =>
        transid.finished(
          this,
          start,
          s"posted creationId: ${msg.creationId} for ${msg.invocationNamespace}/${msg.action} to ${status.topic}[${status.partition}][${status.offset}]",
          logLevel = InfoLevel)
      case Failure(_) =>
        logging.error(this, s"Failed to create container for ${msg.action}, error: error on posting to topic $topic")
        transid.failed(this, start, s"error on posting to topic $topic")
    }
  }

  private def sendDeletionContainerToInvoker(producer: MessageProducer,
                                             invoker: Int,
                                             msg: ContainerDeletionMessage): Future[ResultMetadata] = {
    implicit val transid: TransactionId = msg.transid

    val topic = s"${Scheduler.topicPrefix}invoker$invoker"
    val start = transid.started(this, LoggingMarkers.SCHEDULER_KAFKA, s"posting to $topic")

    producer.send(topic, msg).andThen {
      case Success(status) =>
        transid.finished(
          this,
          start,
          s"posted deletion for ${msg.invocationNamespace}/${msg.action} to ${status.topic}[${status.partition}][${status.offset}]",
          logLevel = InfoLevel)
      case Failure(_) =>
        logging.error(this, s"Failed to delete container for ${msg.action}, error: error on posting to topic $topic")
        transid.failed(this, start, s"error on posting to topic $topic")
    }
  }

  private def warmUpInvoker(invoker: InvokerInstanceId): Unit = {
    logging.info(this, s"Warm up invoker $invoker")
    WarmUp.warmUpContainerCreationMessage(schedulerInstanceId).foreach {
      sendCreationContainerToInvoker(messagingProducer, invoker.instance, _)
    }
  }

  // warm up all invokers
  private def warmUp() = {
    // warm up exist invokers
    ContainerManager.getAvailableInvokers(etcdClient, MemoryLimit.MIN_MEMORY).map { invokers =>
      invokers.foreach { invoker =>
        warmedInvokers.getOrElseUpdate(invoker.id.instance, {
          warmUpInvoker(invoker.id)
          invoker.id.toString
        })
      }
    }

  }

  warmUp()
}

object ContainerManager {
  val fractionConfig: BlackboxFractionConfig =
    loadConfigOrThrow[BlackboxFractionConfig](ConfigKeys.fraction)

  private val managedFraction: Double = Math.max(0.0, Math.min(1.0, fractionConfig.managedFraction))
  private val blackboxFraction: Double = Math.max(1.0 - managedFraction, Math.min(1.0, fractionConfig.blackboxFraction))

  // 需要分别在class与object下都定义这个函数
  def extractTableNames(sql: String): List[String] = {
    // 改进后的正则表达式，支持跨行且能够正确处理逗号分隔的多个表
    val tableNamePattern: Regex = """(?is)(?:FROM|JOIN)\s+([a-zA-Z_][a-zA-Z0-9_\.]*(?:\s*,\s*[a-zA-Z_][a-zA-Z0-9_\.]*)*)""".r

    // 使用正则表达式查找所有匹配的表名组
    val matches = tableNamePattern.findAllIn(sql).toList

    // 初始化一个空的表名列表
    var tables: List[String] = List()

    // 遍历每个匹配结果，处理逗号分隔的表名
    for (matchStr <- matches) {
      // 从每个匹配的字符串中提取逗号分隔的表名
      val splitTables = matchStr.split("\\s*,\\s*").toList
      // 将表名添加到列表中
      tables = tables ++ splitTables.map(_.replaceFirst("(?i)(FROM|JOIN)\\s+", "")) // 去除FROM或JOIN前缀
    }

    // 返回表名列表
    tables.distinct
  }

  def props(jobManagerFactory: ActorRefFactory => ActorRef,
            provider: MessagingProvider,
            schedulerInstanceId: SchedulerInstanceId,
            etcdClient: EtcdClient,
            config: WhiskConfig,
            watcherService: ActorRef)(implicit actorSystem: ActorSystem, logging: Logging): Props =
    Props(new ContainerManager(jobManagerFactory, provider, schedulerInstanceId, etcdClient, config, watcherService))

  /**
   * The rng algorithm is responsible for the invoker distribution, and the better the distribution, the smaller the number of rescheduling.
   *
   */
  def rng(mod: Int): Int = ThreadLocalRandom.current().nextInt(mod)

  // Partition messages that can use warmed containers.
  // return: (list of messages that cannot use warmed containers, list of messages that can take advantage of warmed containers)
  protected[container] def filterWarmedCreations(warmedContainers: Set[String],
                                                 warmedContainerStates: TrieMap[String, Boolean],
                                                 inProgressWarmedContainers: TrieMap[String, String],
                                                 invokers: List[InvokerHealth],
                                                 msgs: List[ContainerCreationMessage])(
    implicit logging: Logging): (List[(ContainerCreationMessage, Option[Int], Option[String])],
                                List[(ContainerCreationMessage, Option[Int], Option[String])]) = {

    val dbInfo = ContainerDbInfoManager.readDbInfo()

    val warmedApplied = msgs.map { msg =>
      logging.info(this, s"filterWarmedCreations() msg: $msg")
      val warmedPrefix =
        containerPrefix(ContainerKeys.warmedPrefix, msg.invocationNamespace, msg.action, Some(msg.revision))
      
      // 存储每个符合条件的容器及其下载时间
      val containerDownloadTimes = TrieMap[String, Int]()

      val container = warmedContainers
        .filter(!inProgressWarmedContainers.values.toSeq.contains(_))
        .find { container =>
          if (container.startsWith(warmedPrefix)) {
            logging.info(this, s"Choose a warmed container $container")

            // 检查该容器中的数据库文件是否包含所有所需的表
            val containerId = container.split("/").last
            logging.info(this, s"containerId $containerId")         
            // 处理 msg.args，确保提取 JsObject
            val sqlQueryOpt = msg.args match {
              case Some(jsObj: JsObject) =>
                jsObj.fields.get("sql_query") match {
                  case Some(JsString(sql)) => Some(sql)  // 提取出 JsString 并转换为字符串
                  case _ => 
                    logging.error(this, s"sql_query not found or not a JsString in msg.args: $jsObj")
                    None  // 如果不是 JsString，则返回 None
                }
              case None => 
                logging.error(this, "msg.args is None")
                None  // 处理 msg.args 为 None 的情况
            }
            val tablesNeeded = sqlQueryOpt match {
              case Some(sqlQuery) => extractTableNames(sqlQuery)  // 你自己的函数调用
              case None => List.empty[String]  // 如果没有找到 sql_query，返回空列表
            }
            logging.info(this, s"tablesNeeded $tablesNeeded")
            val containerInfo = dbInfo.get(containerId)
            logging.info(this, s"containerInfo $containerInfo")

            val isSuitable = containerInfo.exists(info => tablesNeeded.forall(info.tables.contains))
            if (isSuitable) {
              logging.info(this, s"containerId $containerId is suitable")
              
              inProgressWarmedContainers.update(msg.creationId.asString, container)
              warmedContainerStates.update(containerId, true)
              WarmStartInfoManager.createWarmStartInfo(msg.creationId.asString, "wait", "wait", "wait", "warm")// 其实此时containerId也可以确定，但是后面也可以赋值，这里无论是wait还是containerId无所谓
              true
            } else {
              logging.info(this, s"containerId $containerId is unsuitable")
              false
            }
          } else
            false
        }

      val chosenInvoker = container
        .map(_.split("/").takeRight(3).apply(0))
        .filter(
          invoker =>
            invokers
              .filter(_.status.isUsable)
              .map(_.id.instance)
              .contains(invoker.toInt))

      if (chosenInvoker.nonEmpty && container.nonEmpty) {
        // 热启动
        val containerId = container.get.split("/").last        
        // 根据 containerId 从 dbInfo 中查找对应的 dbName
        val dbName = dbInfo.get(containerId).map(_.dbName)
        // 提取 dbName 中的最后部分作为新的 db_file 值
        val dbFile = dbName match {
          case Some(dbName) =>
            // 提取 dbName 中最后的部分（即最后一个 "/" 之后的内容）
            val dbFileName = dbName
            dbFileName // 返回最后的文件名部分
          case None =>
            logging.error(this, s"No dbName found for containerId: $containerId")
            ".db" // 如果没有找到 dbName，则使用一个默认值
        }

        WarmStartInfoManager.updateContainerIdByCreationId(msg.creationId.asString, containerId)

        // 创建一个新的 msg，并更新 args 字段中的 db_file 为 containerId.db
        // val updatedArgs = msg.args match {
        //   case Some(jsObj: JsObject) =>
        //     val updatedFields = jsObj.fields.updated("db_file", JsString(dbFile))
        //     Some(JsObject(updatedFields))
        //   case None =>
        //     Some(JsObject("db_file" -> JsString(dbFile)))
        // }
        // val updatedMsg = msg.copy(args = updatedArgs)    
        // logging.info(this, s"warm updatedMsg: $updatedMsg")
        // (updatedMsg, Some(chosenInvoker.get.toInt), Some(container.get))
        (msg, Some(chosenInvoker.get.toInt), Some(container.get))
      } else{
        // 冷启动
        // val dbFile = s"${msg.creationId.asString}.db"

        // val updatedArgs = msg.args match {
        //   case Some(jsObj: JsObject) =>
        //     val updatedFields = jsObj.fields.updated("db_file", JsString(dbFile))
        //     Some(JsObject(updatedFields))
        //   case None =>
        //     Some(JsObject("db_file" -> JsString(dbFile)))
        // }

        // val updatedMsg = msg.copy(args = updatedArgs)
        // logging.info(this, s"cold updatedMsg: $updatedMsg")
        // (updatedMsg, None, None)
        (msg, None, None)
    }
  }

    warmedApplied.partition { item =>
      item._2.isEmpty
    }
  }

  protected[container] def updateInvokerMemory(invokerId: Int,
                                               requiredMemory: Long,
                                               invokers: List[InvokerHealth]): List[InvokerHealth] = {
    // it must be compared to the instance unique id
    val index = invokers.indexOf(invokers.filter(p => p.id.instance == invokerId).head)
    val invoker = invokers(index)

    // if the invoker has less than minimum memory, drop it from the list.
    if (invoker.id.userMemory.toMB - requiredMemory < MemoryLimit.MIN_MEMORY.toMB) {
      // drop the nth element
      val split = invokers.splitAt(index)
      val _ :: t1 = split._2
      split._1 ::: t1
    } else {
      invokers.updated(
        index,
        invoker.copy(id = invoker.id.copy(userMemory = invoker.id.userMemory - requiredMemory.MB)))
    }
  }

  protected[container] def updateInvokerMemory(invokerId: Option[InvokerInstanceId],
                                               requiredMemory: Long,
                                               invokers: List[InvokerHealth]): List[InvokerHealth] = {
    invokerId match {
      case Some(instanceId) =>
        updateInvokerMemory(instanceId.instance, requiredMemory, invokers)

      case None =>
        // do nothing
        invokers
    }
  }

  /**
   * Assign an invoker to a message
   *
   * Assumption
   *  - The memory of each invoker is larger than minMemory.
   *  - Messages that are not assigned an invoker are discarded.
   *
   * @param invokers Available invoker pool
   * @param msgs Messages to which the invoker will be assigned
   * @param minMemory Minimum memory for all invokers
   * @return A pair of messages and assigned invokers
   */
  def schedule(invokers: List[InvokerHealth], msgs: List[ContainerCreationMessage], minMemory: ByteSize)(
    implicit logging: Logging): List[ScheduledPair] = {
    logging.info(this, s"usable total invoker size: ${invokers.size}")
    val noTaggedInvokers = invokers.filter(_.id.tags.isEmpty)
    val managed = Math.max(1, Math.ceil(noTaggedInvokers.size.toDouble * managedFraction).toInt)
    val blackboxes = Math.max(1, Math.floor(noTaggedInvokers.size.toDouble * blackboxFraction).toInt)
    val managedInvokers = noTaggedInvokers.take(managed)
    val blackboxInvokers = noTaggedInvokers.takeRight(blackboxes)
    logging.info(
      this,
      s"${msgs.size} creation messages for ${msgs.head.invocationNamespace}/${msgs.head.action}, managedFraction:$managedFraction, blackboxFraction:$blackboxFraction, managed invoker size:$managed, blackboxes invoker size:$blackboxes")
    val list = msgs
      .foldLeft((List.empty[ScheduledPair], invokers)) { (tuple, msg: ContainerCreationMessage) =>
        val pairs = tuple._1
        val candidates = tuple._2

        val requiredResources =
          msg.whiskActionMetaData.annotations
            .getAs[Seq[String]](Annotations.InvokerResourcesAnnotationName)
            .getOrElse(Seq.empty[String])
        val resourcesStrictPolicy = msg.whiskActionMetaData.annotations
          .getAs[Boolean](Annotations.InvokerResourcesStrictPolicyAnnotationName)
          .getOrElse(true)
        val isBlackboxInvocation = msg.whiskActionMetaData.toExecutableWhiskAction.exists(_.exec.pull)
        if (requiredResources.isEmpty) {
          // only choose managed invokers or blackbox invokers
          val wantedInvokers = if (isBlackboxInvocation) {
            logging.info(this, s"[${msg.invocationNamespace}/${msg.action}] looking for blackbox invokers to schedule.")
            candidates
              .filter(
                c =>
                  blackboxInvokers
                    .map(b => b.id.instance)
                    .contains(c.id.instance) && c.id.userMemory.toMB >= msg.whiskActionMetaData.limits.memory.megabytes)
              .toSet
          } else {
            logging.info(this, s"[${msg.invocationNamespace}/${msg.action}] looking for managed invokers to schedule.")
            candidates
              .filter(
                c =>
                  managedInvokers
                    .map(m => m.id.instance)
                    .contains(c.id.instance) && c.id.userMemory.toMB >= msg.whiskActionMetaData.limits.memory.megabytes)
              .toSet
          }
          val taggedInvokers = candidates.filter(_.id.tags.nonEmpty)

          if (wantedInvokers.nonEmpty) {
            val scheduledPair = chooseInvokerFromCandidates(wantedInvokers.toList, msg)
            val updatedInvokers =
              updateInvokerMemory(scheduledPair.invokerId, msg.whiskActionMetaData.limits.memory.megabytes, invokers)
            (scheduledPair :: pairs, updatedInvokers)
          } else if (taggedInvokers.nonEmpty) { // if not found from the wanted invokers, choose tagged invokers then
            logging.info(
              this,
              s"[${msg.invocationNamespace}/${msg.action}] since there is no available non-tagged invoker, choose one among tagged invokers.")
            val scheduledPair = chooseInvokerFromCandidates(taggedInvokers, msg)
            val updatedInvokers =
              updateInvokerMemory(scheduledPair.invokerId, msg.whiskActionMetaData.limits.memory.megabytes, invokers)
            (scheduledPair :: pairs, updatedInvokers)
          } else {
            logging.error(
              this,
              s"[${msg.invocationNamespace}/${msg.action}] there is no invoker available to schedule to schedule.")
            val scheduledPair =
              ScheduledPair(msg, invokerId = None, Some(NoAvailableInvokersError))
            (scheduledPair :: pairs, invokers)
          }
        } else {
          logging.info(this, s"[${msg.invocationNamespace}/${msg.action}] looking for tagged invokers to schedule.")
          val wantedInvokers = candidates.filter(health => requiredResources.toSet.subsetOf(health.id.tags.toSet))
          if (wantedInvokers.nonEmpty) {
            val scheduledPair = chooseInvokerFromCandidates(wantedInvokers, msg)
            val updatedInvokers =
              updateInvokerMemory(scheduledPair.invokerId, msg.whiskActionMetaData.limits.memory.megabytes, invokers)
            (scheduledPair :: pairs, updatedInvokers)
          } else if (resourcesStrictPolicy) {
            logging.error(
              this,
              s"[${msg.invocationNamespace}/${msg.action}] there is no available invoker with the resource: ${requiredResources}")
            val scheduledPair =
              ScheduledPair(msg, invokerId = None, Some(NoAvailableResourceInvokersError))
            (scheduledPair :: pairs, invokers)
          } else {
            logging.info(
              this,
              s"[${msg.invocationNamespace}/${msg.action}] since there is no available invoker with the resource, choose any invokers without the resource.")
            val (noTaggedInvokers, taggedInvokers) = candidates.partition(_.id.tags.isEmpty)
            if (noTaggedInvokers.nonEmpty) { // choose no tagged invokers first
              val scheduledPair = chooseInvokerFromCandidates(noTaggedInvokers, msg)
              val updatedInvokers =
                updateInvokerMemory(scheduledPair.invokerId, msg.whiskActionMetaData.limits.memory.megabytes, invokers)
              (scheduledPair :: pairs, updatedInvokers)
            } else {
              val leftInvokers =
                taggedInvokers.filterNot(health => requiredResources.toSet.subsetOf(health.id.tags.toSet))
              if (leftInvokers.nonEmpty) {
                val scheduledPair = chooseInvokerFromCandidates(leftInvokers, msg)
                val updatedInvokers =
                  updateInvokerMemory(
                    scheduledPair.invokerId,
                    msg.whiskActionMetaData.limits.memory.megabytes,
                    invokers)
                (scheduledPair :: pairs, updatedInvokers)
              } else {
                logging.error(this, s"[${msg.invocationNamespace}/${msg.action}] no available invoker is found")
                val scheduledPair =
                  ScheduledPair(msg, invokerId = None, Some(NoAvailableInvokersError))
                (scheduledPair :: pairs, invokers)
              }
            }
          }
        }
      }
      ._1 // pairs
    list
  }

  @tailrec
  protected[container] def chooseInvokerFromCandidates(candidates: List[InvokerHealth], msg: ContainerCreationMessage)(
    implicit logging: Logging): ScheduledPair = {
    val requiredMemory = msg.whiskActionMetaData.limits.memory
    if (candidates.isEmpty) {
      ScheduledPair(msg, invokerId = None, Some(NoAvailableInvokersError))
    } else if (candidates.forall(p => p.id.userMemory.toMB < requiredMemory.megabytes)) {
      ScheduledPair(msg, invokerId = None, Some(NoAvailableResourceInvokersError))
    } else {
      val idx = rng(mod = candidates.size)
      val instance = candidates(idx)
      if (instance.id.userMemory.toMB < requiredMemory.megabytes) {
        val split = candidates.splitAt(idx)
        val _ :: t1 = split._2
        chooseInvokerFromCandidates(split._1 ::: t1, msg)
      } else {
        ScheduledPair(msg, invokerId = Some(instance.id))
      }
    }
  }

  private def sendState(msg: ContainerCreationMessage, err: ContainerCreationError, reason: String)(
    implicit logging: Logging): Unit = {
    val state = FailedCreationJob(msg.creationId, msg.invocationNamespace, msg.action, msg.revision, err, reason)
    QueuePool.get(MemoryQueueKey(state.invocationNamespace, state.action.toDocId.asDocInfo(state.revision))) match {
      case Some(memoryQueueValue) if memoryQueueValue.isLeader =>
        memoryQueueValue.queue ! state
      case _ =>
        logging.error(this, s"get a $state for a nonexistent memory queue or a follower")
    }
  }

  protected[scheduler] def getAvailableInvokers(etcd: EtcdClient, minMemory: ByteSize, invocationNamespace: String)(
    implicit executor: ExecutionContext): Future[List[InvokerHealth]] = {
    etcd
      .getPrefix(InvokerKeys.prefix)
      .map { res =>
        res.getKvsList.asScala
          .map { kv =>
            InvokerResourceMessage
              .parse(kv.getValue.toString(StandardCharsets.UTF_8))
              .map { resourceMessage =>
                val status = resourceMessage.status match {
                  case Healthy.asString   => Healthy
                  case Unhealthy.asString => Unhealthy
                  case _                  => Offline
                }

                val temporalId = InvokerKeys.getInstanceId(kv.getKey.toString(StandardCharsets.UTF_8))
                val invoker = temporalId.copy(
                  userMemory = resourceMessage.freeMemory.MB,
                  busyMemory = Some(resourceMessage.busyMemory.MB),
                  tags = resourceMessage.tags,
                  dedicatedNamespaces = resourceMessage.dedicatedNamespaces)

                InvokerHealth(invoker, status)
              }
              .getOrElse(InvokerHealth(InvokerInstanceId(kv.getKey, userMemory = 0.MB), Offline))
          }
          .filter(i => i.status.isUsable)
          .filter(_.id.userMemory >= minMemory)
          .filter { invoker =>
            invoker.id.dedicatedNamespaces.isEmpty || invoker.id.dedicatedNamespaces.contains(invocationNamespace)
          }
          .toList
      }
  }

  protected[scheduler] def getAvailableInvokers(etcd: EtcdClient, minMemory: ByteSize)(
    implicit executor: ExecutionContext): Future[List[InvokerHealth]] = {
    etcd
      .getPrefix(InvokerKeys.prefix)
      .map { res =>
        res.getKvsList.asScala
          .map { kv =>
            InvokerResourceMessage
              .parse(kv.getValue.toString(StandardCharsets.UTF_8))
              .map { resourceMessage =>
                val status = resourceMessage.status match {
                  case Healthy.asString   => Healthy
                  case Unhealthy.asString => Unhealthy
                  case _                  => Offline
                }

                val temporalId = InvokerKeys.getInstanceId(kv.getKey.toString(StandardCharsets.UTF_8))
                val invoker = temporalId.copy(
                  userMemory = resourceMessage.freeMemory.MB,
                  busyMemory = Some(resourceMessage.busyMemory.MB),
                  tags = resourceMessage.tags,
                  dedicatedNamespaces = resourceMessage.dedicatedNamespaces)
                InvokerHealth(invoker, status)
              }
              .getOrElse(InvokerHealth(InvokerInstanceId(kv.getKey, userMemory = 0.MB), Offline))
          }
          .filter(i => i.status.isUsable)
          .filter(_.id.userMemory >= minMemory)
          .toList
      }
  }

}

case class NoCapacityException(msg: String) extends Exception(msg)
