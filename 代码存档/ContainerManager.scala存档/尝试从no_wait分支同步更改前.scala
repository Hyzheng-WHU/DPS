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
// import java.io.{BufferedReader, FileReader, File, PrintWriter}
import java.io.{BufferedReader, FileReader}

import spray.json._
// import DefaultJsonProtocol._
// import scala.io.Source

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

  import scala.collection.mutable.Queue
  // 用于存储所有等待状态的请求，保证先进先出
  private val waitingQueue = Queue[(ContainerCreationMessage, Option[Int], Option[String])]()

  private val warmedContainerStates = TrieMap[String, Boolean]() // Key: 容器id containerId, Value: true if in use, false if idle

  private val creationJobManager = jobManagerFactory(context)

  private val messagingProducer = provider.getProducer(config)

  private var warmedContainers = Set.empty[String]

  private val warmedInvokers = TrieMap[Int, String]()

  private val inProgressWarmedContainers = TrieMap.empty[String, String]

  private val warmKey = ContainerKeys.warmedPrefix
  private val invokerKey = InvokerKeys.prefix
  private val watcherName = s"container-manager"

  // 尝试重新调度等待态msg
  def rescheduleWaitingMessage(): Unit = {
    val remainingQueue = Queue[(ContainerCreationMessage, Option[Int], Option[String])]()

    while (waitingQueue.nonEmpty) {
      val (msg, invokerOpt, containerOpt) = waitingQueue.dequeue()
      containerOpt match {
        case Some(containerId) if warmedContainers.contains(containerId) =>
          logging.info(this, s"Rescheduling msg: $msg as container $containerId is now warm.")
          
          // 重新调度等待请求
          createContainer(List(msg), msg.whiskActionMetaData.limits.memory.megabytes.MB, msg.invocationNamespace)

        case _ =>
          // 如果容器还不可用，将请求放回 remainingQueue 中
          remainingQueue.enqueue((msg, invokerOpt, containerOpt))
      }
    }

    // 将未重新调度的请求放回 waitingQueue
    waitingQueue ++= remainingQueue
  }

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
          // 有新热容器加入，尝试调度等待态的msg
          rescheduleWaitingMessage()

          // 获取路径中最后的部分，即 containerId
          val containerId = key.split("/").last 
          // 更新容器状态为未被使用
          warmedContainerStates.update(containerId, false)
          logging.info(this, s"容器id$containerId 任务完成, 置warmedContainerStates为false")

          // 更新对应的 dbinfo 条目，将状态置为 "warm"，并清除 workingTimestamp
          ContainerDbInfoManager.updateStateToWarm(containerId)

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
            // 如果状态为 true，说明容器被分配给某个msg使用，因此只从 warmedContainers 中移除，但不删除 db 文件
            logging.info(this, s"Container $key is in use. Removed from warmedContainers but kept DB.")
            
          case Some(false) =>
            // 如果状态为 false，说明容器超时未被重用，因此需要删除 db 文件
            logging.info(this, s"idle container: $key. Removed and cleaned up DB.")
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
    logging.info(this, s"try to delete container: $containerId, and its .db file")
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
  
  private def createContainer(msgs: List[ContainerCreationMessage], memory: ByteSize, invocationNamespace: String)(
    implicit logging: Logging): Unit = {
    // logging.info(this, s"received ${msgs.size} creation message [${msgs.head.invocationNamespace}:${msgs.head.action}]")
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
          val (coldCreations, warmedCreations, waitingCreations) =
            ContainerManager.filterWarmedCreations(warmedContainers, warmedContainerStates, inProgressWarmedContainers, invokers, msgs)
          logging.info(this, s"coldCreations: $coldCreations")
          logging.info(this, s"warmedCreations: $warmedCreations")
          logging.info(this, s"waitingCreations: $waitingCreations")

          // 处理等待态msg
          waitingQueue ++= waitingCreations

          // 冷启动创建的预处理
          coldCreations.foreach { case (msg, _, _) =>
            logging.info(this, s"cold start msg: $msg")
            val dbFile = s"/db/${msg.creationId.asString}.db"
            val dbPath = Paths.get(dbFile)
            // 提取 sql_query 或 sql_file 中的 SQL
            val sql_query: Option[String] = msg.args.flatMap { args =>
              // 检查是否有 `sql_file` 字段
              args.fields.get("sql_file").flatMap { sqlFile =>
                // 如果有 `sql_file` 字段，读取文件内容
                ContainerManager.readSqlFromFile(s"/sql/${sqlFile.convertTo[String].replaceAll("\"", "")}")
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
              val predictTime = msg.args.flatMap(obj => obj.fields.get("predict_time") match {
                case Some(JsNumber(value)) => Some(value.toDouble) // 将 JsValue 提取为 Double
                case _ => None
              })
              ContainerDbInfoManager.createDbInfo(msg.creationId.asString, s"${msg.creationId.asString}.db", tables, predictTime)
              StartInfoManager.createStartInfo(msg.creationId.asString, "wait", "wait", s"${msg.creationId.asString}.db", "cold")
            }
            else {
              logging.error(this, s"dbFile already exists!")
            }
          }

          // 处理热启动创建
          // handle warmed creation
          val chosenInvokers: immutable.Seq[Option[(Int, ContainerCreationMessage)]] = warmedCreations.map {
            warmedCreation =>
              logging.info(this, s"warm start msg: $warmedCreation")
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

          // 处理冷启动创建
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

    // 读取文件内容的函数（适用于 Scala 2.12 及以下版本）
  def readSqlFromFile(filePath: String)(implicit logging: Logging): Option[String] = {
    
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
      logging.error(this, s"SQL文件$filePath 不存在！")
      None
    }
  }

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
                                List[(ContainerCreationMessage, Option[Int], Option[String])],
                                List[(ContainerCreationMessage, Option[Int], Option[String])]) = {

    // 三个列表分别存放冷启动、热启动和需要等待的 working 容器
    val coldCreations = List.newBuilder[(ContainerCreationMessage, Option[Int], Option[String])]
    val warmedCreations = List.newBuilder[(ContainerCreationMessage, Option[Int], Option[String])]
    val waitingCreations = List.newBuilder[(ContainerCreationMessage, Option[Int], Option[String])]

    // 对非冷启动决策，划分等待策略与热启动策略
    def decideWarmedOrWaiting(msg: ContainerCreationMessage, containerId: String, isWarm: Boolean): Unit = {
      val chosenInvoker = warmedContainers
        .find(_.contains(containerId))
        .map(_.split("/").takeRight(3).apply(0))
        .filter(invoker => invokers.filter(_.status.isUsable).map(_.id.instance).contains(invoker.toInt))

      if (chosenInvoker.nonEmpty) {
        if (isWarm) {
          logging.info(this, s"Warm container:$containerId selected for msg: $msg.")
          warmedCreations += ((msg, Some(chosenInvoker.get.toInt), Some(containerId))) // 加入热启动列表

          // 更新 warmedContainerStates 和 inProgressWarmedContainers
          warmedContainerStates.update(containerId, true)
          logging.info(this, s"容器id: $containerId 被分配, 置warmedContainerStates为true")

          // 为防在准备过程中，另一个请求也被分配到这个container，将其状态置为loading
          ContainerDbInfoManager.updateStateToLoading(containerId)
          val predictTime = msg.args.flatMap(obj => obj.fields.get("predict_time") match {
            case Some(JsNumber(value)) => Some(value.toDouble) // 将 JsValue 提取为 Double
            case _ => None
          })
          ContainerDbInfoManager.updatePredictTime(containerId, predictTime)
          inProgressWarmedContainers.update(msg.creationId.asString, warmedContainers.find(_.contains(containerId)).get)
          
          // 创建并更新 StartInfo
          StartInfoManager.createStartInfo(msg.creationId.asString, "wait", containerId, "wait", "warm")
          StartInfoManager.updateContainerIdByCreationId(msg.creationId.asString, containerId)

        } else {
          logging.info(this, s"Working container selected for msg: $msg, will need to wait.")
          waitingCreations += ((msg, Some(chosenInvoker.get.toInt), Some(containerId))) // 加入等待列表
        }
      } else {
        // 如果chosenInvoker为空，系统出错，降级为冷启动
        logging.error(this, s"No usable invoker found for container $containerId (state: ${if (isWarm) "warm" else "working"}). cold start.")
        logging.error(this, s"warmedContainers: $warmedContainers")

        coldCreations +=  ((msg, None, None))
      }
    }

    
    msgs.foreach { msg =>
      logging.info(this, s"filterWarmedCreations() msg: $msg")
      val warmedPrefix =
        containerPrefix(ContainerKeys.warmedPrefix, msg.invocationNamespace, msg.action, Some(msg.revision))

      // 提取msg中的SQL语句
      val sqlQueryOpt: Option[String] = msg.args.flatMap { args =>
          // 检查是否有 sql_file 字段
          args.fields.get("sql_file").flatMap { sqlFile =>
            // 如果有 sql_file 字段，读取文件内容
            readSqlFromFile(s"/sql/${sqlFile.convertTo[String].replaceAll("\"", "")}")
          }.orElse {
            // 如果没有 sql_file 字段，检查是否有 sql_query
            Try(args.fields("sql_query").convertTo[String]).toOption
          }
        }

      // 提取SQL所需的表
      val tablesNeeded = sqlQueryOpt match {
        case Some(sqlQuery) => extractTableNames(sqlQuery)
        case None => List.empty[String]
      }
      logging.info(this, s"tablesNeeded $tablesNeeded")
      val coldStartDownloadTime = TableDownloadPredictor.predictDownloadTime(tablesNeeded, List.empty[String])
      val coldStartContainerInitTime = 350.0 // 假设冷启动的时间成本(ms)
      val coldStartTotalTime = coldStartDownloadTime + coldStartContainerInitTime

      // 尽量后置dbInfo的读取，防止读到更新前的值
      val dbInfo = ContainerDbInfoManager.readDbInfo()
      // 遍历 dbInfo 中的所有容器
      val containerWaitTimes = dbInfo.flatMap {
        case (containerId, info) =>
          logging.info(this, s"inspect container: $containerId, it is $info.state")
          info.state match {
            case "warm" =>
              // 对于 warm 容器，只计算缺失表的下载时间
              val existingTables = info.tables
              val warmDownloadTime = TableDownloadPredictor.predictDownloadTime(tablesNeeded, existingTables)
              logging.info(this, s"Predicted download time for warm container $containerId: $warmDownloadTime ms")
              Some((containerId, warmDownloadTime, true))

            case "working" =>
              // 对于 working 容器，计算缺失表的下载时间和剩余运行时间
              val existingTables = info.tables
              val workingDownloadTime = TableDownloadPredictor.predictDownloadTime(tablesNeeded, existingTables)

              // val elapsedTime = (System.currentTimeMillis() / 1000.0) - info.workingTimestamp.getOrElse(0.0)
              // // 检查 workingTimestamp 是否为空，计算已运行时间
              // val elapsedTime: Option[Double] = info.workingTimestamp.map { 
              //   timestamp =>
              //   (System.currentTimeMillis() / 1000.0) - timestamp
              // }.orElse {
              //   logging.error(this, s"Missing workingTimestamp for container $containerId.")
              //   None // 记录错误日志并跳过该条目
              // }

              // 获取并验证 workingTimestamp 和 predictTime
              val elapsedTimeOpt = info.workingTimestamp.map { timestamp =>
                (System.currentTimeMillis() / 1000.0) - timestamp
              }
              if (elapsedTimeOpt.isEmpty) {
                logging.error(this, s"Missing workingTimestamp for container $containerId. Skipping this container.")
              }

              val predictTimeOpt = info.predictTime
              if (predictTimeOpt.isEmpty) {
                logging.error(this, s"Missing predictTime, info is $info. Skipping this container.")
              }

              // 计算 remainingWorkTime，如果有 missing 值则跳过
              for {
                elapsedTime <- elapsedTimeOpt
                predictTime <- predictTimeOpt
              } yield {
                val timeLeft = predictTime - elapsedTime
                if (timeLeft < 0) {
                  logging.warn(this, s"Remaining work time for container $containerId is negative: $timeLeft ms.")
                }
                val remainingWorkTime = math.max(timeLeft, 0.0) // 确保剩余时间非负

                // 计算总等待时间并返回
                val workingTotalTime = workingDownloadTime + remainingWorkTime
                logging.info(this, s"Predicted total wait time for working container $containerId: $workingTotalTime ms")
                (containerId, workingTotalTime, false)
              }

              // val remainingWorkTime = math.max(info.predictTime.getOrElse(0.0) - elapsedTime, 0.0)
              // // 检查 predictTime 是否为空，计算剩余运行时间
              // val remainingWorkTime = info.predictTime match {
              //   case Some(predictTime) =>
              //     val timeLeft = predictTime - elapsedTime
              //     if (timeLeft < 0) {
              //       // 预测出错，预测运行时间过短导致预测剩余时间 < 0，置为0
              //       logging.warn(this, s"Remaining work time for container $containerId is negative: $timeLeft ms.")
              //     }
              //     math.max(timeLeft, 0.0) // 确保剩余时间为非负值
              //   case None =>
              //     logging.error(this, s"Missing predictTime for container $containerId. Skipping this container.")
              //     None // 记录错误日志并跳过该条目
              // }

              // val workingTotalTime = workingDownloadTime + remainingWorkTime
              // logging.info(this, s"Predicted total wait time for working container $containerId: $workingTotalTime ms")
              // Some((containerId, workingTotalTime))

            case _ =>
              None // 非 warm 或 working 状态的容器跳过
          }
      }.toSeq

      // 构建候选策略列表，包括冷启动的总时间
      // val candidateStrategies = containerWaitTimes :+ ("coldStart", coldStartTotalTime)
      val candidateStrategies = containerWaitTimes.map { case (id, time, isWarm) => (id, time, isWarm) } :+ ("coldStart", coldStartTotalTime, false)
      logging.info(this, s"Candidate strategies (containerId, totalTime): $candidateStrategies")

      // 选择等待时间最短的策略，包括冷启动、warm 容器和 working 容器的等待时间
      val optimalStrategy = candidateStrategies.minBy { case (_, totalTime, _) => totalTime }
      logging.info(this, s"选择最佳策略为$optimalStrategy")

      optimalStrategy match {
        case ("coldStart", _, _) =>
          // 如果选择的是冷启动
          logging.info(this, s"cold start selected for msg: $msg.")
          coldCreations += ((msg, None, None)) // 加入冷启动列表

        case (optimalContainerId, _, isWarm) =>
          // 如果选择的不是冷启动，查看是热启动还是等待
          decideWarmedOrWaiting(msg, optimalContainerId, isWarm)
      }
    }
    (coldCreations.result(), warmedCreations.result(), waitingCreations.result())
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
