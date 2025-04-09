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
import java.io.{BufferedReader, FileReader, File, FileWriter, PrintWriter}
import java.util.concurrent.ConcurrentHashMap

import spray.json._
// import DefaultJsonProtocol._
// import scala.io.Source
// import akka.pattern.ask
import akka.util.Timeout
import scala.concurrent.duration._


import org.apache.calcite.sql.parser.SqlParser
import org.apache.calcite.sql.util.SqlBasicVisitor
import org.apache.calcite.sql.SqlIdentifier
import org.apache.calcite.sql.SqlCall
import org.apache.calcite.sql.SqlKind
import org.apache.calcite.sql.SqlSelect

// (implicit logging: Logging)

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
      logging.info(this, s"sql: ${sql}")
      logging.info(this, s"涉及的表为: ${tableNames}")
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
      
  implicit val timeout: Timeout = Timeout(5.seconds) // 正确写法
  private implicit val ec: ExecutionContextExecutor = context.dispatcher
  
  import scala.collection.mutable.Queue
  // 用于存储所有等待状态的请求，保证先进先出
  // 元素的三个属性分别代表：
  // 1: msg: ContainerCreationMessage
  // 2: isWarm: true代表为热容器，false代表为工作中容器
  // 3: containerId
  private val waitingQueue = Queue[(ContainerCreationMessage, Option[Int], Option[String])]()

  // 记录已经等待过的请求，防止一个请求在调度中决策多次等待
  val waitingAWarmContainerCreationId = scala.collection.mutable.Set[String]()

  private val warmedContainerStates = TrieMap[String, Boolean]() // Key: 容器id containerId, Value: true if in use, false if idle

  // 记录收到请求的时间
  private val requestTimes = new ConcurrentHashMap[String, Long]()
  // 记录调度时间的文件路径
  private val scheduleTimePath = "/db/schedule_time.txt"
  
  private val creationJobManager = jobManagerFactory(context)

  private val messagingProducer = provider.getProducer(config)

  private var warmedContainers = Set.empty[String]

  private val warmedInvokers = TrieMap[Int, String]()

  private val inProgressWarmedContainers = TrieMap.empty[String, String]

  private val warmKey = ContainerKeys.warmedPrefix
  private val invokerKey = InvokerKeys.prefix
  private val watcherName = s"container-manager"

  // 确保调度时间的文件存在
  private def ensureScheduleTimeFileExists()(implicit logging: Logging): Unit = {
    logging.info(this, "正在初始化调度时间记录文件")
    val file = new File(scheduleTimePath)
    val dir = file.getParentFile
    if (!dir.exists()) {
      dir.mkdirs()
    }
    
    // 如果文件存在就删除并新建一个空的
    if (file.exists()) {
      try {
        file.delete()
        logging.info(this, s"成功删除已存在的调度时间记录文件: ${file.getAbsolutePath}")
      } catch {
        case e: Exception =>
          logging.error(this, s"删除已存在的调度时间记录文件失败: ${e.getMessage}")
          return // 如果删除失败则直接返回，不继续创建新文件
      }
    }

    // 创建新文件
    try {
      file.createNewFile()
      logging.info(this, s"成功创建调度时间记录文件: ${file.getAbsolutePath}")
    } catch {
      case e: Exception => 
        logging.error(this, s"创建调度时间记录文件失败: ${e.getMessage}")
    }
  }

  // 将调度时间追加到日志文件
  private def appendToLogFile(schedulingTimeMs: Long): Unit = {
    val writer = new PrintWriter(new FileWriter(scheduleTimePath, true))
    try {
      // 追加逗号和时间值到文件末尾
      writer.print(s"$schedulingTimeMs,")
      // 不换行，直接刷新
      writer.flush()
    } finally {
      writer.close()
    }
  }

  
  // 尝试重新调度等待态msg
  def rescheduleWaitingMessage(): Unit = {
    // logging.info(this, s"try to reschedule waiting message, waitingQueue: $waitingQueue")
    logging.info(this, s"try to reschedule waiting message")
    // 优先的重新调度列表（等待的容器已返回）
    val priorityMsgsToReschedule = List.newBuilder[ContainerCreationMessage]
    // 普通的重新调度列表（等待的容器未返回）
    val msgsToReschedule = List.newBuilder[ContainerCreationMessage]
    // val remainingQueue = Queue[(ContainerCreationMessage, Option[Int], Option[String])]()
          

    while (waitingQueue.nonEmpty) {
      val (msg, invokerOpt, containerOpt) = waitingQueue.dequeue()
      containerOpt match {
        // 如果这个msg等待的容器已经warm了
        case Some(containerId) if warmedContainers.exists(_.split("/").last == containerId) =>
          logging.info(this, s"rescheduling msg: ${msg.creationId.asString} as container $containerId is now warm.")
          waitingAWarmContainerCreationId += msg.creationId.asString 
          
          // 新增Map，来为msg锁定该容器
          TimePredictor.addWaitingCreationIdToContainerIds(msg.creationId.asString, containerId)

          // 将符合条件的消息加入待调度的列表
          priorityMsgsToReschedule += msg
        
        // 不放在上一个if条件的情况中，目的是不指定容器，只要有热容器返回就重新调度，不管返回的容器有没有被等待
        case Some(containerId) =>  
          logging.info(this, s"try to rescheduling msg: ${msg.creationId.asString} but container $containerId is still working.")

          // 需要将containerId从被等待的集合中移除，否则重新调度时会跳过它
          TimePredictor.removeWaitingContainerIds(containerId)

          // 将符合条件的消息加入待调度的列表
          msgsToReschedule += msg
        
        case _ =>
        //   // 如果容器还不可用，将请求放回 remainingQueue 中
        //   remainingQueue.enqueue((msg, invokerOpt, containerOpt))
      }
    }

    // // 将未重新调度的请求放回 waitingQueue
    // if(remainingQueue.nonEmpty) {
    //   waitingQueue ++= remainingQueue
    //   logging.info(this, s"remainingQueue is not empty, waitingQueue after ++ : $waitingQueue, remainingQueue: $remainingQueue")
    // }

    // 如果有需要重新调度的消息，将其打包成 List 并发送给自身(不直接调用createContainer，否则可能会出现并行的情况，造成同一容器的争抢)
    
    val priorityRescheduledMsgs = priorityMsgsToReschedule.result()
    val rescheduledMsgs = msgsToReschedule.result()
    if (priorityRescheduledMsgs.nonEmpty) {
      // 调用 createContainer 处理优先队列
      createContainer(
        priorityRescheduledMsgs,
        priorityRescheduledMsgs.head.whiskActionMetaData.limits.memory.megabytes.MB,
        priorityRescheduledMsgs.head.invocationNamespace
      ).onComplete {
        case scala.util.Success(_) =>
          logging.info(this, s"优先队列中的 ${priorityRescheduledMsgs.size} 个msg 调度完成.")

          if (rescheduledMsgs.nonEmpty) {
            // 在优先队列完成后，再处理非优先队列的任务
            createContainer(
              rescheduledMsgs,
              rescheduledMsgs.head.whiskActionMetaData.limits.memory.megabytes.MB,
              rescheduledMsgs.head.invocationNamespace
            )
            logging.info(this, s"Sent ${rescheduledMsgs.size} messages for rescheduling.")
          }

        case scala.util.Failure(ex) =>
          logging.error(this, s"优先队列调度失败: ${ex.getMessage}")
      }

    } else if (rescheduledMsgs.nonEmpty) {
      // 如果没有优先队列，直接处理剩余的消息
      createContainer(
        rescheduledMsgs,
        rescheduledMsgs.head.whiskActionMetaData.limits.memory.megabytes.MB,
        rescheduledMsgs.head.invocationNamespace
      )
      logging.info(this, s"Sent ${rescheduledMsgs.size} messages for rescheduling.")
    }

    // 此处不需要updateWaitingContainerIds，因为是暂时的出队，在filter时会更新
  }

  watcherService ! WatchEndpoint(warmKey, "", isPrefix = true, watcherName, Set(PutEvent, DeleteEvent))
  watcherService ! WatchEndpoint(invokerKey, "", isPrefix = true, watcherName, Set(PutEvent, DeleteEvent))

  override def receive: Receive = {
    case ContainerCreation(msgs, memory, invocationNamespace) =>
      // logging.info(this, s"receive message ContainerCreation from ${sender()}, msgs:$msgs")
      logging.info(this, s"receive message ContainerCreation from ${sender()}")
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
      logging.info(this, s"receive message rescheduling")
      val msg = rescheduling.toCreationMessage(schedulerInstanceId, rescheduling.retry + 1)
      createContainer(
        List(msg),
        rescheduling.actionMetaData.limits.memory.megabytes.MB,
        rescheduling.invocationNamespace)

    case watchMessage @ WatchEndpointInserted(watchKey, key, _, true) =>
      // logging.info(this, s"received message: watchMessage: $watchMessage")
      watchKey match {
        case `warmKey` => 
          logging.info(this, s"receive message WatchEndpointInserted : case `warmKey`, key:$key")
          warmedContainers += key
          logging.info(this, s"warmedContainers after += :$warmedContainers")

          // 获取路径中最后的部分，即 containerId
          val containerId = key.split("/").last 
          // 更新容器状态为未被使用
          warmedContainerStates.update(containerId, false)
          logging.info(this, s"容器id: $containerId 任务完成, 置warmedContainerStates为false")

          // 更新对应的 dbinfo 条目，将状态置为 "warm"，并清除 updateStateTimestamp
          ContainerDbInfoManager.updateStateToWarm(containerId)
          
          // 有新热容器加入，尝试调度等待态的msg
          if (waitingQueue.nonEmpty){
            rescheduleWaitingMessage()
          }

        case `invokerKey` =>
          logging.info(this, s"receive message WatchEndpointInserted : case `invokerKey`, key:$key")
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
    
    // 使用 ContainerDbInfoManager 查找 dbName
    ContainerDbInfoManager.findDbNameByContainerId(containerId) match {
      case Some(dbName) =>
        // 找到了，删除 .db 文件
        val dbFilePath = Paths.get(s"/db/$dbName")
        try {
          if (Files.exists(dbFilePath)) {
            Files.delete(dbFilePath)
            logging.info(this, s"Deleted database file: $dbName")
          } else {
            logging.error(this, s"try to delete container: $containerId, but its .db file not found: $dbName")
          }
        } catch {
          case e: Exception =>
            logging.error(this, s"Failed to delete database file: $dbName. Error: ${e.getMessage}")
        }

        // 从内存缓存中移除容器信息
        ContainerDbInfoManager.removeContainerInfo(containerId)
        logging.info(this, s"Deleted dbInfo for containerId: $containerId")

      case None =>
        logging.error(this, s"No database information found for containerId: $containerId. Nothing to delete")
    }
  }
  
  private def createContainer(msgs: List[ContainerCreationMessage], memory: ByteSize, invocationNamespace: String)(
    implicit logging: Logging): Future[Unit] = {
    // 改为Future[Unit]

    // 记录收到请求的时间
    val currentTimeMs = System.currentTimeMillis()
    msgs.foreach { msg =>
      requestTimes.put(msg.creationId.asString, currentTimeMs)
    }

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
      .map { invokers =>
        if (invokers.isEmpty) {
          // 处理无可用invoker的情况
          logging.error(this, "there is no available invoker to schedule.")
          msgs.foreach(ContainerManager.sendState(_, NoAvailableInvokersError, NoAvailableInvokersError))
        } else {
          val (coldCreations, warmedCreations, waitingCreations) =
            ContainerManager.filterWarmedCreations(waitingQueue, warmedContainers, warmedContainerStates, inProgressWarmedContainers, invokers, msgs, waitingAWarmContainerCreationId)
          // logging.info(this, s"coldCreations: $coldCreations")
          // logging.info(this, s"warmedCreations: $warmedCreations")
          // logging.info(this, s"waitingCreations: $waitingCreations")

          // 处理等待态msg
          if(!waitingCreations.isEmpty) {
            // 使用同步块，避免多个线程同时操作waitingQueue，导致新值覆盖旧值
            this.synchronized {
              waitingQueue ++= waitingCreations
              // logging.info(this, s"waitingQueue after ++ : $waitingQueue")
            }
          }

          // 冷启动创建的预处理
          coldCreations.foreach { case (msg, _, _) =>
            logging.info(this, s"cold start msg: ${msg.creationId.asString}")
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
            // logging.info(this, s"get sql_query: $sql_query")
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
              val predictWorkingTime = msg.args.flatMap(obj => obj.fields.get("predict_time") match {
                case Some(JsNumber(value)) => Some(value.toDouble) // 将 JsValue 提取为 Double
                case _ => None
              })
              val isPrewarm = msg.args.exists(_.fields.get("db_file").exists(_.convertTo[String] == "prewarm"))
              val state = if (isPrewarm) "prewarming" else "creating"
              ContainerDbInfoManager.createDbInfo(msg.creationId.asString, s"${msg.creationId.asString}.db", state, tables, predictWorkingTime)
              StartInfoManager.createStartInfo(msg.creationId.asString, "wait", "wait", s"${msg.creationId.asString}.db", "cold")
            }
            else {
              logging.error(this, s"dbFile $dbPath already exists!")
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
                    // logging.info(this, s"pair.msg: ${pair.msg}")
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
      .map { _ => // 添加一个map操作来获取调度时间并记录
        msgs.foreach { msg =>
          val requestId = msg.creationId.asString
          if (requestTimes.containsKey(requestId)) {
            val receiveTime = requestTimes.remove(requestId)
            val currentTimeMs = System.currentTimeMillis()
            val schedulingTimeMs = currentTimeMs - receiveTime
            
            // 将调度时间写入文件
            appendToLogFile(schedulingTimeMs)
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
  
  // 初始化各组件
  ensureScheduleTimeFileExists()
  ContainerDbInfoManager.init()
  StartInfoManager.init()
  ContainerRemoveManager.init()
  TimePredictor.initWaitingTimeRecorder()
  RequestRecordManager.startFlush()
}

object ContainerManager {
  import scala.collection.mutable.Queue

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
  protected[container] def filterWarmedCreations(waitingQueue: Queue[(ContainerCreationMessage, Option[Int], Option[String])],
                                                 warmedContainers: Set[String],
                                                 warmedContainerStates: TrieMap[String, Boolean],
                                                 inProgressWarmedContainers: TrieMap[String, String],
                                                 invokers: List[InvokerHealth],
                                                 msgs: List[ContainerCreationMessage],
                                                 waitingAWarmContainerCreationId: scala.collection.mutable.Set[String])(
    implicit logging: Logging): (List[(ContainerCreationMessage, Option[Int], Option[String])],
                                List[(ContainerCreationMessage, Option[Int], Option[String])],
                                List[(ContainerCreationMessage, Option[Int], Option[String])]) = {

    // 三个列表分别存放冷启动、热启动和需要等待的 working 容器
    val coldCreations = List.newBuilder[(ContainerCreationMessage, Option[Int], Option[String])]
    val warmedCreations = List.newBuilder[(ContainerCreationMessage, Option[Int], Option[String])]
    val waitingCreations = List.newBuilder[(ContainerCreationMessage, Option[Int], Option[String])]

    // 对非冷启动决策，划分等待策略与热启动策略
    def decideWarmedOrWaiting(msg: ContainerCreationMessage, containerId: String, isWarm: Boolean): Unit = {
      if (isWarm) {
        // 如果在预处理过程中，另一个请求也被分配到同一容器，在invoker处理时会报错: [CreationJobManager] [guest/remote_data_to_run_sql@0.0.50] [d4e6b1e7fa4b4b6aa6b1e7fa4bab6ad6] Failed to create container 0/5 times for creationId: d4e6b1e7fa4b4b6aa6b1e7fa4bab6ad6, invoker[invoker0/0] doesn't have enough resource for container: guest/remote_data_to_run_sql@0.0.50. Started rescheduling
        logging.info(this, s"Warm container:$containerId selected for msg: $msg.")
        val chosenInvoker = warmedContainers
          .find(_.contains(containerId))
          .map(_.split("/").takeRight(3).apply(0))
          .filter(invoker => invokers.filter(_.status.isUsable).map(_.id.instance).contains(invoker.toInt))

        // 如果没有找到 invoker，则降级为默认值 0
        val invokerId = chosenInvoker.map(_.toInt).getOrElse {
          logging.warn(this, s"选择了热容器 $containerId (state: ${if (isWarm) "warm" else "working"}). 但是 warmedContainers 中没有找到对应的 invoker, 使用默认值 0.")
          logging.warn(this, s"warmedContainers: $warmedContainers")
          0
        }

        // 加入 warmedCreations 列表
        warmedCreations += ((msg, Some(invokerId), Some(containerId)))
      
        // 更新 warmedContainerStates 和 inProgressWarmedContainers
        warmedContainerStates.update(containerId, true)
        logging.info(this, s"容器id: $containerId 被分配, 置warmedContainerStates为true")

        val predictWorkingTime = msg.args.flatMap(obj => obj.fields.get("predict_time") match {
          case Some(JsNumber(value)) => Some(value.toDouble) // 将 JsValue 提取为 Double
          case _ => None
        })
        ContainerDbInfoManager.updatePredictWorkingTime(containerId, predictWorkingTime)
        
        // 更新 inProgressWarmedContainers
        warmedContainers.find(_.contains(containerId)) match {
          case Some(containerInfo) => 
            inProgressWarmedContainers.update(msg.creationId.asString, containerInfo)
          case None                => 
            logging.warn(this, s"未找到匹配的 warmedContainer, 使用默认值更新 inProgressWarmedContainers.")
            inProgressWarmedContainers.update(msg.creationId.asString, s"whisk/warmed/guest/guest/remote_data_to_run_sql/48-bdb54363ce1e6132d05aad2f8fdce111/invoker/0/container/${containerId}")
        }
        
        // 创建并更新 StartInfo
        StartInfoManager.createStartInfo(msg.creationId.asString, "wait", containerId, "wait", "warm")
        StartInfoManager.updateContainerIdByCreationId(msg.creationId.asString, containerId)
        
      } else {
        waitingCreations += ((msg, None, Some(containerId))) // 加入等待列表
        logging.info(this, s"creationId: ${msg.creationId.asString} 选择了等待策略, 加入waitingCreations")
        
        // 及时更新waitingContainerIds，防止在同一个filterWarmedCreations函数中的请求被分到同一个waiting container中
        TimePredictor.addWaitingContainerIds(containerId)
      }
    }

    
    msgs.foreach { msg =>
      val isPrewarm = msg.args.exists(_.fields.get("db_file").exists(_.convertTo[String] == "prewarm"))

      if (isPrewarm) {
        // 如果是 prewarm，直接加入冷启动列表
        logging.info(this, s"收到预热请求, 直接冷启动")
        coldCreations += ((msg, None, None))
      } else {    
        
        logging.info(this, s"filterWarmedCreations() msg creationId: ${msg.creationId.asString}")
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

        RequestRecordManager.addRequestRecord(msg.creationId.asString, tablesNeeded)

        // 判断该 msg 是否等待着一个已返回的容器
        val waitingAWarmContainer: Boolean = waitingAWarmContainerCreationId.contains(msg.creationId.asString)
        if (waitingAWarmContainer){
          logging.info(this, s"msg creationId: ${msg.creationId.asString} 所等待的容器已返回, 在热容器列表中!")
        }

        // 选择等待时间最短的策略，包括冷启动、warm 容器和 working 容器的等待时间
        logging.info(this, s"为 msg creationId: ${msg.creationId.asString} 选择最佳策略...")
        val optimalStrategy = TimePredictor.predictWaitTimeAndGetOptimal(msg.creationId.asString, tablesNeeded, waitingAWarmContainer)
        logging.info(this, s"${msg.creationId.asString} 选择最佳策略为 $optimalStrategy")

        optimalStrategy match {
          case ("coldStart", _, _) =>
            RequestRecordManager.addWaitAndColdRequestRecord(msg.creationId.asString, tablesNeeded, "cold")
            // 如果选择的是冷启动
            // logging.info(this, s"cold start selected for msg: $msg.")
            if (waitingAWarmContainer){
              logging.error(this, s"msg creationId: ${msg.creationId.asString} 所等待的容器已返回, 但决定冷启动!")
              logging.error(this, s"dbInfo: ${ContainerDbInfoManager.getDbInfo()}")
            }
            coldCreations += ((msg, None, None)) // 加入冷启动列表

          case (optimalContainerId, _, isWarm) =>
            // 如果选择的不是冷启动，查看是热启动还是等待
            decideWarmedOrWaiting(msg, optimalContainerId, isWarm)
            
            // 检查之前的wait调度决策是否合理
            if (waitingAWarmContainer){
              if (isWarm){
                waitingAWarmContainerCreationId -= msg.creationId.asString
              } else{
                logging.error(this, s"msg creationId: ${msg.creationId.asString} 所等待的容器已返回, 但选择了一个等待中容器!")
                logging.error(this, s"dbInfo: ${ContainerDbInfoManager.getDbInfo()}")
              }
            }

            // 对热启动，将状态置为loading
            if (isWarm){
              val predictWorkingTime = msg.args.flatMap(obj => obj.fields.get("predict_time") match {
                case Some(JsNumber(value)) => Some(value.toDouble) // 将 JsValue 提取为 Double
                case _ => None
              })
              ContainerDbInfoManager.updatePredictWorkingTime(optimalContainerId, predictWorkingTime)
              ContainerDbInfoManager.updateDbInfoTables(optimalContainerId, tablesNeeded)
            } else {
              RequestRecordManager.addWaitAndColdRequestRecord(msg.creationId.asString, tablesNeeded, "wait")
            }
          }
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
