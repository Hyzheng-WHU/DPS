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

package org.apache.openwhisk.core.containerpool.v2

import java.net.InetSocketAddress
import java.time.Instant
import akka.actor.Status.{Failure => FailureMessage}
import akka.actor.{actorRef2Scala, ActorRef, ActorRefFactory, ActorSystem, FSM, Props, Stash}
import akka.event.Logging.InfoLevel
import akka.io.{IO, Tcp}
import akka.pattern.pipe
import org.apache.openwhisk.common.tracing.WhiskTracerProvider
import org.apache.openwhisk.common.{LoggingMarkers, TransactionId, _}
import org.apache.openwhisk.core.ConfigKeys
import org.apache.openwhisk.core.ack.ActiveAck
import org.apache.openwhisk.core.connector.{
  ActivationMessage,
  CombinedCompletionAndResultMessage,
  CompletionMessage,
  ResultMessage
}
import org.apache.openwhisk.core.containerpool._
import org.apache.openwhisk.core.containerpool.logging.LogCollectingException
import org.apache.openwhisk.core.containerpool.v2.FunctionPullingContainerProxy.{
  constructWhiskActivation,
  containerName
}
import org.apache.openwhisk.core.database._
import org.apache.openwhisk.core.entity.ExecManifest.ImageName
import org.apache.openwhisk.core.entity.size._
import org.apache.openwhisk.core.entity.{ExecutableWhiskAction, ActivationResponse => ExecutionResponse, _}
import org.apache.openwhisk.core.etcd.EtcdKV.ContainerKeys
import org.apache.openwhisk.core.invoker.Invoker.LogsCollector
import org.apache.openwhisk.core.invoker.NamespaceBlacklist
import org.apache.openwhisk.core.scheduler.SchedulerEndpoints
import org.apache.openwhisk.core.service.{RegisterData, UnregisterData}
import org.apache.openwhisk.grpc.RescheduleResponse
import org.apache.openwhisk.http.Messages
import pureconfig.loadConfigOrThrow
import spray.json.DefaultJsonProtocol.{StringJsonFormat, _}
import spray.json._
import pureconfig.generic.auto._

import scala.concurrent.duration._
import scala.concurrent.Future
import scala.util.{Failure, Success, Try}

// Events used internally
case class RunActivation(action: ExecutableWhiskAction, msg: ActivationMessage)
case class RunActivationCompleted(container: Container, action: ExecutableWhiskAction, duration: Option[Long])
case class InitCodeCompleted(data: WarmData)

// Events received by the actor
case class Initialize(invocationNamespace: String,
                      fqn: FullyQualifiedEntityName,
                      action: ExecutableWhiskAction,
                      schedulerHost: String,
                      rpcPort: Int,
                      transId: TransactionId)
case class Start(exec: CodeExec[_], memoryLimit: ByteSize, ttl: Option[FiniteDuration] = None)

// Event sent by the actor
case class ContainerCreationFailed(throwable: Throwable)
case class ContainerIsPaused(data: WarmData)
case class ClientCreationFailed(throwable: Throwable,
                                container: Container,
                                invocationNamespace: String,
                                action: ExecutableWhiskAction)
case class ReadyToWork(data: PreWarmData)
case class Initialized(data: InitializedData)
case class Resumed(data: WarmData)
case class ResumeFailed(data: WarmData)
case class RecreateClient(action: ExecutableWhiskAction)
case object PingCache
case class DetermineKeepContainer(attempt: Int)

// States
sealed trait ProxyState
case object LeaseStart extends ProxyState
case object Uninitialized extends ProxyState
case object CreatingContainer extends ProxyState
case object ContainerCreated extends ProxyState
case object CreatingClient extends ProxyState
case object ClientCreated extends ProxyState
case object Running extends ProxyState
case object Pausing extends ProxyState
case object Paused extends ProxyState
case object Removing extends ProxyState
case object Rescheduling extends ProxyState

// Errors
case class ContainerHealthErrorWithResumedRun(tid: TransactionId, msg: String, resumeRun: RunActivation)
    extends Exception(msg)

// Data
sealed abstract class Data(val memoryLimit: ByteSize) {
  def getContainer: Option[Container]
}
case class NonexistentData() extends Data(0.B) {
  override def getContainer = None
}
case class MemoryData(override val memoryLimit: ByteSize) extends Data(memoryLimit) {
  override def getContainer = None
}
trait WithClient { val clientProxy: ActorRef }
case class PreWarmData(container: Container,
                       kind: String,
                       override val memoryLimit: ByteSize,
                       expires: Option[Deadline] = None)
    extends Data(memoryLimit) {
  override def getContainer = Some(container)
  def isExpired(): Boolean = expires.exists(_.isOverdue())
}

object BasicContainerInfo extends DefaultJsonProtocol {
  implicit val prewarmedPoolSerdes = jsonFormat4(BasicContainerInfo.apply)
}

sealed case class BasicContainerInfo(containerId: String, namespace: String, action: String, kind: String)

sealed abstract class ContainerAvailableData(container: Container,
                                             invocationNamespace: String,
                                             action: ExecutableWhiskAction)
    extends Data(action.limits.memory.megabytes.MB) {
  override def getContainer = Some(container)

  val basicContainerInfo =
    BasicContainerInfo(container.containerId.asString, invocationNamespace, action.name.asString, action.exec.kind)
}

case class ContainerCreatedData(container: Container, invocationNamespace: String, action: ExecutableWhiskAction)
    extends ContainerAvailableData(container, invocationNamespace, action)

case class InitializedData(container: Container,
                           invocationNamespace: String,
                           action: ExecutableWhiskAction,
                           override val clientProxy: ActorRef)
    extends ContainerAvailableData(container, invocationNamespace, action)
    with WithClient {
  override def getContainer = Some(container)
  def toReschedulingData(resumeRun: RunActivation) =
    ReschedulingData(container, invocationNamespace, action, clientProxy, resumeRun)
}

case class WarmData(container: Container,
                    invocationNamespace: String,
                    action: ExecutableWhiskAction,
                    revision: DocRevision,
                    lastUsed: Instant,
                    override val clientProxy: ActorRef)
    extends ContainerAvailableData(container, invocationNamespace, action)
    with WithClient {
  override def getContainer = Some(container)
  def toReschedulingData(resumeRun: RunActivation) =
    ReschedulingData(container, invocationNamespace, action, clientProxy, resumeRun)
}

case class ReschedulingData(container: Container,
                            invocationNamespace: String,
                            action: ExecutableWhiskAction,
                            clientProxy: ActorRef,
                            resumeRun: RunActivation)
    extends ContainerAvailableData(container, invocationNamespace, action)
    with WithClient

class FunctionPullingContainerProxy(
  factory: (TransactionId,
            String,
            ImageName,
            Boolean,
            ByteSize,
            Int,
            Option[Double],
            Option[ExecutableWhiskAction]) => Future[Container],
  entityStore: ArtifactStore[WhiskEntity],
  namespaceBlacklist: NamespaceBlacklist,
  get: (ArtifactStore[WhiskEntity], DocId, DocRevision, Boolean, Boolean) => Future[WhiskAction],
  dataManagementService: ActorRef,
  clientProxyFactory: (ActorRefFactory,
                       String,
                       FullyQualifiedEntityName,
                       DocRevision,
                       String,
                       Int,
                       ContainerId) => ActorRef,
  sendActiveAck: ActiveAck,
  storeActivation: (TransactionId, WhiskActivation, Boolean, UserContext) => Future[Any],
  collectLogs: LogsCollector,
  getLiveContainerCount: (String, FullyQualifiedEntityName, DocRevision) => Future[Long],
  getWarmedContainerLimit: (String) => Future[(Int, FiniteDuration)],
  instance: InvokerInstanceId,
  invokerHealthManager: ActorRef,
  poolConfig: ContainerPoolConfig,
  timeoutConfig: ContainerProxyTimeoutConfig,
  healtCheckConfig: ContainerProxyHealthCheckConfig,
  testTcp: Option[ActorRef])(implicit actorSystem: ActorSystem, logging: Logging)
    extends FSM[ProxyState, Data]
    with Stash {
  startWith(Uninitialized, NonexistentData())

  implicit val ec = actorSystem.dispatcher

  private val UnusedTimeoutName = "UnusedTimeout"
  private val unusedTimeout = timeoutConfig.pauseGrace
  // private val unusedTimeout = 1.seconds
  private val IdleTimeoutName = "PausingTimeout"
  private val idleTimeout = timeoutConfig.idleContainer
  private val KeepingTimeoutName = "KeepingTimeout"
  private val RunningActivationTimeoutName = "RunningActivationTimeout"
  private val runningActivationTimeout = 10.seconds
  private val PingCacheName = "PingCache"
  private val pingCacheInterval = 1.minute
  private var timedOut = false

  var healthPingActor: Option[ActorRef] = None //setup after prewarm starts
  val tcp: ActorRef = testTcp.getOrElse(IO(Tcp)) //allows to testing interaction with Tcp extension

  val runningActivations = new java.util.concurrent.ConcurrentHashMap[String, Boolean]

  
  import java.nio.file.{Files, Paths}
  import java.nio.charset.StandardCharsets
  import scala.sys.process._

  val scheduler0IPcommand = Seq("sh", "-c", s"docker inspect -f '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}' scheduler0")
  val scheduler0IP = scheduler0IPcommand.!!.trim // 执行命令
  val invoker0IPcommand = Seq("sh", "-c", s"docker inspect -f '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}' invoker0")
  val invoker0IP = invoker0IPcommand.!!.trim // 执行命令

  val jsonContent = s"""{"scheduler0IP": "$scheduler0IP", "invoker0IP": "$invoker0IP"}"""
  val path = Paths.get("/db/dockerIP.json")
  Files.write(path, jsonContent.getBytes(StandardCharsets.UTF_8))

  when(Uninitialized) {
    // pre warm a container (creates a stem cell container)
    case Event(job: Start, _) =>
      factory(
        TransactionId.invokerWarmup,
        containerName(instance, "prewarm", job.exec.kind),
        job.exec.image,
        job.exec.pull,
        job.memoryLimit,
        poolConfig.cpuShare(job.memoryLimit),
        poolConfig.cpuLimit(job.memoryLimit),
        None)
        .map(container => PreWarmData(container, job.exec.kind, job.memoryLimit, expires = job.ttl.map(_.fromNow)))
        .pipeTo(self)
      goto(CreatingContainer)

    // cold start
    case Event(job: Initialize, _) =>
      // 在 Uninitialized 状态下收到 Initialize 消息时，表示需要为任务创建一个新的容器（冷启动）
      factory( // create a new container
        TransactionId.invokerColdstart,
        containerName(instance, job.action.namespace.namespace, job.action.name.asString),
        job.action.exec.image,
        job.action.exec.pull,
        job.action.limits.memory.megabytes.MB,
        poolConfig.cpuShare(job.action.limits.memory.megabytes.MB),
        poolConfig.cpuLimit(job.action.limits.memory.megabytes.MB),
        None)
        .andThen {
          case Failure(t) =>
            context.parent ! ContainerCreationFailed(t)
        }
        .map { container =>
          logging.debug(this, s"a container ${container.containerId} is created for ${job.action}")
          // create a client
          // 创建一个客户端代理
          Try(
            clientProxyFactory(
              context,
              job.invocationNamespace,
              job.fqn, // include binding field
              job.action.rev,
              job.schedulerHost,
              job.rpcPort,
              container.containerId)) match {
            case Success(clientProxy) =>
              InitializedData(container, job.invocationNamespace, job.action, clientProxy)
            case Failure(t) =>
              logging.error(this, s"failed to create activation client caused by: $t")
              ClientCreationFailed(t, container, job.invocationNamespace, job.action)
          }
        }
        // 结果发送到自身
        .pipeTo(self)
      // 状态转移到 CreatingClient，等待客户端代理的创建
      goto(CreatingClient)

    case _ => delay
  }

  when(CreatingContainer) {
    // container was successfully obtained
    case Event(completed: PreWarmData, _: NonexistentData) =>
      context.parent ! ReadyToWork(completed)
      goto(ContainerCreated) using completed

    // container creation failed
    case Event(t: FailureMessage, _: NonexistentData) =>
      context.parent ! ContainerRemoved(true)
      stop()

    case _ => delay
  }

  // prewarmed state, container created
  when(ContainerCreated) {
    case Event(job: Initialize, data: PreWarmData) =>
      val res = Try(
        clientProxyFactory(
          context,
          job.invocationNamespace,
          job.fqn, // include binding field
          job.action.rev,
          job.schedulerHost,
          job.rpcPort,
          data.container.containerId)) match {
        case Success(proxy) =>
          InitializedData(data.container, job.invocationNamespace, job.action, proxy)
        case Failure(t) =>
          logging.error(this, s"failed to create activation client for ${job.action} caused by: $t")
          ClientCreationFailed(t, data.container, job.invocationNamespace, job.action)
      }

      self ! res

      goto(CreatingClient)

    case Event(Remove, data: PreWarmData) =>
      cleanUp(data.container, None, false)

    // prewarm container failed by health check
    case Event(_: FailureMessage, data: PreWarmData) =>
      MetricEmitter.emitCounterMetric(LoggingMarkers.INVOKER_CONTAINER_HEALTH_FAILED_PREWARM)
      cleanUp(data.container, None)

    case _ => delay
  }

  when(CreatingClient) {
    // wait for client creation when cold start
    case Event(job: InitializedData, _) =>
      job.clientProxy ! StartClient

      stay() using job

    // client was successfully obtained
    // 客户端代理 clientProxy 已就绪
    case Event(ClientCreationCompleted, data: InitializedData) =>
      val fqn = data.action.fullyQualifiedName(true)
      val revision = data.action.rev

      // 注册容器信息
      dataManagementService ! RegisterData(
        s"${ContainerKeys.existingContainers(data.invocationNamespace, fqn, revision, Some(instance), Some(data.container.containerId))}",
        "")
      // 将当前数据 (data) 发送给自身
      self ! data

      logging.info(this, s"创建完成, ActorRef: $self")
      // 状态转移到 ClientCreated
      goto(ClientCreated)

    // client creation failed
    case Event(t: ClientCreationFailed, _) =>
      invokerHealthManager ! HealthMessage(state = false)
      cleanUp(t.container, t.invocationNamespace, t.action.fullyQualifiedName(withVersion = true), t.action.rev, None)

    case Event(ClientClosed, data: InitializedData) =>
      invokerHealthManager ! HealthMessage(state = false)
      cleanUp(
        data.container,
        data.invocationNamespace,
        data.action.fullyQualifiedName(withVersion = true),
        data.action.rev,
        None)

    // container creation failed when cold start
    case Event(_: FailureMessage, _) =>
      context.parent ! ContainerRemoved(true)
      stop()

    case _ => delay
  }

  // this is for first invocation, once the first invocation is over we are ready to trigger getActivation for action concurrency
  when(ClientCreated) {
    // 1. request activation message to client
    case Event(initializedData: InitializedData, _) =>
      // 通知父代理，该容器已经初始化完成。
      context.parent ! Initialized(initializedData)
      // 通过 clientProxy 发送 RequestActivation() 消息，请求下一个任务激活消息。
      initializedData.clientProxy ! RequestActivation()
      // 启动定时器
      startTimerWithFixedDelay(PingCacheName, PingCache, pingCacheInterval)
      startSingleTimer(UnusedTimeoutName, StateTimeout, unusedTimeout)
      stay() using initializedData

    // 2. read executable action data from db
    case Event(job: ActivationMessage, data: InitializedData) =>
      // 取消超时标记
      timedOut = false
      cancelTimer(UnusedTimeoutName)

      // 调用 handleActivationMessage 解析和准备任务激活消息（如读取任务代码），
      // 并将结果发送给自身（self）。这一步通常会生成 RunActivation 消息，标识激活任务已准备好，可以开始执行。
      handleActivationMessage(job, data.action)
        .pipeTo(self)
      stay() using data

    // 3. request initialize and run command to container
    case Event(job: RunActivation, data: InitializedData) =>
      implicit val transid = job.msg.transid
      logging.debug(this, s"received RunActivation ${job.msg.activationId} for ${job.action} in $stateName")

      // 调用 initializeAndRunActivation 方法在容器中初始化并运行激活任务
      initializeAndRunActivation(data.container, data.clientProxy, job.action, job.msg, Some(job))
        .map { activation =>
          // 执行完成后返回 RunActivationCompleted，标记任务执行结束
          RunActivationCompleted(data.container, job.action, activation.duration)
        }
        .pipeTo(self)

      // when it receives InitCodeCompleted, it will move to Running
      stay using data

    case Event(RetryRequestActivation, data: InitializedData) =>
      // if this Container is marked with time out, do not retry
      if (timedOut)
        cleanUp(
          data.container,
          data.invocationNamespace,
          data.action.fullyQualifiedName(withVersion = true),
          data.action.rev,
          Some(data.clientProxy))
      else {
        data.clientProxy ! RequestActivation()
        stay()
      }

    // code initialization was successful
    case Event(completed: InitCodeCompleted, data: InitializedData) =>
      // TODO support concurrency?
      data.clientProxy ! ContainerWarmed // this container is warmed
      1 until completed.data.action.limits.concurrency.maxConcurrent foreach { _ =>
        data.clientProxy ! RequestActivation()
      }

      goto(Running) using completed.data // set warm data

    // ContainerHealthError should cause
    case Event(FailureMessage(e: ContainerHealthErrorWithResumedRun), data: InitializedData) =>
      logging.error(
        this,
        s"container ${data.container.containerId.asString} health check failed on $stateName, ${e.resumeRun.msg.activationId} activation will be rescheduled")
      MetricEmitter.emitCounterMetric(LoggingMarkers.INVOKER_CONTAINER_HEALTH_FAILED_WARM)

      // reschedule message
      data.clientProxy ! RescheduleActivation(
        data.invocationNamespace,
        data.action.fullyQualifiedName(withVersion = true),
        data.action.rev,
        e.resumeRun.msg)

      goto(Rescheduling) using data.toReschedulingData(e.resumeRun)

    // Failed to get activation or execute the action
    case Event(t: FailureMessage, data: InitializedData) =>
      logging.error(
        this,
        s"failed to initialize a container or run an activation for ${data.action} in state: $stateName caused by: $t")
      // Stop containerProxy and ActivationClientProxy both immediately
      cleanUp(
        data.container,
        data.invocationNamespace,
        data.action.fullyQualifiedName(withVersion = true),
        data.action.rev,
        Some(data.clientProxy))

    case Event(StateTimeout, data: InitializedData) =>
      logging.info(this, s"No more activation is coming in state: $stateName, action: ${data.action}")
      // Just mark the ContainerProxy is timedout
      timedOut = true

      stay() // stay here because the ActivationClientProxy may send a new Activation message

    case Event(ClientClosed, data: InitializedData) =>
      logging.error(this, s"The Client closed in state: $stateName, action: ${data.action}")
      // Stop ContainerProxy(ActivationClientProxy will stop also when send ClientClosed to ContainerProxy).
      cleanUp(
        data.container,
        data.invocationNamespace,
        data.action.fullyQualifiedName(withVersion = true),
        data.action.rev,
        None)

    case x: Event if x.event != PingCache => delay
  }

  when(Rescheduling, stateTimeout = 10.seconds) {

    case Event(res: RescheduleResponse, data: ReschedulingData) =>
      implicit val transId = data.resumeRun.msg.transid
      if (!res.isRescheduled) {
        logging.warn(this, s"failed to reschedule the message ${data.resumeRun.msg.activationId}, clean up data")
        fallbackActivationForReschedulingData(data)
      } else {
        logging.warn(this, s"unhandled message is rescheduled, clean up data")
      }
      cleanUp(
        data.container,
        data.invocationNamespace,
        data.action.fullyQualifiedName(withVersion = true),
        data.action.rev,
        Some(data.clientProxy))

    case Event(StateTimeout, data: ReschedulingData) =>
      logging.error(this, s"Timeout for rescheduling message ${data.resumeRun.msg.activationId}, clean up data")(
        data.resumeRun.msg.transid)

      fallbackActivationForReschedulingData(data)
      cleanUp(
        data.container,
        data.invocationNamespace,
        data.action.fullyQualifiedName(withVersion = true),
        data.action.rev,
        Some(data.clientProxy))

    case x: Event if x.event != PingCache => delay
  }

  when(Running) {
    // Run was successful.
    // 1. request activation message to client
    case Event(activationResult: RunActivationCompleted, data: WarmData) =>
      // create timeout
      // 设置一个单次的 UnusedTimeout 定时器，用于检测是否在指定的 unusedTimeout 时间内收到下一个激活请求
      // 如果没有激活请求，超时计时器会触发 StateTimeout 事件
      startSingleTimer(UnusedTimeoutName, StateTimeout, unusedTimeout)

      // 发送 RequestActivation 消息到 data.clientProxy，
      // 其中附带了上一个任务的执行时长，通知 clientProxy 容器已准备好接收下一个激活请求。
      data.clientProxy ! RequestActivation(activationResult.duration)
      stay() using data

    // 2. read executable action data from db
    case Event(job: ActivationMessage, data: WarmData) =>
      timedOut = false
      cancelTimer(UnusedTimeoutName)
      // 将 handleActivationMessage 的执行结果（Future[RunActivation]）传递给自身（self）
      handleActivationMessage(job, data.action)
        .pipeTo(self)
      stay() using data

    // 3. request run command to container
    case Event(job: RunActivation, data: WarmData) =>
      logging.debug(this, s"received RunActivation ${job.msg.activationId} for ${job.action} in $stateName")
      implicit val transid = job.msg.transid

      // 调用 initializeAndRunActivation 方法在容器中初始化并运行激活任务
      initializeAndRunActivation(data.container, data.clientProxy, job.action, job.msg, Some(job))
        .map { activation =>
          // 执行完成后返回 RunActivationCompleted，标记任务执行结束
          RunActivationCompleted(data.container, job.action, activation.duration)
        }
        .pipeTo(self)
      stay using data.copy(lastUsed = Instant.now)

    case Event(RetryRequestActivation, data: WarmData) =>
      // if this Container is marked with time out, do not retry
      if (timedOut) {
        // 如果 timedOut 为 true，表示容器已经超时，因此选择暂停容器，而不是继续请求新的激活任务。
        data.container.suspend()(TransactionId.invokerNanny).map(_ => ContainerPaused).pipeTo(self)
        goto(Pausing)
      } else {
        // 如果容器仍然处于活跃状态，因此可以继续向 clientProxy 请求新的激活任务。
        data.clientProxy ! RequestActivation()
        stay()
      }

    case Event(_: ResumeFailed, data: WarmData) =>
      invokerHealthManager ! HealthMessage(state = false)
      cleanUp(
        data.container,
        data.invocationNamespace,
        data.action.fullyQualifiedName(withVersion = true),
        data.action.rev,
        Some(data.clientProxy))

    // ContainerHealthError should cause
    case Event(FailureMessage(e: ContainerHealthError), data: WarmData) =>
      logging.error(this, s"health check failed on $stateName caused by: ContainerHealthError $e")
      MetricEmitter.emitCounterMetric(LoggingMarkers.INVOKER_CONTAINER_HEALTH_FAILED_WARM)
      // Stop containerProxy and ActivationClientProxy both immediately,
      invokerHealthManager ! HealthMessage(state = false)
      cleanUp(
        data.container,
        data.invocationNamespace,
        data.action.fullyQualifiedName(withVersion = true),
        data.action.rev,
        Some(data.clientProxy))

    // ContainerHealthError should cause
    case Event(FailureMessage(e: ContainerHealthErrorWithResumedRun), data: WarmData) =>
      logging.error(
        this,
        s"container ${data.container.containerId.asString} health check failed on $stateName, ${e.resumeRun.msg.activationId} activation will be rescheduled")
      MetricEmitter.emitCounterMetric(LoggingMarkers.INVOKER_CONTAINER_HEALTH_FAILED_WARM)

      // reschedule message
      data.clientProxy ! RescheduleActivation(
        data.invocationNamespace,
        data.action.fullyQualifiedName(withVersion = true),
        data.action.rev,
        e.resumeRun.msg)

      goto(Rescheduling) using data.toReschedulingData(e.resumeRun)

    // Failed to get activation or execute the action
    case Event(t: FailureMessage, data: WarmData) =>
      logging.error(this, s"failed to init or run in state: $stateName caused by: $t")
      // Stop containerProxy and ActivationClientProxy both immediately,
      // and don't send unhealthy state message to the health manager, it's already sent.
      cleanUp(
        data.container,
        data.invocationNamespace,
        data.action.fullyQualifiedName(withVersion = true),
        data.action.rev,
        Some(data.clientProxy))

    case Event(StateTimeout, data: WarmData) =>
      logging.info(
        this,
        s"No more run activation is coming in state: $stateName, action: ${data.action}, container: ${data.container.containerId}")
      // Just mark the ContainerProxy is timedout
      // 标志本容器超时
      timedOut = true

      // 尽管容器超时，但代码并未进行状态转换，而是通过 stay() 保持在 Running 状态。原因是系统可能会稍后收到一个新的激活请求
      stay() // stay here because the ActivationClientProxy may send a new Activation message
      // goto(Pausing)

    case Event(ClientClosed, data: WarmData) =>
      if (runningActivations.isEmpty) {
        logging.info(this, s"The Client closed in state: $stateName, action: ${data.action}")
        // Stop ContainerProxy(ActivationClientProxy will stop also when send ClientClosed to ContainerProxy).
        cleanUp(
          data.container,
          data.invocationNamespace,
          data.action.fullyQualifiedName(withVersion = true),
          data.action.rev,
          None)
      } else {
        logging.info(
          this,
          s"Remain running activations ${runningActivations.keySet().toString()} when received ClientClosed")
        startSingleTimer(RunningActivationTimeoutName, ClientClosed, runningActivationTimeout)
        stay
      }

    // shutdown the client first and wait for any remaining activation to be executed
    // ContainerProxy will be terminated by StateTimeout if there is no further activation
    case Event(GracefulShutdown, data: WarmData) =>
      logging.info(this, s"receive GracefulShutdown for action: ${data.action}")
      // clean up the etcd data first so that the scheduler can provision more containers in advance.
      dataManagementService ! UnregisterData(
        ContainerKeys.existingContainers(
          data.invocationNamespace,
          data.action.fullyQualifiedName(true),
          data.action.rev,
          Some(instance),
          Some(data.container.containerId)))

      // Just send GracefulShutdown to ActivationClientProxy, make ActivationClientProxy throw ClientClosedException when fetchActivation next time.
      data.clientProxy ! GracefulShutdown
      stay

    case x: Event if x.event != PingCache => delay
  }

  when(Pausing) {
    case Event(ContainerPaused, data: WarmData) =>
      logging.info(this, s"当前状态: Pausing, 收到ContainerPaused消息, 进入处理流程")
      // 将当前容器的状态注册到 dataManagementService 中，以标记该容器已暂停。
      // 这使得容器的状态信息可以被系统的其他部分查询到，例如任务调度模块可以判断哪些容器处于暂停状态，可以在需要时重新唤醒。
      dataManagementService ! RegisterData(
        ContainerKeys.warmedContainers(
          data.invocationNamespace,
          data.action.fullyQualifiedName(false),
          data.revision,
          instance,
          data.container.containerId),
        "")
      // remove existing key so MemoryQueue can be terminated when timeout
      // 将容器的现有注册信息（existingContainers 键）从 dataManagementService 中移除
      dataManagementService ! UnregisterData(
        s"${ContainerKeys.existingContainers(data.invocationNamespace, data.action.fullyQualifiedName(true), data.action.rev, Some(instance), Some(data.container.containerId))}")
      // 发送 ContainerIsPaused 消息给父级（通常是 ContainerProxy），告知容器已成功进入暂停状态。
      context.parent ! ContainerIsPaused(data)
      logging.info(this, s"ContainerPaused消息处理完毕, 进入Paused状态")
      goto(Paused)

    case Event(_: FailureMessage, data: WarmData) =>
      cleanUp(
        data.container,
        data.invocationNamespace,
        data.action.fullyQualifiedName(false),
        data.action.rev,
        Some(data.clientProxy))

    case x: Event if x.event != PingCache => delay
  }

  when(Paused) {
    case Event(job: Initialize, data: WarmData) =>
      logging.info(this, s"当前状态: Paused, 收到Initialize消息, 进入处理流程")
      implicit val transId = job.transId
      val parent = context.parent
      cancelTimer(IdleTimeoutName)
      cancelTimer(KeepingTimeoutName)
      cancelTimer(DetermineKeepContainer.toString)
      data.container
        .resume()
        .map { _ =>
          logging.info(this, s"Resumed container ${data.container.containerId}")
          // put existing key again
          dataManagementService ! RegisterData(
            s"${ContainerKeys.existingContainers(data.invocationNamespace, data.action.fullyQualifiedName(true), data.action.rev, Some(instance), Some(data.container.containerId))}",
            "")
          parent ! Resumed(data)
          // the new queue may locates on an different scheduler, so recreate the activation client when necessary
          // since akka port will no be used, we can put any value except 0 here
          data.clientProxy ! RequestActivation(
            newScheduler = Some(SchedulerEndpoints(job.schedulerHost, job.rpcPort, 10)))
          startSingleTimer(UnusedTimeoutName, StateTimeout, unusedTimeout)
          timedOut = false
        }
        .recover {
          case t: Throwable =>
            logging.error(this, s"Failed to resume container ${data.container.containerId}, error: $t")
            parent ! ResumeFailed(data)
            self ! ResumeFailed(data)
        }

      // always clean data in etcd regardless of success and failure
      dataManagementService ! UnregisterData(
        ContainerKeys.warmedContainers(
          data.invocationNamespace,
          data.action.fullyQualifiedName(false),
          data.revision,
          instance,
          data.container.containerId))
      goto(Running)
    case Event(StateTimeout, _: WarmData) =>
      self ! DetermineKeepContainer(0)
      stay
    case Event(DetermineKeepContainer(attempt), data: WarmData) =>
      getLiveContainerCount(data.invocationNamespace, data.action.fullyQualifiedName(false), data.revision)
        .flatMap(count => {
          getWarmedContainerLimit(data.invocationNamespace).map(warmedContainerInfo => {
            logging.info(
              this,
              s"Live container count: $count, warmed container keeping count configuration: ${warmedContainerInfo._1} in namespace: ${data.invocationNamespace}")
            if (count <= warmedContainerInfo._1) {
              self ! Keep(warmedContainerInfo._2)
            } else {
              self ! Remove
            }
          })
        })
        .recover({
          case t: Throwable =>
            logging.error(
              this,
              s"Failed to determine whether to keep or remove container on pause timeout for ${data.container.containerId}, retrying. Caused by: $t")
            if (attempt < 5) {
              startSingleTimer(DetermineKeepContainer.toString, DetermineKeepContainer(attempt + 1), 500.milli)
            } else {
              self ! Remove
            }
        })
      stay
    case Event(Keep(warmedContainerKeepingTimeout), data: WarmData) =>
      logging.info(
        this,
        s"This is the remaining container for ${data.action}. The container will stop after $warmedContainerKeepingTimeout.")
      startSingleTimer(KeepingTimeoutName, Remove, warmedContainerKeepingTimeout)
      stay
    case Event(Remove | GracefulShutdown, data: WarmData) =>
      cancelTimer(DetermineKeepContainer.toString)
      dataManagementService ! UnregisterData(
        ContainerKeys.warmedContainers(
          data.invocationNamespace,
          data.action.fullyQualifiedName(false),
          data.revision,
          instance,
          data.container.containerId))
      cleanUp(
        data.container,
        data.invocationNamespace,
        data.action.fullyQualifiedName(false),
        data.action.rev,
        Some(data.clientProxy))

    case x: Event if x.event != PingCache => delay
  }

  when(Removing, unusedTimeout) {
    // only if ClientProxy is closed, ContainerProxy stops. So it is important for ClientProxy to send ClientClosed.
    case Event(ClientClosed, _) =>
      stop()

    // even if any error occurs, it still waits for ClientClosed event in order to be stopped after the client is closed.
    case Event(t: FailureMessage, _) =>
      logging.error(this, s"unable to delete a container due to ${t}")

      stay

    case Event(StateTimeout, _) =>
      logging.warn(this, s"could not receive ClientClosed for ${unusedTimeout}, so just stop the container proxy.")

      stop()

    case Event(Remove | GracefulShutdown, _) =>
      stay()

    case Event(DetermineKeepContainer(_), _) =>
      stay()
  }

  whenUnhandled {
    case Event(PingCache, data: WarmData) =>
      val actionId = data.action.fullyQualifiedName(false).toDocId.asDocInfo(data.revision)
      get(entityStore, actionId.id, actionId.rev, true, false).map(_ => {
        logging.debug(
          this,
          s"Refreshed function cache for action ${data.action} from container ${data.container.containerId}.")
      })
      stay
    case Event(PingCache, _) =>
      logging.debug(this, "Container is not warm, ignore function cache ping.")
      stay
  }

  onTransition {
    case _ -> Uninitialized     => unstashAll()
    case _ -> CreatingContainer => unstashAll()
    case _ -> ContainerCreated =>
      if (healtCheckConfig.enabled) {
        nextStateData.getContainer.foreach { c =>
          logging.info(this, s"enabling health ping for ${c.containerId.asString} on ContainerCreated")
          enableHealthPing(c)
        }
      }
      unstashAll()
    case _ -> CreatingClient => unstashAll()
    case _ -> ClientCreated  => unstashAll()
    case _ -> Running =>
      if (healtCheckConfig.enabled && healthPingActor.isDefined) {
        nextStateData.getContainer.foreach { c =>
          logging.info(this, s"disabling health ping for ${c.containerId.asString} on Running")
          disableHealthPing()
        }
      }
      unstashAll()
    case _ -> Paused   => startSingleTimer(IdleTimeoutName, StateTimeout, idleTimeout)
    case _ -> Removing => unstashAll()
  }

  initialize()

  /** Delays all incoming messages until unstashAll() is called */
  def delay = {
    stash()
    stay
  }

  /**
   * Only change the state if the currentState is not the newState.
   *
   * @param newState of the InvokerActor
   */
  private def gotoIfNotThere(newState: ProxyState) = {
    if (stateName == newState) stay() else goto(newState)
  }

  /**
   * Clean up all meta data of invoking action
   *
   * @param container the container to destroy
   * @param fqn the action to stop
   * @param clientProxy the client to destroy
   * @return
   */
  private def cleanUp(container: Container,
                      invocationNamespace: String,
                      fqn: FullyQualifiedEntityName,
                      revision: DocRevision,
                      clientProxy: Option[ActorRef]): State = {
    cancelTimer(PingCacheName)
    dataManagementService ! UnregisterData(
      s"${ContainerKeys.existingContainers(invocationNamespace, fqn, revision, Some(instance), Some(container.containerId))}")

    cleanUp(container, clientProxy)
  }

  private def cleanUp(container: Container, clientProxy: Option[ActorRef], replacePrewarm: Boolean = true): State = {
    context.parent ! ContainerRemoved(replacePrewarm)
    val unpause = stateName match {
      case Paused => container.resume()(TransactionId.invokerNanny)
      case _      => Future.successful(())
    }
    unpause.andThen {
      case Success(_) => destroyContainer(container)
      case Failure(t) =>
        // docker may hang when try to remove a paused container, so we shouldn't remove it
        logging.error(this, s"Failed to resume container ${container.containerId}, error: $t")
    }
    clientProxy match {
      case Some(clientProxy) => clientProxy ! StopClientProxy
      case None              => self ! ClientClosed
    }
    logging.info(this, "going to stay: Removing")
    gotoIfNotThere(Removing)
  }

  /**
   * Destroys the container
   *
   * @param container the container to destroy
   */
  private def destroyContainer(container: Container) = {
    container
      .destroy()(TransactionId.invokerNanny)
      .andThen {
        case Failure(t) =>
          logging.error(this, s"Failed to destroy container: ${container.containerId.asString} caused by ${t}")
      }
  }

  // 从数据库中读取action代码
  private def handleActivationMessage(msg: ActivationMessage, action: ExecutableWhiskAction): Future[RunActivation] = {
    implicit val transid = msg.transid
    logging.info(this, s"received a message ${msg.activationId} for ${msg.action} in $stateName")

    // 检查消息中的 user 是否在命名空间黑名单中
    if (!namespaceBlacklist.isBlacklisted(msg.user)) {
      logging.debug(this, s"namespace ${msg.user.namespace.name} is not in the namespaceBlacklist")

      // 获取动作的actionid
      val namespace = msg.action.path
      val name = msg.action.name
      val actionid = FullyQualifiedEntityName(namespace, name).toDocId.asDocInfo(msg.revision)
      val subject = msg.user.subject

      logging.debug(this, s"${actionid.id} $subject ${msg.activationId}")

      // set trace context to continue tracing
      // 设置 WhiskTracerProvider 的追踪上下文，以便于后续的日志追踪
      WhiskTracerProvider.tracer.setTraceContext(transid, msg.traceContext)

      // caching is enabled since actions have revision id and an updated
      // action will not hit in the cache due to change in the revision id;
      // if the doc revision is missing, then bypass cache
      // 读取数据库中的action
      if (actionid.rev == DocRevision.empty)
        logging.warn(this, s"revision was not provided for ${actionid.id}")

      get(entityStore, actionid.id, actionid.rev, actionid.rev != DocRevision.empty, false)
        .flatMap { action =>
          {
            // action that exceed the limit cannot be executed
            action.limits.checkLimits(msg.user)
            action.toExecutableWhiskAction match {
              case Some(executable) =>
                Future.successful(RunActivation(executable, msg))
              case None =>
                logging
                  .error(this, s"non-executable action reached the invoker ${action.fullyQualifiedName(false)}")
                Future.failed(new IllegalStateException("non-executable action reached the invoker"))
            }
          }
        }
        .recoverWith {
          case DocumentRevisionMismatchException(_) =>
            // if revision is mismatched, the action may have been updated,
            // so try again with the latest code
            logging.warn(
              this,
              s"msg ${msg.activationId} for ${msg.action} in $stateName is updated, fetching latest code")
            handleActivationMessage(msg.copy(revision = DocRevision.empty), action)
          case t =>
            // If the action cannot be found, the user has concurrently deleted it,
            // making this an application error. All other errors are considered system
            // errors and should cause the invoker to be considered unhealthy.
            val response = t match {
              case _: NoDocumentException =>
                ExecutionResponse.applicationError(Messages.actionRemovedWhileInvoking)
              case e: ActionLimitsException =>
                ExecutionResponse.applicationError(e.getMessage) // return generated failed message
              case _: DocumentTypeMismatchException | _: DocumentUnreadable =>
                ExecutionResponse.whiskError(Messages.actionMismatchWhileInvoking)
              case e: Throwable =>
                logging.error(this, s"An unknown DB connection error occurred while fetching an action: $e.")
                ExecutionResponse.whiskError(Messages.actionFetchErrorWhileInvoking)
            }
            val errMsg = s"Error to fetch action ${msg.action} for msg ${msg.activationId}, error is ${t.getMessage}"
            logging.error(this, errMsg)

            val context = UserContext(msg.user)
            val activation = generateFallbackActivation(action, msg, response)
            sendActiveAck(
              transid,
              activation,
              msg.blocking,
              msg.rootControllerIndex,
              msg.user.namespace.uuid,
              CombinedCompletionAndResultMessage(transid, activation, instance))
            storeActivation(msg.transid, activation, msg.blocking, context)

            // in case action is removed container proxy should be terminated
            Future.failed(new IllegalStateException(errMsg))
        }
    } else {
      // 黑名单内的处理
      // Iff the current namespace is blacklisted, an active-ack is only produced to keep the loadbalancer protocol
      // Due to the protective nature of the blacklist, a database entry is not written.
      val activation =
        generateFallbackActivation(action, msg, ExecutionResponse.applicationError(Messages.namespacesBlacklisted))
      sendActiveAck(
        msg.transid,
        activation,
        false,
        msg.rootControllerIndex,
        msg.user.namespace.uuid,
        CombinedCompletionAndResultMessage(msg.transid, activation, instance))
      logging.warn(
        this,
        s"namespace ${msg.user.namespace.name} was blocked in containerProxy, complete msg ${msg.activationId} with error.")
      Future.failed(new IllegalStateException(s"namespace ${msg.user.namespace.name} was blocked in containerProxy."))
    }

  }

  private def enableHealthPing(c: Container) = {
    val hpa = healthPingActor.getOrElse {
      logging.info(this, s"creating health ping actor for ${c.addr.asString()}")
      val hp = context.actorOf(
        TCPPingClient
          .props(tcp, c.toString(), healtCheckConfig, new InetSocketAddress(c.addr.host, c.addr.port)))
      healthPingActor = Some(hp)
      hp
    }
    hpa ! HealthPingEnabled(true)
  }

  private def disableHealthPing() = {
    healthPingActor.foreach(_ ! HealthPingEnabled(false))
  }

  def fallbackActivationForReschedulingData(data: ReschedulingData): Unit = {
    val context = UserContext(data.resumeRun.msg.user)
    val activation =
      generateFallbackActivation(data.action, data.resumeRun.msg, ExecutionResponse.whiskError(Messages.abnormalRun))

    sendActiveAck(
      data.resumeRun.msg.transid,
      activation,
      data.resumeRun.msg.blocking,
      data.resumeRun.msg.rootControllerIndex,
      data.resumeRun.msg.user.namespace.uuid,
      CombinedCompletionAndResultMessage(data.resumeRun.msg.transid, activation, instance))

    storeActivation(data.resumeRun.msg.transid, activation, data.resumeRun.msg.blocking, context)
  }

  /**
   * Runs the job, initialize first if necessary.
   * Completes the job by:
   * 1. sending an activate ack,
   * 2. fetching the logs for the run,
   * 3. indicating the resource is free to the parent pool,
   * 4. recording the result to the data store
   *
   * @param container the container to run the job on
   * @param job the job to run
   * @return a future completing after logs have been collected and
   *         added to the WhiskActivation
   */
  private def initializeAndRunActivation(
    container: Container,
    clientProxy: ActorRef,
    action: ExecutableWhiskAction,
    msg: ActivationMessage,
    resumeRun: Option[RunActivation] = None)(implicit tid: TransactionId): Future[WhiskActivation] = {

    logging.info(this, s"进入initializeAndRunActivation()函数, container: $container, activationId: ${msg.activationId.asString}")
    // Add the activation to runningActivations set
    // 将当前 activationId 添加到 runningActivations 集合中
    runningActivations.put(msg.activationId.asString, true)

    val actionTimeout = action.limits.timeout.duration

    //  分离成运行参数和初始化参数
    val (env, parameters) = ContainerProxy.partitionArguments(msg.content, msg.initArgs)

    // 准备环境变量
    val environment = Map(
      "namespace" -> msg.user.namespace.name.toJson,
      "action_name" -> msg.action.qualifiedNameWithLeadingSlash.toJson,
      "action_version" -> msg.action.version.toJson,
      "activation_id" -> msg.activationId.toString.toJson,
      "transaction_id" -> msg.transid.id.toJson)

    // if the action requests the api key to be injected into the action context, add it here;
    // treat a missing annotation as requesting the api key for backward compatibility
    // 处理密钥
    val authEnvironment = {
      if (action.annotations.isTruthy(Annotations.ProvideApiKeyAnnotationName, valueForNonExistent = true)) {
        msg.user.authkey.toEnvironment.fields
      } else Map.empty
    }

    // Only initialize iff we haven't yet warmed the container
    // 容器初始化（冷启动逻辑）
    val initialize = stateData match {
      case _: WarmData =>
        Future.successful(None)
      case _ =>
        // 若当前状态 stateData 不属于 WarmData，则认为容器尚未初始化，执行 container.initialize 来进行初始化操作。初始化时，注入了 authEnvironment 和 environment。
        val owEnv = (authEnvironment ++ environment ++ Map(
          "deadline" -> (Instant.now.toEpochMilli + actionTimeout.toMillis).toString.toJson)) map {
          case (key, value) => "__OW_" + key.toUpperCase -> value
        }
        container
          .initialize(action.containerInitializer(env ++ owEnv), actionTimeout, action.limits.concurrency.maxConcurrent)
          .map(Some(_))
    }

    // 初始化结束后，若返回成功，stateData 更新为 WarmData。
    val activation: Future[WhiskActivation] = initialize
      .flatMap { initInterval =>
        // immediately setup warmedData for use (before first execution) so that concurrent actions can use it asap
        if (initInterval.isDefined) {
          stateData match {
            case _: InitializedData =>
              self ! InitCodeCompleted(
                WarmData(container, msg.user.namespace.name.asString, action, msg.revision, Instant.now, clientProxy))

            case _ =>
              Future.failed(new IllegalStateException("lease does not exist"))
          }
        }
        // 准备用于执行操作的环境变量 env 和参数 parameters
        val env = authEnvironment ++ environment ++ Map(
          // compute deadline on invoker side avoids discrepancies inside container
          // but potentially under-estimates actual deadline
          "deadline" -> (Instant.now.toEpochMilli + actionTimeout.toMillis).toString.toJson)

        // 调用trait Container 中的 run 方法
        container
          .run(
            parameters,
            env.toJson.asJsObject,
            actionTimeout,
            action.limits.concurrency.maxConcurrent,
            msg.user.limits.allowedMaxPayloadSize,
            msg.user.limits.allowedTruncationSize,
            resumeRun.isDefined)(msg.transid)
          .map {
            case (runInterval, response) =>
              // run 方法返回执行的时间间隔 runInterval 和响应 response。
              val initRunInterval = initInterval
                .map(i => Interval(runInterval.start.minusMillis(i.duration.toMillis), runInterval.end))
                .getOrElse(runInterval)
              // 根据 response 构造 WhiskActivation 对象
              constructWhiskActivation(
                action,
                msg,
                initInterval,
                initRunInterval,
                runInterval.duration >= actionTimeout,
                response)
          }
      }
      .recoverWith {
        // 错误处理
        case h: ContainerHealthError if resumeRun.isDefined =>
          // health error occurs
          logging.error(this, s"caught healthchek check error while running activation")
          Future.failed(ContainerHealthErrorWithResumedRun(h.tid, h.msg, resumeRun.get))

        case InitializationError(interval, response) =>
          Future.successful(
            constructWhiskActivation(
              action,
              msg,
              Some(interval),
              interval,
              interval.duration >= actionTimeout,
              response))

        case t =>
          // Actually, this should never happen - but we want to make sure to not miss a problem
          logging.error(this, s"caught unexpected error while running activation: $t")
          Future.successful(
            constructWhiskActivation(
              action,
              msg,
              None,
              Interval.zero,
              false,
              ExecutionResponse.whiskError(Messages.abnormalRun)))
      }

    val splitAckMessagesPendingLogCollection = collectLogs.logsToBeCollected(action)
    // Sending an active ack is an asynchronous operation. The result is forwarded as soon as
    // possible for blocking activations so that dependent activations can be scheduled. The
    // completion message which frees a load balancer slot is sent after the active ack future
    // completes to ensure proper ordering.
    val sendResult = if (msg.blocking) {
      logging.info(this, "阻塞操作, 尽早发送ActiveAck")
      // 如果 msg 是阻塞的，则立即发送 ResultMessage 或 CombinedCompletionAndResultMessage 到控制器，以便依赖该操作结果的激活可以尽早调度。
      activation.map { result =>
        val ackMsg =
          if (splitAckMessagesPendingLogCollection) ResultMessage(tid, result)
          else CombinedCompletionAndResultMessage(tid, result, instance)
        sendActiveAck(tid, result, msg.blocking, msg.rootControllerIndex, msg.user.namespace.uuid, ackMsg)
      }
    } else {
      // For non-blocking request, do not forward the result.
      if (splitAckMessagesPendingLogCollection) Future.successful(())
      else
        activation.map { result =>
          val ackMsg = CompletionMessage(tid, result, instance)
          sendActiveAck(tid, result, msg.blocking, msg.rootControllerIndex, msg.user.namespace.uuid, ackMsg)
        }
    }

    // 如果激活响应无错误，则向 invokerHealthManager 发送 HealthMessage 表示容器运行正常。
    activation.foreach { activation =>
      val healthMessage = HealthMessage(!activation.response.isWhiskError)
      invokerHealthManager ! healthMessage
    }

    val context = UserContext(msg.user)

    // 日志采集
    // Adds logs to the raw activation.
    val activationWithLogs: Future[Either[ActivationLogReadingError, WhiskActivation]] = activation
      .flatMap { activation =>
        // Skips log collection entirely, if the limit is set to 0
        if (action.limits.logs.asMegaBytes == 0.MB) {
          Future.successful(Right(activation))
        } else {
          val start = tid.started(this, LoggingMarkers.INVOKER_COLLECT_LOGS, logLevel = InfoLevel)
          collectLogs(tid, msg.user, activation, container, action)
            .andThen {
              case Success(_) => tid.finished(this, start)
              case Failure(t) => tid.failed(this, start, s"reading logs failed: $t")
            }
            .map(logs => Right(activation.withLogs(logs)))
            .recover {
              case LogCollectingException(logs) =>
                Left(ActivationLogReadingError(activation.withLogs(logs)))
              case _ =>
                Left(ActivationLogReadingError(activation.withLogs(ActivationLogs(Vector(Messages.logFailure)))))
            }
        }
      }

    activationWithLogs
      .map(_.fold(_.activation, identity))
      .foreach { activation =>
        // Sending the completion message to the controller after the active ack ensures proper ordering
        // (result is received before the completion message for blocking invokes).
        if (splitAckMessagesPendingLogCollection) {
          sendResult.onComplete(
            _ =>
              sendActiveAck(
                tid,
                activation,
                msg.blocking,
                msg.rootControllerIndex,
                msg.user.namespace.uuid,
                CompletionMessage(tid, activation, instance)))
        }

        // Storing the record. Entirely asynchronous and not waited upon.
        // 将激活记录存储到数据库中
        storeActivation(tid, activation, msg.blocking, context)
      }

    // Disambiguate activation errors and transform the Either into a failed/successful Future respectively.
    activationWithLogs
      .andThen {
        // remove activationId from runningActivations in any case
        case _ => runningActivations.remove(msg.activationId.asString)
      }
      .flatMap {
        case Right(act) if !act.response.isSuccess && !act.response.isApplicationError =>
          Future.failed(ActivationUnsuccessfulError(act))
        case Left(error) => Future.failed(error)
        case Right(act)  => Future.successful(act)
      }
  }

  /** Generates an activation with zero runtime. Usually used for error cases */
  private def generateFallbackActivation(action: ExecutableWhiskAction,
                                         msg: ActivationMessage,
                                         response: ExecutionResponse): WhiskActivation = {
    val now = Instant.now
    val causedBy = if (msg.causedBySequence) {
      Some(Parameters(WhiskActivation.causedByAnnotation, JsString(Exec.SEQUENCE)))
    } else None

    WhiskActivation(
      activationId = msg.activationId,
      namespace = msg.user.namespace.name.toPath,
      subject = msg.user.subject,
      cause = msg.cause,
      name = msg.action.name,
      version = msg.action.version.getOrElse(SemVer()),
      start = now,
      end = now,
      duration = Some(0),
      response = response,
      annotations = {
        Parameters(WhiskActivation.pathAnnotation, JsString(msg.action.copy(version = None).asString)) ++
          Parameters(WhiskActivation.kindAnnotation, JsString(action.exec.kind)) ++
          causedBy
      })
  }

}

object FunctionPullingContainerProxy {

  def props(factory: (TransactionId,
                      String,
                      ImageName,
                      Boolean,
                      ByteSize,
                      Int,
                      Option[Double],
                      Option[ExecutableWhiskAction]) => Future[Container],
            entityStore: ArtifactStore[WhiskEntity],
            namespaceBlacklist: NamespaceBlacklist,
            get: (ArtifactStore[WhiskEntity], DocId, DocRevision, Boolean, Boolean) => Future[WhiskAction],
            dataManagementService: ActorRef,
            clientProxyFactory: (ActorRefFactory,
                                 String,
                                 FullyQualifiedEntityName,
                                 DocRevision,
                                 String,
                                 Int,
                                 ContainerId) => ActorRef,
            ack: ActiveAck,
            store: (TransactionId, WhiskActivation, Boolean, UserContext) => Future[Any],
            collectLogs: LogsCollector,
            getLiveContainerCount: (String, FullyQualifiedEntityName, DocRevision) => Future[Long],
            getWarmedContainerLimit: (String) => Future[(Int, FiniteDuration)],
            instance: InvokerInstanceId,
            invokerHealthManager: ActorRef,
            poolConfig: ContainerPoolConfig,
            timeoutConfig: ContainerProxyTimeoutConfig,
            healthCheckConfig: ContainerProxyHealthCheckConfig =
              loadConfigOrThrow[ContainerProxyHealthCheckConfig](ConfigKeys.containerProxyHealth),
            tcp: Option[ActorRef] = None)(implicit actorSystem: ActorSystem, logging: Logging) =
    Props(
      new FunctionPullingContainerProxy(
        factory,
        entityStore,
        namespaceBlacklist,
        get,
        dataManagementService,
        clientProxyFactory,
        ack,
        store,
        collectLogs,
        getLiveContainerCount,
        getWarmedContainerLimit,
        instance,
        invokerHealthManager,
        poolConfig,
        timeoutConfig,
        healthCheckConfig,
        tcp))

  private val containerCount = new Counter

  /**
   * Generates a unique container name.
   *
   * @param prefix the container name's prefix
   * @param suffix the container name's suffix
   * @return a unique container name
   */
  def containerName(instance: InvokerInstanceId, prefix: String, suffix: String): String = {
    def isAllowed(c: Char): Boolean = c.isLetterOrDigit || c == '_'

    val sanitizedPrefix = prefix.filter(isAllowed)
    val sanitizedSuffix = suffix.filter(isAllowed)

    s"${ContainerFactory.containerNamePrefix(instance)}_${containerCount.next()}_${sanitizedPrefix}_${sanitizedSuffix}"
  }

  /**
   * Creates a WhiskActivation ready to be sent via active ack.
   *
   * @param job the job that was executed
   * @param interval the time it took to execute the job
   * @param response the response to return to the user
   * @return a WhiskActivation to be sent to the user
   */
  def constructWhiskActivation(action: ExecutableWhiskAction,
                               msg: ActivationMessage,
                               initInterval: Option[Interval],
                               totalInterval: Interval,
                               isTimeout: Boolean,
                               response: ExecutionResponse) = {

    val causedBy = if (msg.causedBySequence) {
      Some(Parameters(WhiskActivation.causedByAnnotation, JsString(Exec.SEQUENCE)))
    } else None

    val waitTime = {
      val end = initInterval.map(_.start).getOrElse(totalInterval.start)
      Parameters(WhiskActivation.waitTimeAnnotation, Interval(msg.transid.meta.start, end).duration.toMillis.toJson)
    }

    val initTime = {
      initInterval.map(initTime => Parameters(WhiskActivation.initTimeAnnotation, initTime.duration.toMillis.toJson))
    }

    val binding =
      msg.action.binding.map(f => Parameters(WhiskActivation.bindingAnnotation, JsString(f.asString)))

    WhiskActivation(
      activationId = msg.activationId,
      namespace = msg.user.namespace.name.toPath,
      subject = msg.user.subject,
      cause = msg.cause,
      name = action.name,
      version = action.version,
      start = totalInterval.start,
      end = totalInterval.end,
      duration = Some(totalInterval.duration.toMillis),
      response = response,
      annotations = {
        Parameters(WhiskActivation.limitsAnnotation, action.limits.toJson) ++
          Parameters(WhiskActivation.pathAnnotation, JsString(action.fullyQualifiedName(false).asString)) ++
          Parameters(WhiskActivation.kindAnnotation, JsString(action.exec.kind)) ++
          Parameters(WhiskActivation.timeoutAnnotation, JsBoolean(isTimeout)) ++
          causedBy ++ initTime ++ waitTime ++ binding
      })
  }

}
