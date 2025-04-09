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

package org.apache.openwhisk.core.scheduler.queue

import akka.actor.{Actor, ActorSystem, Props}
import org.apache.openwhisk.common.Logging
import org.apache.openwhisk.core.entity.FullyQualifiedEntityName
import org.apache.openwhisk.core.scheduler.SchedulingConfig

import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success}

class SchedulingDecisionMaker(
  invocationNamespace: String,
  action: FullyQualifiedEntityName,
  schedulingConfig: SchedulingConfig)(implicit val actorSystem: ActorSystem, ec: ExecutionContext, logging: Logging)
    extends Actor {

  private val staleThreshold: Double = schedulingConfig.staleThreshold.toMillis.toDouble

  override def receive: Receive = {
    case msg: QueueSnapshot =>
      decide(msg)
        .andThen {
          case Success(DecisionResults(Skip, _, _)) =>
          // do nothing
          case Success(result: DecisionResults) =>
            msg.recipient ! result
            logging.info(this, s"msg.recipient:${msg.recipient}")
          case Failure(e) =>
            logging.error(this, s"failed to make a scheduling decision due to $e");
        }
  }

  private[queue] def decide(snapshot: QueueSnapshot) = {
    // 刷日志
    // logging.info(this, s"snapshot:$snapshot")
    val activationIds = snapshot.activationMessages.map(_.activationId.toString) // 提取 activationIds
    // 函数从 QueueSnapshot 中解包出多个参数
    val QueueSnapshot(
      initialized, // 是否是初始化队列
      incoming, // 正在到来的msg数
      currentMsg, // 当前msg数
      existing, // 现存容器数
      inProgress, // 正在执行任务的容器数
      staleActivationNum, // 过时的激活消息
      existingContainerCountInNs,
      inProgressContainerCountInNs,
      averageDuration,
      namespaceLimit,
      actionLimit,
      maxActionConcurrency,
      stateName,
      recipient,
      activationMessages) = snapshot
    // 计算当前正在使用的容器总数
    // 不加inProgress，因为等待中的请求也会占一个inProgress，导致capacity不够用
    // val totalContainers = existing + inProgress
    val totalContainers = existing
    // 计算当前和即将到来的消息总数
    val availableMsg = currentMsg + incoming.get()
    // action的可用槽数
    val actionCapacity = actionLimit - totalContainers
    // 命名空间的可用槽数
    val namespaceCapacity = namespaceLimit - existingContainerCountInNs - inProgressContainerCountInNs
    // 允许在某些配置下进行超额分配，以便应对资源需求高峰，本步计算允许超额分配的容量
    val overProvisionCapacity = ceiling(namespaceLimit * schedulingConfig.namespaceOverProvisionBeforeThrottleRatio) - existingContainerCountInNs - inProgressContainerCountInNs

    if (Math.min(namespaceLimit, actionLimit) <= 0) {
      // 如果 namespaceLimit 或 actionLimit 小于等于零，将其视为错误：
      // 在“Flushing”状态下，它会返回 Skip（跳过）决策。
      // 否则，将返回 Pause（暂停）决策。
      logging.error(this, s"min(namespaceLimit, actionLimit) <= 0, namespaceLimit: $namespaceLimit, actionLimit: $actionLimit")
      // this is an error case, the limit should be bigger than 0
      stateName match {
        case Flushing => Future.successful(DecisionResults(Skip, activationIds, 0))//改
        case _        => Future.successful(DecisionResults(Pausing, activationIds, 0))//改
      }
    } else {
      val capacity = if (schedulingConfig.allowOverProvisionBeforeThrottle && totalContainers == 0) {
        // 如果配置允许超配（allowOverProvisionBeforeThrottle），且当前没有容器，则尝试在超配限制下创建一个新容器以平衡流量。 

        // if space available within the over provision ratio amount above namespace limit, create one container for new
        // action so namespace traffic can attempt to re-balance without blocking entire action
        if (overProvisionCapacity > 0) {
          1
        } else {
          0
        }
      } else {
        // 否则，capacity 取命名空间和操作容量的最小值。
        Math.min(namespaceCapacity, actionCapacity)
      }
      // 刷日志
      // logging.info(this, s"schedulingConfig.allowOverProvisionBeforeThrottle && totalContainers == 0, capacity: $capacity")

      if (capacity <= 0) {
        logging.error(this, s"capacity: $capacity <= 0")
        logging.error(this, s"actionCapacity:$actionCapacity = actionLimit:$actionLimit - totalContainers:$totalContainers")
        logging.error(this, s"totalContainers:$totalContainers = existing:$existing")
        logging.error(this, s"namespaceCapacity:$namespaceCapacity = namespaceLimit:$namespaceLimit - existingContainerCountInNs:$existingContainerCountInNs - inProgressContainerCountInNs:$inProgressContainerCountInNs")
        
        stateName match {

          /**
           * If the container is created later (for any reason), all activations fail(too many requests).
           *
           * However, if the container exists(totalContainers != 0), the activation is not treated as a failure and the activation is delivered to the container.
           */
          case Running
              if !schedulingConfig.allowOverProvisionBeforeThrottle || (schedulingConfig.allowOverProvisionBeforeThrottle && overProvisionCapacity <= 0) =>
            logging.info(
              this,
              s"there is no capacity activations will be dropped or throttled, (availableMsg: $availableMsg totalContainers: $totalContainers, actionLimit: $actionLimit, namespaceLimit: $namespaceLimit, namespaceContainers: $existingContainerCountInNs, namespaceInProgressContainer: $inProgressContainerCountInNs) [$invocationNamespace:$action]")
            Future.successful(DecisionResults(EnableNamespaceThrottling(dropMsg = totalContainers == 0), activationIds, 0))//
          case NamespaceThrottled if schedulingConfig.allowOverProvisionBeforeThrottle && overProvisionCapacity > 0 =>
            Future.successful(DecisionResults(DisableNamespaceThrottling, activationIds, 0))//
          // do nothing
          case _ =>
            // no need to print any messages if the state is already NamespaceThrottled
            Future.successful(DecisionResults(Skip, activationIds, 0))//
        }
      } else {
        (stateName, averageDuration) match {
          // there is no container
          case (Running, None) if totalContainers == 0 && !initialized =>
            logging.info(
              this,
              s"add one initial container if totalContainers($totalContainers) == 0 [$invocationNamespace:$action]")
            Future.successful(DecisionResults(AddInitialContainer, activationIds, 1))//

          // Todo: when disabling throttling we may create some containers.
          case (NamespaceThrottled, _) =>
            Future.successful(DecisionResults(DisableNamespaceThrottling, activationIds, 0))//

          // 为了应对averageDuration为None的情况新增的逻辑
          // 一旦有msg需要,马上创建新容器
          case (Running, _) if availableMsg > 0 =>
            logging.info(
              this,
              s"add one container if availableMsg($availableMsg) > 0 [$invocationNamespace:$action]")
            Future.successful(DecisionResults(AddContainer, activationIds, 1))

          // this is an exceptional case, create a container immediately
          case (Running, _) if totalContainers == 0 && availableMsg > 0 =>
            logging.info(
              this,
              s"add one container if totalContainers($totalContainers) == 0 && availableMsg($availableMsg) > 0 [$invocationNamespace:$action]")
            Future.successful(DecisionResults(AddContainer, activationIds, 1))//

          case (Flushing, _) if totalContainers == 0 =>
            logging.info(
              this,
              s"add one container case Paused if totalContainers($totalContainers) == 0 [$invocationNamespace:$action]")
            // it is highly likely the queue could not create an initial container if the limit is 0
            Future.successful(DecisionResults(AddInitialContainer, activationIds, 1))//

          // there is no activation result yet, but some activations became stale
          // it may cause some over-provisioning if it takes much time to create a container and execution time is short.
          // but it is a kind of trade-off and we place latency on top of over-provisioning
          case (Running, None) if staleActivationNum > 0 =>
            // we can safely get the value as we already checked the existence
            val num = ceiling(staleActivationNum.toDouble / maxActionConcurrency.toDouble) - inProgress
            // if it tries to create more containers than existing messages, we just create shortage
            val actualNum = if (num > availableMsg) availableMsg else num
            logging.info(this, s"case (Running, None), num:$num, actualNum:$actualNum, availableMsg:$availableMsg")
            addServersIfPossible(
              existing,
              inProgress,
              0,
              availableMsg,
              capacity,
              namespaceCapacity,
              actualNum,
              staleActivationNum,
              0.0,
              Running,
              activationIds)
          // need more containers and a message is already processed
          case (Running, Some(duration)) =>
            // we can safely get the value as we already checked the existence, have extra protection in case duration is somehow negative
            val containerThroughput = if (duration <= 0) {
              maxActionConcurrency
            } else {
              (staleThreshold / duration) * maxActionConcurrency
            }
            val expectedTps = containerThroughput * (existing + inProgress)
            val availableNonStaleActivations = availableMsg - staleActivationNum

            var staleContainerProvision = 0
            if (staleActivationNum > 0) {

              val num = ceiling(staleActivationNum.toDouble / containerThroughput)
              // if it tries to create more containers than existing messages, we just create shortage
              staleContainerProvision = (if (num > staleActivationNum) staleActivationNum else num) - inProgress
              logging.info(this, s"staleActivationNum > 0, it is $staleActivationNum, staleContainerProvision: $staleContainerProvision")
            }

            if (availableNonStaleActivations >= expectedTps && existing + inProgress < availableNonStaleActivations && duration > 0) {
              val num = ceiling((availableNonStaleActivations / containerThroughput) - existing - inProgress)
              // if it tries to create more containers than existing messages, we just create shortage
              val actualNum =
                if (num + totalContainers > availableNonStaleActivations) availableNonStaleActivations - totalContainers
                else num                
              logging.info(this, s"if1 num: $num, totalContainers: $totalContainers, availableNonStaleActivations: $availableNonStaleActivations, staleContainerProvision: $staleContainerProvision ")
              addServersIfPossible(
                existing,
                inProgress,
                containerThroughput,
                availableMsg,
                capacity,
                namespaceCapacity,
                actualNum + staleContainerProvision,
                staleActivationNum,
                duration,
                Running,
                activationIds)
            } else if (staleContainerProvision > 0) {
              logging.info(this, s"if2 totalContainers: $totalContainers, availableNonStaleActivations: $availableNonStaleActivations, staleContainerProvision: $staleContainerProvision ")
              addServersIfPossible(
                existing,
                inProgress,
                containerThroughput,
                availableMsg,
                capacity,
                namespaceCapacity,
                staleContainerProvision,
                staleActivationNum,
                duration,
                Running,
                activationIds)
            } else {
              // logging.warn(this, s"staleContainerProvision: $staleContainerProvision <= 0, so no if2 and skip")
              Future.successful(DecisionResults(Skip, activationIds, 0))//
            }

          // generally we assume there are enough containers for actions when shutting down the scheduler
          // but if there were already too many activation in the queue with not enough containers,
          // we should add more containers to quickly consume those messages.
          // this case is for that as a last resort.
          case (Removing, Some(duration)) if staleActivationNum > 0 =>
            // we can safely get the value as we already checked the existence
            val containerThroughput = if (duration <= 0) {
              maxActionConcurrency
            } else {
              (staleThreshold / duration) * maxActionConcurrency
            }
            val num = ceiling(staleActivationNum.toDouble / containerThroughput)
            // if it tries to create more containers than existing messages, we just create shortage
            val actualNum = (if (num > staleActivationNum) staleActivationNum else num) - inProgress
            logging.info(this, s" case (Removing, Some(duration), num: $num, staleActivationNum: $staleActivationNum, totalContainers: $totalContainers")
            addServersIfPossible(
              existing,
              inProgress,
              containerThroughput,
              availableMsg,
              capacity,
              namespaceCapacity,
              actualNum,
              staleActivationNum,
              duration,
              Running,
              activationIds)

          // same with the above case but no duration exist.
          case (Removing, None) if staleActivationNum > 0 =>
            // we can safely get the value as we already checked the existence
            val num = ceiling(staleActivationNum.toDouble / maxActionConcurrency.toDouble) - inProgress
            // if it tries to create more containers than existing messages, we just create shortage
            val actualNum = if (num > availableMsg) availableMsg else num
            logging.info(this, s"case (Removing, None)")
            addServersIfPossible(
              existing,
              inProgress,
              0,
              availableMsg,
              capacity,
              namespaceCapacity,
              actualNum,
              staleActivationNum,
              0.0,
              Running,
              activationIds)

          // do nothing
          case _ =>
            Future.successful(DecisionResults(Skip, activationIds, 0))//
        }
      }
    }
  }

  private def addServersIfPossible(existing: Int,
                                   inProgress: Int,
                                   containerThroughput: Double,
                                   availableMsg: Int,
                                   capacity: Int,
                                   namespaceCapacity: Int,
                                   actualNum: Int,
                                   staleActivationNum: Int,
                                   duration: Double = 0.0,
                                   state: MemoryQueueState,
                                   activationIds: List[String]) = {
    logging.info(this, s"actualNum:$actualNum ,capacity$capacity")
    if (actualNum > capacity) {
      if (capacity >= namespaceCapacity) {
        // containers can be partially created. throttling should be enabled
        logging.info(
          this,
          s"[$state] enable namespace throttling and add $capacity container, staleActivationNum: $staleActivationNum, duration: $duration, containerThroughput: $containerThroughput, availableMsg: $availableMsg, existing: $existing, inProgress: $inProgress, capacity: $capacity [$invocationNamespace:$action]")
        Future.successful(DecisionResults(EnableNamespaceThrottling(dropMsg = false), _, capacity))//
      } else {
        logging.info(
          this,
          s"[$state] reached max containers allowed for this action adding $capacity containers, but there is still capacity on the namespace so namespace throttling is not turned on." +
            s" staleActivationNum: $staleActivationNum, duration: $duration, containerThroughput: $containerThroughput, availableMsg: $availableMsg, existing: $existing, inProgress: $inProgress, capacity: $capacity [$invocationNamespace:$action]")
        Future.successful(DecisionResults(AddContainer, _, capacity))//
      }
    } else if (actualNum <= 0) {
      // it means nothing
      // logging.info(this, s"actualNum:$actualNum <= 0, so skip")
      Future.successful(DecisionResults(Skip, _, 0))//
    } else {
      // create num containers
      // we need to create one more container than expected because existing container would already took the message
      logging.info(
        this,
        s"[$state]add $actualNum container, staleActivationNum: $staleActivationNum, duration: $duration, containerThroughput: $containerThroughput, availableMsg: $availableMsg, existing: $existing, inProgress: $inProgress, capacity: $capacity [$invocationNamespace:$action]")
      logging.info(this, s"generate DecisionResults: ${DecisionResults(AddContainer, activationIds, actualNum)}")
      Future.successful(DecisionResults(AddContainer, activationIds, actualNum))//
    }
  }

  private def ceiling(d: Double) = math.ceil(d).toInt
}

object SchedulingDecisionMaker {
  def props(invocationNamespace: String, action: FullyQualifiedEntityName, schedulingConfig: SchedulingConfig)(
    implicit actorSystem: ActorSystem,
    ec: ExecutionContext,
    logging: Logging): Props = {
    Props(new SchedulingDecisionMaker(invocationNamespace, action, schedulingConfig))
  }
}
