/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
*/
package kafka.controller

import kafka.api.LeaderAndIsr
import kafka.common.StateChangeFailedException
import kafka.server.KafkaConfig
import kafka.utils.Implicits._
import kafka.utils.Logging
import kafka.zk.KafkaZkClient
import kafka.zk.KafkaZkClient.UpdateLeaderAndIsrResult
import kafka.zk.TopicPartitionStateZNode
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.errors.ControllerMovedException
import org.apache.zookeeper.KeeperException.Code
import scala.collection.{Seq, mutable}

abstract class ReplicaStateMachine(controllerContext: ControllerContext) extends Logging {
  /**
   * Invoked on successful controller election see {@link KafkaController#onControllerFailover}.
   */ // 每次当controller选取成功时，调用副本状态机，处理副本，使副本处于正确的状态
  def startup(): Unit = {
    info("Initializing replica state")
    initializeReplicaState() // 状态机初始化
    info("Triggering online replica state changes")
    val (onlineReplicas, offlineReplicas) = controllerContext.onlineAndOfflineReplicas // controller元数据维护的在线副本和离线副本
    handleStateChanges(onlineReplicas.toSeq, OnlineReplica) // 处理onlineReplicas使其处于上线状态
    info("Triggering offline replica state changes")
    handleStateChanges(offlineReplicas.toSeq, OfflineReplica) // 处理offlineReplicas使其处于下线状态
    debug(s"Started replica state machine with initial state -> ${controllerContext.replicaStates}")
  }

  /**
   * Invoked on controller shutdown.
   */
  def shutdown(): Unit = {
    info("Stopped replica state machine")
  }

  /**
   * Invoked on startup of the replica's state machine to set the initial state for replicas of all existing partitions
   * in zookeeper
   */ // 状态机初始化
  private def initializeReplicaState(): Unit = {
    controllerContext.allPartitions.foreach { partition =>
      val replicas = controllerContext.partitionReplicaAssignment(partition)
      replicas.foreach { replicaId =>
        val partitionAndReplica = PartitionAndReplica(partition, replicaId)
        if (controllerContext.isReplicaOnline(replicaId, partition)) {
          controllerContext.putReplicaState(partitionAndReplica, OnlineReplica)
        } else {
          // mark replicas on dead brokers as failed for topic deletion, if they belong to a topic to be deleted.
          // This is required during controller failover since during controller failover a broker can go down,
          // so the replicas on that broker should be moved to ReplicaDeletionIneligible to be on the safer side.
          controllerContext.putReplicaState(partitionAndReplica, ReplicaDeletionIneligible)
        }
      }
    }
  }
  // 状态机处理逻辑；用于处理副本状态，将给定副本转换成指定状态，并向broker发送控制类请求 [处理状态的变更，是对外提供状态转换操作的入口方法]
  def handleStateChanges(replicas: Seq[PartitionAndReplica], targetState: ReplicaState): Unit
} // replicas是一组副本对象，每个副本对象都封装了它们各自所属的主题、分区以及副本所在的Broker ID数据；targetState是这组副本对象要转换成的目标状态

/**
 * This class represents the state machine for replicas. It defines the states that a replica can be in, and
 * transitions to move the replica to another legal state. The different states that a replica can be in are -
 * 1. NewReplica        : The controller can create new replicas during partition reassignment. In this state, a
 *                        replica can only get become follower state change request.  Valid previous
 *                        state is NonExistentReplica
 * 2. OnlineReplica     : Once a replica is started and part of the assigned replicas for its partition, it is in this
 *                        state. In this state, it can get either become leader or become follower state change requests.
 *                        Valid previous state are NewReplica, OnlineReplica, OfflineReplica and ReplicaDeletionIneligible
 * 3. OfflineReplica    : If a replica dies, it moves to this state. This happens when the broker hosting the replica
 *                        is down. Valid previous state are NewReplica, OnlineReplica, OfflineReplica and ReplicaDeletionIneligible
 * 4. ReplicaDeletionStarted: If replica deletion starts, it is moved to this state. Valid previous state is OfflineReplica
 * 5. ReplicaDeletionSuccessful: If replica responds with no error code in response to a delete replica request, it is
 *                        moved to this state. Valid previous state is ReplicaDeletionStarted
 * 6. ReplicaDeletionIneligible: If replica deletion fails, it is moved to this state. Valid previous states are
 *                        ReplicaDeletionStarted and OfflineReplica
 * 7. NonExistentReplica: If a replica is deleted successfully, it is moved to this state. Valid previous state is
 *                        ReplicaDeletionSuccessful
 */ // 副本状态机唯一实现类，KafkaController对象在构建的时候，就会初始化一个ZkReplicaStateMachine实例，如果一个Broker没有被选举为Controller，它也会构建KafkaController对象实例, 所有Broker在启动时，都会创建KafkaController实例，因而也会创建ZKReplicaStateMachine实例。
class ZkReplicaStateMachine(config: KafkaConfig,         // 每个Broker都会创建这些实例，并不代表每个Broker都会启动副本状态机。事实上，只有在Controller所在的Broker上，副本状态机才会被启动。具体的启动代码位于KafkaController的onControllerFailover方法; 当Broker被成功推举为Controller后，onControllerFailover方法会被调用，进而调用ReplicaStateMachine#start方法启动该Broker早已创建好的副本状态机和分区状态机
                            stateChangeLogger: StateChangeLogger,
                            controllerContext: ControllerContext, // ControllerContext封装了Controller端保存的所有集群元数据信息。
                            zkClient: KafkaZkClient, // 用于同zk交互
                            controllerBrokerRequestBatch: ControllerBrokerRequestBatch) // 用于给集群Broker发送控制类请求（LeaderAndIsrRequest、StopReplicaRequest和UpdateMetadataRequest）
  extends ReplicaStateMachine(controllerContext) with Logging {                         // 主要的功能是将给定的Request发送给指定的Broker，在副本状态转换操作的逻辑中，一个很重要的步骤，就是为Broker上的副本更新信息，而这是通过Controller给Broker发送请求实现的

  private val controllerId = config.brokerId
  this.logIdent = s"[ReplicaStateMachine controllerId=$controllerId] "
  // 处理状态的变更，是对外提供状态转换操作的入口方法, replicas是一组副本对象，每个副本对象都封装了它们各自所属的主题、分区以及副本所在的Broker ID数据；targetState是这组副本对象要转换成的目标状态
  override def handleStateChanges(replicas: Seq[PartitionAndReplica], targetState: ReplicaState): Unit = {
    if (replicas.nonEmpty) {
      try { // check是否可以创建newBatch
        controllerBrokerRequestBatch.newBatch()
        replicas.groupBy(_.replica).forKeyValue { (replicaId, replicas) =>
          doHandleStateChanges(replicaId, replicas, targetState) // 将所有副本对象按照Broker进行分组，依次执行状态转换操作
        } // 发送对应的Controller请求[控制类请求]给Broker
        controllerBrokerRequestBatch.sendRequestsToBrokers(controllerContext.epoch) // controller给broker发送控制类请求,将给定的Request发送给指定的Broker
      } catch {                                                                     // 在副本状态转换操作的逻辑中，一个很重要的步骤，就是为Broker上的副本更新信息，而这是通过Controller给Broker发送请求实现的
        case e: ControllerMovedException => // 如果Controller易主，则记录错误日志然后抛出异常
          error(s"Controller moved to another broker when moving some replicas to $targetState state", e)
          throw e
        case e: Throwable => error(s"Error while moving some replicas to $targetState state", e)
      }
    }
  } // 代码逻辑总体上分为两步：第1步是调用doHandleStateChanges方法执行真正的副本状态转换；第2步是给集群中的相应Broker批量发送请求。
    // 在执行第1步的时候，会将replicas按照Broker ID进行分组。举个例子，如果我们使用<主题名，分区号，副本Broker ID>表示副本对象，假设replicas为集合（, , , ），那么，在调用doHandleStateChanges方法前，代码会将replicas按照Broker ID进行分组，即变成：Map(1001 -> Set(, )，1002 -> Set(, ))。待这些都做完之后，代码开始调用doHandleStateChanges方法，执行状态转换操作。这个方法看着很长，其实都是不同的代码分支。
  /**
   * This API exercises the replica's state machine. It ensures that every state transition happens from a legal
   * previous state to the target state. Valid state transitions are:
   * NonExistentReplica --> NewReplica
   * --send LeaderAndIsr request with current leader and isr to the new replica and UpdateMetadata request for the
   *   partition to every live broker
   *
   * NewReplica -> OnlineReplica
   * --add the new replica to the assigned replica list if needed
   *
   * OnlineReplica,OfflineReplica -> OnlineReplica
   * --send LeaderAndIsr request with current leader and isr to the new replica and UpdateMetadata request for the
   *   partition to every live broker
   *
   * NewReplica,OnlineReplica,OfflineReplica,ReplicaDeletionIneligible -> OfflineReplica
   * --send StopReplicaRequest to the replica (w/o deletion)
   * --remove this replica from the isr and send LeaderAndIsr request (with new isr) to the leader replica and
   *   UpdateMetadata request for the partition to every live broker.
   *
   * OfflineReplica -> ReplicaDeletionStarted
   * --send StopReplicaRequest to the replica (with deletion)
   *
   * ReplicaDeletionStarted -> ReplicaDeletionSuccessful
   * -- mark the state of the replica in the state machine
   *
   * ReplicaDeletionStarted -> ReplicaDeletionIneligible
   * -- mark the state of the replica in the state machine
   *
   * ReplicaDeletionSuccessful -> NonExistentReplica
   * -- remove the replica from the in memory partition replica assignment cache
   *
   * @param replicaId The replica for which the state transition is invoked
   * @param replicas The partitions on this replica for which the state transition is invoked
   * @param targetState The end state that the replica should be moved to
   */ // 执行状态变更和转换操作的主力方法。
  private def doHandleStateChanges(replicaId: Int, replicas: Seq[PartitionAndReplica], targetState: ReplicaState): Unit = {
    val stateLogger = stateChangeLogger.withControllerEpoch(controllerContext.epoch)
    val traceEnabled = stateLogger.isTraceEnabled
    replicas.foreach(replica => controllerContext.putReplicaStateIfNotExists(replica, NonExistentReplica)) // 代码的第1步，会尝试获取给定副本对象在Controller端元数据缓存中的当前状态，如果没有保存某个副本对象的状态，代码会将其初始化为NonExistentReplica状态。
    val (validReplicas, invalidReplicas) = controllerContext.checkValidReplicaStateChange(replicas, targetState) // 第2步，代码根据不同ReplicaState中定义的合法前置状态集合以及传入的目标状态（targetState），将给定的副本对象集合划分成两部分：能够合法转换的副本对象集合，以及执行非法状态转换的副本对象集合。
    invalidReplicas.foreach(replica => logInvalidTransition(replica, targetState)) // 打印错误日志，记录每个非法状态转换的副本对象。

    targetState match {
      case NewReplica =>
        validReplicas.foreach { replica => // 遍历所有能够执行转换的副本对象
          val partition = replica.topicPartition // 获取该副本对象的分区对象，即<主题名，分区号>数据
          val currentState = controllerContext.replicaState(replica) // 获取副本对象的当前状态[ReplicaState]
          // 尝试从元数据缓存中获取该分区当前信息，包括Leader是谁、ISR都有哪些副本等数据
          controllerContext.partitionLeadershipInfo(partition) match {
            case Some(leaderIsrAndControllerEpoch) => // 如果成功拿到分区数据信息
              if (leaderIsrAndControllerEpoch.leaderAndIsr.leader == replicaId) { // 如果该副本是Leader副本，记录错误日志。Leader副本不能被设置成NewReplica状态
                val exception = new StateChangeFailedException(s"Replica $replicaId for partition $partition cannot be moved to NewReplica state as it is being requested to become leader")
                logFailedStateChange(replica, currentState, OfflineReplica, exception)
              } else { // 否则，给该副本所在的Broker发送LeaderAndIsrRequest，向它同步该分区的数据, 之后给集群当前所有Broker发送UpdateMetadataRequest通知它们该分区数据发生变更
                controllerBrokerRequestBatch.addLeaderAndIsrRequestForBrokers(Seq(replicaId),
                  replica.topicPartition,
                  leaderIsrAndControllerEpoch,
                  controllerContext.partitionFullReplicaAssignment(replica.topicPartition),
                  isNew = true)
                if (traceEnabled)
                  logSuccessfulTransition(stateLogger, replicaId, partition, currentState, NewReplica)
                controllerContext.putReplicaState(replica, NewReplica) // 更新元数据缓存中该副本对象的当前状态为NewReplica
              }
            case None => // 如果没有相应数据
              if (traceEnabled)
                logSuccessfulTransition(stateLogger, replicaId, partition, currentState, NewReplica)
              controllerContext.putReplicaState(replica, NewReplica) // 仅仅更新元数据缓存中该副本对象的当前状态为NewReplica即可
          } // case NewReplica: 尝试从元数据缓存中，获取这些副本对象的分区信息数据，包括分区的Leader副本在哪个Broker上、ISR中都有哪些副本，等等。如果找不到对应的分区数据，就直接把副本状态更新为NewReplica。否则，代码就需要给该副本所在的Broker发送请求，让它知道该分区的信息。同时，代码还要给集群所有运行中的Broker发送请求，让它们感知到新副本的加入。
        }
      case OnlineReplica => // 遍历副本对象，第1步，获取元数据中该副本所属的分区对象，以及该副本的当前状态。第2步，查看当前状态是否是NewReplica。如果是，则获取分区的副本列表，并判断该副本是否在当前的副本列表中，假如不在，就记录错误日志，并更新元数据中的副本列表；如果状态不是NewReplica，就说明，这是一个已存在的副本对象，那么，源码会获取对应分区的详细数据，然后向该副本对象所在的Broker发送LeaderAndIsrRequest请求，令其同步获知，并保存该分区数据。第3步，将该副本对象状态变更为OnlineReplica。至此，该副本处于正常工作状态。
        validReplicas.foreach { replica =>
          val partition = replica.topicPartition // 获取副本所在分区
          val currentState = controllerContext.replicaState(replica) // 获取副本当前状态

          currentState match {
            case NewReplica => // 如果当前状态是NewReplica，从元数据缓存中拿到分区副本列表
              val assignment = controllerContext.partitionFullReplicaAssignment(partition)
              if (!assignment.replicas.contains(replicaId)) { // 如果副本列表不包含当前副本，视为异常情况
                error(s"Adding replica ($replicaId) that is not part of the assignment $assignment")
                val newAssignment = assignment.copy(replicas = assignment.replicas :+ replicaId) // 将该副本加入到副本列表中，并更新元数据缓存中该分区的副本列表
                controllerContext.updatePartitionFullReplicaAssignment(partition, newAssignment)
              } // 如果当前状态是其他状态
            case _ =>
              controllerContext.partitionLeadershipInfo(partition) match { // 尝试获取该分区当前信息数据，包括Leader是谁、ISR都有哪些副本等数据
                case Some(leaderIsrAndControllerEpoch) => // 如果存在分区信息，向该副本对象所在Broker发送请求，令其同步该分区数据
                  controllerBrokerRequestBatch.addLeaderAndIsrRequestForBrokers(Seq(replicaId),
                    replica.topicPartition,
                    leaderIsrAndControllerEpoch,
                    controllerContext.partitionFullReplicaAssignment(partition), isNew = false)
                case None =>
              }
          }
          if (traceEnabled)
            logSuccessfulTransition(stateLogger, replicaId, partition, currentState, OnlineReplica)
          controllerContext.putReplicaState(replica, OnlineReplica) // 将该副本对象设置成OnlineReplica状态
        }
      case OfflineReplica => // 【1】首先，代码会给所有符合状态转换的副本所在的Broker，发送StopReplicaRequest请求，显式地告诉这些Broker停掉其上的对应副本。Kafka的副本管理器组件（ReplicaManager）负责处理这个逻辑。【2】其次，代码根据分区是否保存了Leader信息，将副本集合划分成两个子集：有Leader副本集合和无Leader副本集合。有无Leader信息并不仅仅包含Leader，还有ISR和controllerEpoch等数据。大致可以认为，副本集合是根据有无Leader进行划分的。【3】接下来，源码会遍历有Leader的子集合，向这些副本所在的Broker发送LeaderAndIsrRequest请求，去更新停止副本操作之后的分区信息，再把这些分区状态设置为OfflineReplica。【4】最后，源码遍历无Leader的子集合，执行与上一步非常类似的操作。只不过，对于无Leader而言，因为我们没有执行任何Leader选举操作，所以给这些副本所在的Broker发送的就不是LeaderAndIsrRequest请求了，而是UpdateMetadataRequest请求，显式去告知它们更新对应分区的元数据即可，然后再把副本状态设置为OfflineReplica。
        validReplicas.foreach { replica => // 向副本所在Broker发送StopReplicaRequest请求，停止对应副本
          controllerBrokerRequestBatch.addStopReplicaRequestForBrokers(Seq(replicaId), replica.topicPartition, deletePartition = false)
        } // 将副本对象集合划分成有Leader信息的副本集合和无Leader信息的副本集合
        val (replicasWithLeadershipInfo, replicasWithoutLeadershipInfo) = validReplicas.partition { replica =>
          controllerContext.partitionLeadershipInfo(replica.topicPartition).isDefined
        } // 对于有Leader信息的副本集合而言，从它们对应的所有分区中移除该副本对象并更新ZooKeeper节点
        val updatedLeaderIsrAndControllerEpochs = removeReplicasFromIsr(replicaId, replicasWithLeadershipInfo.map(_.topicPartition))
        updatedLeaderIsrAndControllerEpochs.forKeyValue { (partition, leaderIsrAndControllerEpoch) => // 遍历每个更新过的分区信息
          stateLogger.info(s"Partition $partition state changed to $leaderIsrAndControllerEpoch after removing replica $replicaId from the ISR as part of transition to $OfflineReplica")
          if (!controllerContext.isTopicQueuedUpForDeletion(partition.topic)) { // 如果分区对应主题并未被删除
            val recipients = controllerContext.partitionReplicaAssignment(partition).filterNot(_ == replicaId) // 获取该分区除给定副本以外的其他副本所在的Broker
            controllerBrokerRequestBatch.addLeaderAndIsrRequestForBrokers(recipients, // 向这些Broker发送请求更新该分区更新过的分区LeaderAndIsr数据
              partition,
              leaderIsrAndControllerEpoch,
              controllerContext.partitionFullReplicaAssignment(partition), isNew = false)
          }
          val replica = PartitionAndReplica(partition, replicaId)
          val currentState = controllerContext.replicaState(replica)
          if (traceEnabled)
            logSuccessfulTransition(stateLogger, replicaId, partition, currentState, OfflineReplica)
          controllerContext.putReplicaState(replica, OfflineReplica) // 设置该分区给定副本的状态为OfflineReplica
        }
        // 遍历无Leader信息的所有副本对象
        replicasWithoutLeadershipInfo.foreach { replica =>
          val currentState = controllerContext.replicaState(replica)
          if (traceEnabled)
            logSuccessfulTransition(stateLogger, replicaId, replica.topicPartition, currentState, OfflineReplica)
          controllerBrokerRequestBatch.addUpdateMetadataRequestForBrokers(controllerContext.liveOrShuttingDownBrokerIds.toSeq, Set(replica.topicPartition)) // 向集群所有Broker发送请求，更新对应分区的元数据
          controllerContext.putReplicaState(replica, OfflineReplica) // 设置该分区给定副本的状态为OfflineReplica
        } // 把副本状态变更为OfflineReplica的主要逻辑，其实就是停止对应副本+更新远端Broker元数据的操作。
      case ReplicaDeletionStarted =>
        validReplicas.foreach { replica =>
          val currentState = controllerContext.replicaState(replica)
          if (traceEnabled)
            logSuccessfulTransition(stateLogger, replicaId, replica.topicPartition, currentState, ReplicaDeletionStarted)
          controllerContext.putReplicaState(replica, ReplicaDeletionStarted)
          controllerBrokerRequestBatch.addStopReplicaRequestForBrokers(Seq(replicaId), replica.topicPartition, deletePartition = true)
        }
      case ReplicaDeletionIneligible =>
        validReplicas.foreach { replica =>
          val currentState = controllerContext.replicaState(replica)
          if (traceEnabled)
            logSuccessfulTransition(stateLogger, replicaId, replica.topicPartition, currentState, ReplicaDeletionIneligible)
          controllerContext.putReplicaState(replica, ReplicaDeletionIneligible)
        }
      case ReplicaDeletionSuccessful =>
        validReplicas.foreach { replica =>
          val currentState = controllerContext.replicaState(replica)
          if (traceEnabled)
            logSuccessfulTransition(stateLogger, replicaId, replica.topicPartition, currentState, ReplicaDeletionSuccessful)
          controllerContext.putReplicaState(replica, ReplicaDeletionSuccessful)
        }
      case NonExistentReplica =>
        validReplicas.foreach { replica =>
          val currentState = controllerContext.replicaState(replica) // 获取副本当前状态
          val newAssignedReplicas = controllerContext
            .partitionFullReplicaAssignment(replica.topicPartition) // 获取分区的副本信息，并删除当前副本
            .removeReplica(replica.replica)

          controllerContext.updatePartitionFullReplicaAssignment(replica.topicPartition, newAssignedReplicas) // 并更新元数据缓存中该分区的副本列表
          if (traceEnabled)
            logSuccessfulTransition(stateLogger, replicaId, replica.topicPartition, currentState, NonExistentReplica)
          controllerContext.removeReplicaState(replica) // 从元数据中移除该副本对象
        }
    }
  }

  /**
   * Repeatedly attempt to remove a replica from the isr of multiple partitions until there are no more remaining partitions
   * to retry.
   * @param replicaId The replica being removed from isr of multiple partitions
   * @param partitions The partitions from which we're trying to remove the replica from isr
   * @return The updated LeaderIsrAndControllerEpochs of all partitions for which we successfully removed the replica from isr.
   */ // 调用doRemoveReplicasFromIsr方法，实现将给定的副本对象从给定分区ISR中移除的功能。
  private def removeReplicasFromIsr(
    replicaId: Int,
    partitions: Seq[TopicPartition]
  ): Map[TopicPartition, LeaderIsrAndControllerEpoch] = {
    var results = Map.empty[TopicPartition, LeaderIsrAndControllerEpoch]
    var remaining = partitions
    while (remaining.nonEmpty) {
      val (finishedRemoval, removalsToRetry) = doRemoveReplicasFromIsr(replicaId, remaining)
      remaining = removalsToRetry

      finishedRemoval.foreach {
        case (partition, Left(e)) =>
            val replica = PartitionAndReplica(partition, replicaId)
            val currentState = controllerContext.replicaState(replica)
            logFailedStateChange(replica, currentState, OfflineReplica, e)
        case (partition, Right(leaderIsrAndEpoch)) =>
          results += partition -> leaderIsrAndEpoch
      }
    }
    results
  }

  /**
   * Try to remove a replica from the isr of multiple partitions.
   * Removing a replica from isr updates partition state in zookeeper.
   *
   * @param replicaId The replica being removed from isr of multiple partitions
   * @param partitions The partitions from which we're trying to remove the replica from isr
   * @return A tuple of two elements:
   *         1. The updated Right[LeaderIsrAndControllerEpochs] of all partitions for which we successfully
   *         removed the replica from isr. Or Left[Exception] corresponding to failed removals that should
   *         not be retried
   *         2. The partitions that we should retry due to a zookeeper BADVERSION conflict. Version conflicts can occur if
   *         the partition leader updated partition state while the controller attempted to update partition state.
   */ // 把给定的副本对象从给定分区ISR中移除。
  private def doRemoveReplicasFromIsr(
    replicaId: Int,
    partitions: Seq[TopicPartition]
  ): (Map[TopicPartition, Either[Exception, LeaderIsrAndControllerEpoch]], Seq[TopicPartition]) = {
    val (leaderAndIsrs, partitionsWithNoLeaderAndIsrInZk) = getTopicPartitionStatesFromZk(partitions)
    val (leaderAndIsrsWithReplica, leaderAndIsrsWithoutReplica) = leaderAndIsrs.partition { case (_, result) =>
      result.map { leaderAndIsr =>
        leaderAndIsr.isr.contains(replicaId)
      }.getOrElse(false)
    }

    val adjustedLeaderAndIsrs: Map[TopicPartition, LeaderAndIsr] = leaderAndIsrsWithReplica.flatMap {
      case (partition, result) =>
        result.toOption.map { leaderAndIsr =>
          val newLeader = if (replicaId == leaderAndIsr.leader) LeaderAndIsr.NoLeader else leaderAndIsr.leader
          val adjustedIsr = if (leaderAndIsr.isr.size == 1) leaderAndIsr.isr else leaderAndIsr.isr.filter(_ != replicaId)
          partition -> leaderAndIsr.newLeaderAndIsr(newLeader, adjustedIsr)
        }
    }

    val UpdateLeaderAndIsrResult(finishedPartitions, updatesToRetry) = zkClient.updateLeaderAndIsr(
      adjustedLeaderAndIsrs, controllerContext.epoch, controllerContext.epochZkVersion)

    val exceptionsForPartitionsWithNoLeaderAndIsrInZk: Map[TopicPartition, Either[Exception, LeaderIsrAndControllerEpoch]] =
      partitionsWithNoLeaderAndIsrInZk.iterator.flatMap { partition =>
        if (!controllerContext.isTopicQueuedUpForDeletion(partition.topic)) {
          val exception = new StateChangeFailedException(
            s"Failed to change state of replica $replicaId for partition $partition since the leader and isr " +
            "path in zookeeper is empty"
          )
          Option(partition -> Left(exception))
        } else None
      }.toMap

    val leaderIsrAndControllerEpochs: Map[TopicPartition, Either[Exception, LeaderIsrAndControllerEpoch]] =
      (leaderAndIsrsWithoutReplica ++ finishedPartitions).map { case (partition, result) =>
        (partition, result.map { leaderAndIsr =>
          val leaderIsrAndControllerEpoch = LeaderIsrAndControllerEpoch(leaderAndIsr, controllerContext.epoch)
          controllerContext.putPartitionLeadershipInfo(partition, leaderIsrAndControllerEpoch)
          leaderIsrAndControllerEpoch
        })
      }

    (leaderIsrAndControllerEpochs ++ exceptionsForPartitionsWithNoLeaderAndIsrInZk, updatesToRetry)
  }

  /**
   * Gets the partition state from zookeeper
   * @param partitions the partitions whose state we want from zookeeper
   * @return A tuple of two values:
   *         1. The Right(LeaderAndIsrs) of partitions whose state we successfully read from zookeeper.
   *         The Left(Exception) to failed zookeeper lookups or states whose controller epoch exceeds our current epoch
   *         2. The partitions that had no leader and isr state in zookeeper. This happens if the controller
   *         didn't finish partition initialization.
   */ // 从ZooKeeper中获取指定分区的状态信息，包括每个分区的Leader副本、ISR集合等数据。
  private def getTopicPartitionStatesFromZk(
    partitions: Seq[TopicPartition]
  ): (Map[TopicPartition, Either[Exception, LeaderAndIsr]], Seq[TopicPartition]) = {
    val getDataResponses = try {
      zkClient.getTopicPartitionStatesRaw(partitions)
    } catch {
      case e: Exception =>
        return (partitions.iterator.map(_ -> Left(e)).toMap, Seq.empty)
    }

    val partitionsWithNoLeaderAndIsrInZk = mutable.Buffer.empty[TopicPartition]
    val result = mutable.Map.empty[TopicPartition, Either[Exception, LeaderAndIsr]]

    getDataResponses.foreach[Unit] { getDataResponse =>
      val partition = getDataResponse.ctx.get.asInstanceOf[TopicPartition]
      if (getDataResponse.resultCode == Code.OK) {
        TopicPartitionStateZNode.decode(getDataResponse.data, getDataResponse.stat) match {
          case None =>
            partitionsWithNoLeaderAndIsrInZk += partition
          case Some(leaderIsrAndControllerEpoch) =>
            if (leaderIsrAndControllerEpoch.controllerEpoch > controllerContext.epoch) {
              val exception = new StateChangeFailedException(
                "Leader and isr path written by another controller. This probably " +
                s"means the current controller with epoch ${controllerContext.epoch} went through a soft failure and " +
                s"another controller was elected with epoch ${leaderIsrAndControllerEpoch.controllerEpoch}. Aborting " +
                "state change by this controller"
              )
              result += (partition -> Left(exception))
            } else {
              result += (partition -> Right(leaderIsrAndControllerEpoch.leaderAndIsr))
            }
        }
      } else if (getDataResponse.resultCode == Code.NONODE) {
        partitionsWithNoLeaderAndIsrInZk += partition
      } else {
        result += (partition -> Left(getDataResponse.resultException.get))
      }
    }

    (result.toMap, partitionsWithNoLeaderAndIsrInZk)
  }
  // 记录一次成功的状态转换操作。
  private def logSuccessfulTransition(logger: StateChangeLogger, replicaId: Int, partition: TopicPartition,
                                      currState: ReplicaState, targetState: ReplicaState): Unit = {
    logger.trace(s"Changed state of replica $replicaId for partition $partition from $currState to $targetState")
  }
  // 同样也是记录错误之用，记录一次非法的状态转换。
  private def logInvalidTransition(replica: PartitionAndReplica, targetState: ReplicaState): Unit = {
    val currState = controllerContext.replicaState(replica)
    val e = new IllegalStateException(s"Replica $replica should be in the ${targetState.validPreviousStates.mkString(",")} " +
      s"states before moving to $targetState state. Instead it is in $currState state")
    logFailedStateChange(replica, currState, targetState, e)
  }
  // logFailedStateChange：仅仅是记录一条错误日志，表明执行了一次无效的状态变更。
  private def logFailedStateChange(replica: PartitionAndReplica, currState: ReplicaState, targetState: ReplicaState, t: Throwable): Unit = {
    stateChangeLogger.withControllerEpoch(controllerContext.epoch)
      .error(s"Controller $controllerId epoch ${controllerContext.epoch} initiated state change of replica ${replica.replica} " +
        s"for partition ${replica.topicPartition} from $currState to $targetState failed", t)
  }
}
// ReplicaState接口及其实现对象定义了每种状态的序号，以及合法的前置状态
sealed trait ReplicaState { // 副本状态
  def state: Byte // 状态值
  def validPreviousStates: Set[ReplicaState] // [定义合法的前置状态]： 当前状态之前的合法状态，即：规定哪些状态可以转换成当前状态
}

case object NewReplica extends ReplicaState { // 副本处于新建状态 [副本被创建之后所处的状态]
  val state: Byte = 1
  val validPreviousStates: Set[ReplicaState] = Set(NonExistentReplica)
}

case object OnlineReplica extends ReplicaState { // 副本处于上线状态 [副本正常提供服务时所处的状态]
  val state: Byte = 2
  val validPreviousStates: Set[ReplicaState] = Set(NewReplica, OnlineReplica, OfflineReplica, ReplicaDeletionIneligible)
} // OnlineReplica的validPreviousStates属性是一个集合类型，里面包含NewReplica、OnlineReplica、OfflineReplica和ReplicaDeletionIneligible。这说明，Kafka只允许副本从刚刚这4种状态变更到OnlineReplica状态。如果从ReplicaDeletionStarted状态跳转到OnlineReplica状态，就是非法的状态转换。

case object OfflineReplica extends ReplicaState { // 副本处于下线状态 [副本服务下线时所处的状态]
  val state: Byte = 3
  val validPreviousStates: Set[ReplicaState] = Set(NewReplica, OnlineReplica, OfflineReplica, ReplicaDeletionIneligible)
}

case object ReplicaDeletionStarted extends ReplicaState { // 副本处于开始还未删除的状态 [副本被删除时所处的状态]
  val state: Byte = 4
  val validPreviousStates: Set[ReplicaState] = Set(OfflineReplica)
}

case object ReplicaDeletionSuccessful extends ReplicaState { // 副本处于成功删除状态 [副本被成功删除后所处的状态]
  val state: Byte = 5
  val validPreviousStates: Set[ReplicaState] = Set(ReplicaDeletionStarted)
}

case object ReplicaDeletionIneligible extends ReplicaState { // 副本处于禁止删除状态 [开启副本删除，但副本暂时无法被删除时所处的状态]
  val state: Byte = 6
  val validPreviousStates: Set[ReplicaState] = Set(OfflineReplica, ReplicaDeletionStarted)
}

case object NonExistentReplica extends ReplicaState { // 副本处于不存在状态 [副本从副本状态机被移除前所处的状态]
  val state: Byte = 7
  val validPreviousStates: Set[ReplicaState] = Set(ReplicaDeletionSuccessful)
}
// 一个基本的状态管理流程:
// 当副本对象首次被创建出来后，它会被置于NewReplica状态。经过一番初始化之后，当副本对象能够对外提供服务之后，状态机会将其调整为OnlineReplica，并一直以该状态持续工作。
// 如果副本所在的Broker关闭或者是因为其他原因不能正常工作了，副本需要从OnlineReplica变更为OfflineReplica，表明副本已处于离线状态。
// 一旦开启了如删除主题这样的操作，状态机会将副本状态跳转到ReplicaDeletionStarted，以表明副本删除已然开启。倘若删除成功，则置为ReplicaDeletionSuccessful，倘若不满足删除条件（如所在Broker处于下线状态），那就设置成ReplicaDeletionIneligible，以便后面重试。
// 当副本对象被删除后，其状态会变更为NonExistentReplica，副本状态机将移除该副本数据。
