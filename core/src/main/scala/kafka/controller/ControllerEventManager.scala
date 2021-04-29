/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
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

import java.util.ArrayList
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.{CountDownLatch, LinkedBlockingQueue, TimeUnit}
import java.util.concurrent.locks.ReentrantLock

import kafka.metrics.{KafkaMetricsGroup, KafkaTimer}
import kafka.utils.CoreUtils.inLock
import kafka.utils.ShutdownableThread
import org.apache.kafka.common.utils.Time

import scala.collection._

object ControllerEventManager {
  val ControllerEventThreadName = "controller-event-thread"
  val EventQueueTimeMetricName = "EventQueueTimeMs"
  val EventQueueSizeMetricName = "EventQueueSize"
}
// Controller端的事件处理器接口。支持普通处理和抢占式处理 1、普通处理Controller事件 2、抢占处理Controller事件
trait ControllerEventProcessor { // 目前，在Kafka源码中，KafkaController类是Controller组件的功能实现类，它也是ControllerEventProcessor接口的唯一实现类。
  def process(event: ControllerEvent): Unit // 接收一个Controller事件，并进行处理，是Controller事件处理的主力方法。【重点】1、process方法处理各类Controller事件的代码结构是什么样的？2、准确地找到处理每类事件的子方法。
  def preempt(event: ControllerEvent): Unit // 接收一个Controller事件，并抢占队列之前的事件进行优先处理。【只有两类事件（ShutdownEventThread和Expire）需要抢占式处理】
}
// 每个QueuedEvent对象实例都裹挟了一个ControllerEvent。另外，每个QueuedEvent还定义了process、preempt和awaitProcessing方法，分别表示处理事件、以抢占方式处理事件，以及等待事件处理。其中，process方法和preempt方法的实现原理，就是调用给定ControllerEventProcessor接口的process和preempt方法
class QueuedEvent(val event: ControllerEvent, // ControllerEvent类，表示Controller事件
                  val enqueueTimeMs: Long) {  // 表示Controller事件被放入到事件队列的时间戳
  val processingStarted = new CountDownLatch(1) // 标识事件是否开始被处理；Kafka源码非常喜欢用CountDownLatch来做各种条件控制，比如用于侦测线程是否成功启动、成功关闭，等等。这里是为了确保Expire事件在建立ZooKeeper会话前被处理
  val spent = new AtomicBoolean(false) // 标识事件是否被处理过

  def process(processor: ControllerEventProcessor): Unit = { // 开始处理事件
    if (spent.getAndSet(true)) // 如果事件已经被处理过，直接返回
      return
    processingStarted.countDown() // 标识事件开始处理
    processor.process(event) // 调用ControllerEventProcessor的process方法处理事件
  } // 方法首先会判断该事件是否已经被处理过，如果是，就直接返回；如果不是，就调用ControllerEventProcessor的process方法处理事件。

  def preempt(processor: ControllerEventProcessor): Unit = { // 抢占式处理事件
    if (spent.getAndSet(true))
      return
    processor.preempt(event)
  }

  def awaitProcessing(): Unit = { // 阻塞等待事件处理完成
    processingStarted.await()
  }

  override def toString: String = {
    s"QueuedEvent(event=$event, enqueueTimeMs=$enqueueTimeMs)"
  } // 知识点：1、掌握Controller端处理各类事件的原理；2、提升在实际场景中处理Controller各类问题的能力；
}   // 重点：1、事件队列的实现；2、以及专属线程如何访问事件队列
    // 在0.11.0.0版本之前，Controller组件的源码非常复杂。集群元数据信息在程序中同时被多个线程访问，因此，源码里有大量的Monitor锁、Lock锁或其他线程安全机制，自0.11.0.0版本开始，社区陆续对Controller代码结构进行了改造。其中非常重要的一环，就是将多线程并发访问的方式改为了单线程的事件队列方式。单线程，并非是指Controller只有一个线程了，而是指对局部状态的访问限制在一个专属线程上，即让这个特定线程排他性地操作Controller元数据信息。这样一来，整个组件代码就不必担心多线程访问引发的各种线程安全问题了，源码也可以抛弃各种不必要的锁机制，大大简化了Controller端的代码结构。
class ControllerEventManager(controllerId: Int,
                             processor: ControllerEventProcessor,
                             time: Time,
                             rateAndTimeMetrics: Map[ControllerState, KafkaTimer],
                             eventQueueTimeTimeoutMs: Long = 300000) extends KafkaMetricsGroup { // 单线程事件处理器，就是Controller端定义的一个组件。该组件内置了一个专属线程，负责处理其他线程发送过来的Controller事件。另外，它还定义了一些管理方法，用于为专属线程输送待处理事件。
  import ControllerEventManager._                                                                // 事件处理器，用于创建和管理ControllerEventThread

  @volatile private var _state: ControllerState = ControllerState.Idle
  private val putLock = new ReentrantLock()
  private val queue = new LinkedBlockingQueue[QueuedEvent] // 事件队列; Controller端有多个线程向事件队列写入不同种类的事件; 比如，ZooKeeper端注册的Watcher线程、KafkaRequestHandler线程、Kafka定时任务线程，等等。而在事件队列的另一端，只有一个名为ControllerEventThread的线程专门负责“消费”或处理队列中的事件。这就是所谓的单线程事件队列模型。
  // Visible for test
  private[controller] var thread = new ControllerEventThread(ControllerEventThreadName) // 事件处理线程（单线程）, 线程名：controller-event-thread

  private val eventQueueTimeHist = newHistogram(EventQueueTimeMetricName)

  newGauge(EventQueueSizeMetricName, () => queue.size)

  def state: ControllerState = _state

  def start(): Unit = thread.start()

  def close(): Unit = {
    try {
      thread.initiateShutdown()
      clearAndPut(ShutdownEventThread)
      thread.awaitShutdown()
    } finally {
      removeMetric(EventQueueTimeMetricName)
      removeMetric(EventQueueSizeMetricName)
    }
  }

  def put(event: ControllerEvent): QueuedEvent = inLock(putLock) {
    val queuedEvent = new QueuedEvent(event, time.milliseconds())
    queue.put(queuedEvent) // 插入到事件队列
    queuedEvent
  }

  def clearAndPut(event: ControllerEvent): QueuedEvent = inLock(putLock){
    val preemptedEvents = new ArrayList[QueuedEvent]()
    queue.drainTo(preemptedEvents) // 移除此队列中所有可用的元素，并将它们添加到给定 collection 中。
    preemptedEvents.forEach(_.preempt(processor)) // 优先处理抢占式事件
    put(event) // 调用上面的put方法将给定事件插入到事件队列
  }

  def isEmpty: Boolean = queue.isEmpty

  class ControllerEventThread(name: String) extends ShutdownableThread(name = name, isInterruptible = false) { // 专属的事件处理线程，唯一的作用是处理不同种类的ControllEvent。这个类是ControllerEventManager类内部定义的线程类。
    logIdent = s"[ControllerEventThread controllerId=$controllerId] "

    override def doWork(): Unit = {
      val dequeued = pollFromEventQueue() // 从事件队列中获取待处理的Controller事件，否则等待
      dequeued.event match { // 如果是关闭线程事件，什么都不用做。关闭线程由外部来执行
        case ShutdownEventThread => // The shutting down of the thread has been initiated at this point. Ignore this event.
        case controllerEvent =>
          _state = controllerEvent.state

          eventQueueTimeHist.update(time.milliseconds() - dequeued.enqueueTimeMs) // 更新对应事件在队列中保存的时间

          try {
            def process(): Unit = dequeued.process(processor)

            rateAndTimeMetrics.get(state) match { // 处理事件，同时计算处理速率
              case Some(timer) => timer.time { process() }
              case None => process()
            }
          } catch {
            case e: Throwable => error(s"Uncaught error processing event $controllerEvent", e)
          }

          _state = ControllerState.Idle
      }
    }
  }

  private def pollFromEventQueue(): QueuedEvent = {
    val count = eventQueueTimeHist.count()
    if (count != 0) {
      val event  = queue.poll(eventQueueTimeTimeoutMs, TimeUnit.MILLISECONDS)
      if (event == null) {
        eventQueueTimeHist.clear()
        queue.take()
      } else {
        event
      }
    } else {
      queue.take()
    }
  }

}
