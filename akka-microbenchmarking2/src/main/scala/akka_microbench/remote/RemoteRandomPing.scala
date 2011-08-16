/**
 * @author Francisco de Freitas
 *
 * This is a microbenchmark to test Akka
 *
 * The algorithm sends 'ping' messages to all the workers (akka-actors).
 * These then randomly choose a worker to send another 'ping' message to.
 * At each worker, the message hop is decreased until it reaches zero.
 * Each benchmark run ends when all initial messages have reached their maximum number of hops.
 *
 * USAGE: (akka.conf used provided in root folder)
 *
 * Define IPs, start machine 1 first then start machine 0. Machine 1 is the slave, machine 0 is the master
 *
 * Machine 0 is where the master worker stays and is the one collecting results.
 *
 * Both machines have 4 workers each, summing a total of 8 workers
 *
 * machine1$: java -jar test.jar 1
 *
 * machine0$: java -jar test.jar 0
 *
 */
package akka_microbench.remote

import akka.actor.Actor
import Actor._
import akka.actor.ActorRef
import akka.actor.PoisonPill
import akka.actor.ReceiveTimeout
import akka.dispatch._
import akka.dispatch.MailboxType

import java.util.Date
import java.net.InetAddress

import scala.util.Random

sealed trait PingMessage
case object Start extends PingMessage
case class PingMsg(hops: Int) extends PingMessage
case object End extends PingMessage

trait IpDefinition {

  def machine0 = "130.60.157.52"

  def machine1 = "130.60.157.139"
}

/**
 * Receives ping messages and sends out another ping, decreasing the hop counter at receive.
 */
class Worker(coordRef: ActorRef, numWorkers: Int, dispatcher: MessageDispatcher) extends Actor with IpDefinition {

  self.dispatcher = dispatcher //Dispatchers.newThreadBasedDispatcher(self)

  var workers: Array[ActorRef] = new Array[ActorRef](numWorkers)

  override def preStart {

    for (i <- 0 until 4)
      workers(i) = remote.actorFor("worker-service" + i, machine0, 2552)

    for (i <- 4 until 8)
      workers(i) = remote.actorFor("worker-service" + i, machine1, 2552)

  }

  def receive = {

    case PingMsg(hops) =>
      if (hops == 0)
        coordRef ! End
      else
        workers(Random.nextInt(numWorkers)) ! PingMsg(hops - 1)

    case End =>
      self.stop()
  }

}

/**
 * Coordinates initial ping messages and receive messages from workers when they are finished for time calculation
 */
class Master(numWorkers: Int, numMessages: Int, numHops: Int, repetitions: Int) extends Actor with IpDefinition {

  self.dispatcher = Dispatchers.newThreadBasedDispatcher(self)

  var start: Long = 0
  var end: Long = 0
  var receivedEnds: Int = 0
  var reps: Int = 1

  var runs: List[Long] = List()
  var workers: Array[ActorRef] = new Array[ActorRef](numWorkers)

  def receive = {

    case Start =>

      receivedEnds = 0

      // create the workers
      for (i <- 0 until 4)
        workers(i) = remote.actorFor("worker-service" + i, machine0, 2552)

      for (i <- 4 until 8)
        workers(i) = remote.actorFor("worker-service" + i, machine1, 2552)

      println("Master start run #" + reps)

      start = System.nanoTime

      // send to all of the workers 'numMessages' messages
      for (i <- 0 until numWorkers)
        for (j <- 0 until numMessages)
          workers(i) ! PingMsg(numHops)

    case End =>
      receivedEnds += 1

      // all messages have reached 0 hops
      if (receivedEnds == numWorkers * numMessages) {
        end = System.nanoTime

        println("Run #" + reps + " ended! Time = " + ((end - start) / 1000000.0) + "ms")

        runs = (end - start) :: runs

        if (reps != repetitions) {
          reps += 1
          self ! Start
        } else {
          println("Repetitions reached. Broadcasting shutdown...")
          workers.foreach { x => x ! PoisonPill }
          self.stop()
        }
      }

  }

  override def preStart {
    println("Start pinging around @ " + new Date(System.currentTimeMillis))
  }

  override def postStop {
    println("End: " + new Date(System.currentTimeMillis))
    val avg = runs.foldLeft(0l)(_ + _) / runs.size
    println("Average execution time = " + avg / 1000000.0 + " ms")
    System.exit(0)
  }

}
/**
 * Start this using arguments 1 first in one machine
 * Then, start this using arguments 0 in another machine
 *
 * e.g.:
 *
 * machine1$: java -jar test.jar 1 01 a -1
 *
 * machine0$: java -jar test.jar 0 01 a -1
 *
 */
object RemoteRandomPingMachine extends IpDefinition {

  def main(args: Array[String]): Unit = {

    val numWorkersTotal = 8

    remote.start(InetAddress.getLocalHost.getHostAddress, 2552)

    val coord = remote.actorFor("coord-service", machine0, 2552)

    /**
     *
     * Queue01 = withNewThreadPoolWithLinkedBlockingQueueWithUnboundedCapacity
     * Queue02 = withNewThreadPoolWithLinkedBlockingQueueWithCapacity(8)
     * Queue03 = withNewThreadPoolWithLinkedBlockingQueueWithCapacity(128)
     * Queue04 = withNewThreadPoolWithSynchronousQueueWithFairness(true)
     * Queue05 = withNewThreadPoolWithSynchronousQueueWithFairness(false)
     * Queue06 = withNewThreadPoolWithArrayBlockingQueueWithCapacityAndFairness(8, false)
     * Queue07 = withNewThreadPoolWithArrayBlockingQueueWithCapacityAndFairness(8, true)
     *
     */

    var poolSize = 0
    var maxPoolSize = 0
    var throughput = Integer.parseInt(args(3))

    args(2) match {

      case "a" =>
        poolSize = 4
        maxPoolSize = 8

      case "b" =>
        poolSize = 16
        maxPoolSize = 128

    }

    println("Pool size = " + poolSize)
    println("Max size = " + maxPoolSize)
    println("Throughput = " + throughput)

    println("Queue type = " + args(1))

    var sharedDispatcher: MessageDispatcher = null

    if (args(1) equals "01") {

      println("Queue01 = withNewThreadPoolWithLinkedBlockingQueueWithUnboundedCapacity")

      sharedDispatcher = Dispatchers.newExecutorBasedEventDrivenDispatcher("shared executor dispatcher", throughput, UnboundedMailbox())
        .withNewThreadPoolWithLinkedBlockingQueueWithUnboundedCapacity
        .setCorePoolSize(poolSize)
        .setMaxPoolSize(maxPoolSize)
        .build

    } else if (args(1) equals "02") {

      println("Queue02 = withNewThreadPoolWithLinkedBlockingQueueWithCapacity(8)")

      sharedDispatcher = Dispatchers.newExecutorBasedEventDrivenDispatcher("shared executor dispatcher", throughput, UnboundedMailbox())
        .withNewThreadPoolWithLinkedBlockingQueueWithCapacity(8)
        .setCorePoolSize(poolSize)
        .setMaxPoolSize(maxPoolSize)
        .build

    } else if (args(1) equals "03") {

      println("Queue03 = withNewThreadPoolWithLinkedBlockingQueueWithCapacity(128)")

      sharedDispatcher = Dispatchers.newExecutorBasedEventDrivenDispatcher("shared executor dispatcher", throughput, UnboundedMailbox())
        .withNewThreadPoolWithLinkedBlockingQueueWithCapacity(100)
        .setCorePoolSize(poolSize)
        .setMaxPoolSize(maxPoolSize)
        .build

    } else if (args(1) equals "04") {

      println("Queue04 = withNewThreadPoolWithSynchronousQueueWithFairness(true)")

      sharedDispatcher = Dispatchers.newExecutorBasedEventDrivenDispatcher("shared executor dispatcher", throughput, UnboundedMailbox())
        .withNewThreadPoolWithSynchronousQueueWithFairness(true)
        .setCorePoolSize(poolSize)
        .setMaxPoolSize(maxPoolSize)
        .build

    } else if (args(1) equals "05") {

      println("Queue05 = withNewThreadPoolWithSynchronousQueueWithFairness(false)")

      sharedDispatcher = Dispatchers.newExecutorBasedEventDrivenDispatcher("shared executor dispatcher", throughput, UnboundedMailbox())
        .withNewThreadPoolWithSynchronousQueueWithFairness(false)
        .setCorePoolSize(poolSize)
        .setMaxPoolSize(maxPoolSize)
        .build

    } else if (args(1) equals "06") {

      println("Queue06 = withNewThreadPoolWithArrayBlockingQueueWithCapacityAndFairness(8, false)")

      sharedDispatcher = Dispatchers.newExecutorBasedEventDrivenDispatcher("shared executor dispatcher", throughput, UnboundedMailbox())
        .withNewThreadPoolWithArrayBlockingQueueWithCapacityAndFairness(8, false)
        .setCorePoolSize(poolSize)
        .setMaxPoolSize(maxPoolSize)
        .build

    } else if (args(1) equals "07") {

      println("Queue07 = withNewThreadPoolWithArrayBlockingQueueWithCapacityAndFairness(8, true)")

      sharedDispatcher = Dispatchers.newExecutorBasedEventDrivenDispatcher("shared executor dispatcher", throughput, UnboundedMailbox())
        .withNewThreadPoolWithArrayBlockingQueueWithCapacityAndFairness(8, true)
        .setCorePoolSize(poolSize)
        .setMaxPoolSize(maxPoolSize)
        .build
    }

    /** MACHINE 0 */
    if (args(0) equals "0") {

      remote.register("worker-service0", actorOf(new Worker(coord, numWorkersTotal, sharedDispatcher)))
      remote.register("worker-service1", actorOf(new Worker(coord, numWorkersTotal, sharedDispatcher)))
      remote.register("worker-service2", actorOf(new Worker(coord, numWorkersTotal, sharedDispatcher)))
      remote.register("worker-service3", actorOf(new Worker(coord, numWorkersTotal, sharedDispatcher)))

      val workers = 8
      val messages = 10000
      val hops = 100
      val repetitions = 5

      println("Workers: " + workers)
      println("Messages: " + messages)
      println("Hops: " + hops)
      println("Repetitions: " + repetitions)

      // create the master
      remote.register("coord-service", actorOf(new Master(workers, messages, hops, repetitions)))

      // start the calculation
      coord ! Start

    } else {

      /** MACHINE 1 */

      remote.register("worker-service4", actorOf(new Worker(coord, numWorkersTotal, sharedDispatcher)))
      remote.register("worker-service5", actorOf(new Worker(coord, numWorkersTotal, sharedDispatcher)))
      remote.register("worker-service6", actorOf(new Worker(coord, numWorkersTotal, sharedDispatcher)))
      remote.register("worker-service7", actorOf(new Worker(coord, numWorkersTotal, sharedDispatcher)))
    }

  }

}