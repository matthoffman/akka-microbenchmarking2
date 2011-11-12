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
 */
package akka_microbench.remote

import akka.actor.Actor
import Actor._
import akka.actor.ActorRef
import akka.dispatch._
import java.util.Date
import akka.serialization.RemoteActorSerialization
import scalala.library.Plotting._
import java.io.{FileWriter, File}
import akka_microbench.local.Ping
import collection.mutable.{ListBuffer, Buffer}

sealed trait PingMessage

case object Start extends PingMessage

case class PingMsg(hops: Int) extends PingMessage

case class Workers(actorRefs: Array[Array[Byte]]) extends PingMessage

case object End extends PingMessage

case class MessageCount(count: Long) extends PingMessage

case class MailboxSizes(workerId: String, sizes: List[Long]) extends PingMessage

/*trait Unbound extends MessageQueue { self: LinkedTransferQueue[MessageInvocation] =>
  @inline
  final def enqueue(handle: MessageInvocation): Unit = this add handle
  @inline
  final def dequeue(): MessageInvocation = this.poll()
}*/

trait IpDefinition {

  def machine0 = "127.0.0.1"

  def machine1 = "127.0.0.1"
}

/**
 * Receives ping messages and sends out another ping, decreasing the hop counter at receive.
 */
class Worker(id: Int, coordRef: ActorRef, numWorkers: Int, numMessages: Int, numHops: Int, dispatcher: MessageDispatcher) extends Actor with IpDefinition {

  self.dispatcher = dispatcher

  var workers: Array[ActorRef] = new Array[ActorRef](numWorkers)
  val mailboxSize: Buffer[Long] = ListBuffer()

  var lastTime = 0l
  var messages = 0L
  //
  //  override def preStart {
  //
  //    for (i <- 0 until numWorkers) {
  //      if (i % 2 == 0) {
  //        workers(i) = remote.actorFor("worker-service" + i, machine0, 2552)
  //      } else {
  //        workers(i) = remote.actorFor("worker-service" + i, machine1, 2553)
  //      }
  //    }
  //
  //
  //  }

  def receive = {

    case Start =>
      for (i <- 0 until numMessages)
        self ! PingMsg(numHops)

    case PingMsg(hops) =>
      messages += 1

      if (hops == 0)
        coordRef ! End
      else {

        var now = System.nanoTime

        if (now - lastTime > 1000000000) {
          println(dispatcher.mailboxSize(self))
          lastTime = System.nanoTime
          println("h:" + hops)
        }

        /*if (id < 4)
          workers(Random.nextInt(3) + 4) ! PingMsg(hops - 1)
        else
          workers(Random.nextInt(3)) ! PingMsg(hops - 1)*/

        val nextWorker = numWorkers - id - 1
        workers(nextWorker) ! PingMsg(hops - 1)
      }

    case Workers(x) =>
      workers = x.map(bytes => RemoteActorSerialization.fromBinaryToRemoteActorRef(bytes))

    case End =>
      println("worker " + id + " handled " + messages + " messages")
      coordRef ! MessageCount(messages)
      coordRef ! MailboxSizes(self.uuid.toString, mailboxSize.toList)
      self.stop()
  }

}

/**
 * Coordinates initial ping messages and receive messages from workers when they are finished for time calculation
 */

/**
 * Coordinates initial ping messages and receive messages from workers when they are finished for time calculation
 */
class Master(title: String, numMessages: Int, numHops: Int, repetitions: Int) extends Actor with IpDefinition {

  self.dispatcher = Dispatchers.newThreadBasedDispatcher(self)

  var start: Long = 0
  var end: Long = 0
  var receivedEnds: Int = 0
  var reps: Int = 1
  var totalMessages = 0L
  var runs: List[Long] = List()
  var workers: Array[ActorRef] = Array[ActorRef]()
  var numClients = 0
  var receivedConcludingResults = 0

  def sendMessageToWorker(worker: ActorRef) {
    worker ! Ping(numHops)
  }

  def receive = {

    case Workers(w) =>
      workers ++= w.map(bytes => RemoteActorSerialization.fromBinaryToRemoteActorRef(bytes))
      numClients += 1 // we're assuming that we'll get a list of workers once per connected client

    case Start =>

      receivedEnds = 0

      //      // create the workers
      //      for (i <- 0 until 4)
      //        workers(i) = remote.actorFor("worker-service" + i, machine0, 2552)
      //
      //      for (i <- 4 until 8)
      //        workers(i) = remote.actorFor("worker-service" + i, machine1, 2552)

      println("Master start run #" + reps)

      start = System.nanoTime

      // let each worker know about all the other workers.
      println("Known workers: " + workers.map(w => w.toString()).reduceLeft((acc, n) => acc + ", " + n))

      for (i <- 0 until workers.size)
        workers(i) ! Workers(workers.map(w => RemoteActorSerialization.toRemoteActorRefProtocol(w).toByteArray))

      // send to all of the workers 'numMessages' messages
      for (i <- 0 until workers.size)
        for (j <- 0 until numMessages)
          sendMessageToWorker(workers(0))

    case End =>
      receivedEnds += 1

      // all messages have reached 0 hops
      if (receivedEnds == workers.size * numMessages) {
        end = System.nanoTime

        println("Run #" + reps + " ended! Time = " + ((end - start) / 1000000.0) + "ms")

        runs = (end - start) :: runs

        if (reps != repetitions) {
          reps += 1
          self ! Start
        } else {
          println("Repetitions reached. Broadcasting shutdown...")
          workers.foreach {
            x => x ! End
          }
          // we don't want to stop ourselves; workers are going to send us summaries
          //          self.stop()
        }
      }

    case MessageCount(count) =>
      totalMessages += count
      println("total messages: " + totalMessages)

    case MailboxSizes(senderId, sizes) =>
      plot(sizes.indices.toArray, sizes.toArray)
      plot.hold
      xlabel("time")
      ylabel("mailbox size")
      saveas("mailbox_size_" + senderId + "_" + System.currentTimeMillis() + ".png")
      receivedConcludingResults += 1
      if (receivedConcludingResults >= workers.size) {
        // we've gotten an update from everyone we expect
        self.stop()
      }
  }

  override def preStart {
    println("Start pinging around @ " + new Date(System.currentTimeMillis))
  }

  /**
   * Write out a header for our csv file of results
   */
  def writeHeader(file: File) {
    val writer = new FileWriter(file, true)
    try {
      writer.write("title,startTime,numMessages,numHops,repetitions,clients,totalWorkers,avgTime,minTime,maxTime,medianTime,msgPerSecond\n")
    } finally {
      writer.close()
    }
  }

  /**
   * Write out a CSV file with our results, so we can chart results over time
   */
  def writeResultFile(avg: Long, msgPerSecond: Double) {
    println("writing results to results.csv")
    val outFile = new File("results.csv")
    if (!outFile.exists()) writeHeader(outFile)

    val writer = new FileWriter(outFile, true)
    try {
      //startTime,numMessages,numHops,repetitions,clients,totalWorkers,avgTime,minTime,maxTime,medianTime
      writer.write(title + "," + start + "," + numMessages + "," + numHops + "," + repetitions + "," + numClients + "," + workers.size + "," + avg + "," + runs.min + "," + runs.max + "," + median(runs) + "," + msgPerSecond + "\n")
    } finally {
      writer.close()
    }
  }

  override def postStop {
    println("End: " + new Date(System.currentTimeMillis))
    val avg = runs.foldLeft(0L)(_ + _) / runs.size
    println("Average execution time = " + avg / 1000000.0 + " ms")
    val msgPerSecond = totalMessages.doubleValue() / (runs.foldLeft(0L)(_ + _) / 1000000000 /* nanoseconds in a second */)
    println("Total messages per second = " + msgPerSecond)

    writeResultFile(avg, msgPerSecond)

    System.exit(0)
  }

  def median(s: Seq[Long]) = {
    val (lower, upper) = s.sortWith(_ < _).splitAt(s.size / 2)
    if (s.size % 2 == 0) (lower.last + upper.head) / 2 else upper.head
  }


}

/**
 * Creates workers for one machine
 */
object RemoteRandomPingWorkerManager extends IpDefinition {

  def start(coord: ActorRef, port: Int = 2552, numWorkers: Int = 4, host: String = "localhost"): Seq[ActorRef] = {

    // start up our remote actor service.
    remote.start("localhost", port)

    // this returns a list of (numWorkers) actors
    (1 to numWorkers).map {
      i =>
        val newWorker = actorOf(new WorkerProtoBuf(coord))
        remote.register("worker-" + host + "-" + port + "-" + i, newWorker)
        newWorker
    }
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

    if (args(0).equals("0")) {
      remote.start("localhost", 2552)
    } else {
      remote.start("localhost", 2553)
    }

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

    /*var sharedDispatcher: MessageDispatcher = null
    sharedDispatcher = new ExecutorBasedEventDrivenDispatcher("shared executor dispatcher", -1, UnboundedMailbox()) {

      val self = this

      override def createMailbox(actorRef: ActorRef): AnyRef = mailboxType match {
        case b: UnboundedMailbox => new LinkedTransferQueue[MessageInvocation] with Unbound with ExecutableMailbox {
          def dispatcher = self
        }
        case _ => super.createMailbox(actorRef)
      }

    }*/

    val workers = 8
    val messages = 10000
    val hops = 100
    val repetitions = 3

    /**MACHINE 0 */
    if (args(0) equals "0") {

      for (i <- 0 until workers) {
        if (i % 2 == 0) {
          remote.register("worker-service" + i, actorOf(new Worker(i, coord, numWorkersTotal, messages, hops, sharedDispatcher)))
        }
      }

      println("Workers: " + workers)
      println("Messages: " + messages)
      println("Hops: " + hops)
      println("Repetitions: " + repetitions)

      // create the master
      remote.register("coord-service", actorOf(new Master("remote no protobuf 1.3", messages, hops, repetitions)))

      // start the calculation
      coord ! Start

    } else {

      /**MACHINE 1 */

      for (i <- 0 until workers) {
        if (i % 2 == 1) {
          remote.register("worker-service" + i, actorOf(new Worker(i, coord, numWorkersTotal, messages, hops, sharedDispatcher)))
        }
      }
    }
  }
}