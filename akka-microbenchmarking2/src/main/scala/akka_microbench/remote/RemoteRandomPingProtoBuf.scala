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

import akka.actor.Actor._
import akka.dispatch._

import java.util.Date

import scala.util.Random

import com.example.tutorial.MyProto._
import akka.serialization.RemoteActorSerialization
import akka.actor._
import collection.mutable.{ListBuffer, Buffer}
import scalala.library.Plotting._
import java.io.{FileWriter, File}

;

/**
 * Receives ping messages and sends out another ping, decreasing the hop counter at receive.
 */
class WorkerProtoBuf(coordRef: ActorRef) extends Actor with IpDefinition {

  self.dispatcher = Dispatchers.newThreadBasedDispatcher(self)

  var workers: Array[ActorRef] = Array()

  val mailboxSize: Buffer[Long] = ListBuffer()
  var lastTime = 0L
  var messages = 0L

  def receive = {

    /*case Ping(hops) =>
      if (hops == 0)
        coordRef ! End
      else
        workers(Random.nextInt(numWorkers)) !  Ping(hops - 1)*/
    case ping: Ping =>
      messages += 1

      val hops = ping.getHop

      if (hops == 0)
        coordRef ! End
      else {

        var now = System.nanoTime
        if (now - lastTime > 1000000000) {
          lastTime = now
          mailboxSize += self.dispatcher.mailboxSize(self)
        }

        val msg = Ping.newBuilder.setHop(hops - 1).build

        workers(Random.nextInt(workers.size)) ! msg

      }

    case Workers(x) =>
      workers = x.map(bytes => RemoteActorSerialization.fromBinaryToRemoteActorRef(bytes))

    case End =>
      println("I handled " + messages + " messages")
      coordRef ! MessageCount(messages)
      coordRef ! MailboxSizes(self.uuid.toString, mailboxSize.toList)
      self.stop()
  }

}

/**
 * Coordinates initial ping messages and receive messages from workers when they are finished for time calculation
 */
class MasterProtoBuf(title: String, numMessages: Int, numHops: Int, repetitions: Int) extends Actor with IpDefinition {

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
          workers(i) ! Ping.newBuilder.setHop(numHops).build /*Ping(numHops)*/

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
    val msgPerSecond = totalMessages.doubleValue() / (runs.foldLeft(0L)(_ + _) / 1000 / 1000)
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
object RemoteRandomPingProtoBufWorkerManager extends IpDefinition {

  def start(coord: ActorRef, port: Int = 2552, numWorkers: Int = 4, host: String = "localhost"): Seq[ActorRef] = {

    // start up our remote actor service.
    remote.start("localhost", port)

    // this returns a list of (numWorkers) actors
    val workers = (1 to numWorkers).map {
      i =>
      // create our actor and register it
        remote.register("worker-" + host + "-" + port + "-" + i, actorOf(new WorkerProtoBuf(coord)))
        // look up a RemoteActorRef for the actor we just registered
        // we have to look it up because we need a RemoteActorRef, not a local ActorRef
        remote.actorFor("worker-" + host + "-" + port + "-" + i, host, port)
    }

    println("Actors currently registered: " + registry.actors.map(_.toString()).reduceLeft((a, b) => a + ", " + b))

    // tell the coordinator about these new workers
    coord ! Workers(workers.toArray.map(w => RemoteActorSerialization.toRemoteActorRefProtocol(w).toByteArray))
    // return our workers
    workers
  }
}

object RemoteRandomPingProtoBufMaster {
  def createCoordinator(port: Int, messages: Int, hops: Int, repetitions: Int, title: String = ""): ActorRef = {

    //    println("Workers: " + workers)
    println("Messages: " + messages)
    println("Hops: " + hops)
    println("Repetitions: " + repetitions)

    remote.start("localhost", port)
    // create the master
    val coordService = actorOf(new MasterProtoBuf(title, messages, hops, repetitions))
    remote.register("coord-service", coordService)

    println("Actors currently registered: " + registry.actors.map(_.toString()).reduceLeft((a, b) => a + ", " + b))
    // return an actorRef to the coordService
    coordService
  }
}

object OneVMTest {

  def main(args: Array[String]): Unit = {
    val workers = 8
    val messages = 1000
    val hops = 100
    val repetitions = 3

    val coordRef = RemoteRandomPingProtoBufMaster.createCoordinator(2551, messages, hops, repetitions, "akka 1.3-SNAPSHOT")

    RemoteRandomPingProtoBufWorkerManager.start(coordRef, port = 2551, numWorkers = workers / 2)
    RemoteRandomPingProtoBufWorkerManager.start(coordRef, port = 2551, numWorkers = workers / 2)

    // start the calculation
    coordRef ! Start
  }
}


object MultiVMTestMaster {

  def main(args: Array[String]): Unit = {
    val messages = 10000
    val hops = 100
    val repetitions = 5

    val coordRef = RemoteRandomPingProtoBufMaster.createCoordinator(2551, messages, hops, repetitions)

    RemoteRandomPingProtoBufWorkerManager.start(coordRef, port = 2552, numWorkers = 4)
    RemoteRandomPingProtoBufWorkerManager.start(coordRef, port = 2553, numWorkers = 4)

    println("hit enter once all clients have been started ")
    readLine()

    // start the calculation
    coordRef ! Start
  }
}

object MultiVMTestClient {

  def usage(): String = this.getClass() + " [--help ] [ --port XYZ ] [--masterHost ABC ] [--workers num] "

  def main(args: Array[String]): Unit = {
    val (workers, masterHost, port, shouldExit) = readArgs(args)
    if (shouldExit) return // there was an issue with our arguments; return

    val coordRef = Actor.remote.actorFor("coord-service", masterHost, 2551)

    RemoteRandomPingProtoBufWorkerManager.start(coordRef, port = port, numWorkers = workers)
  }

  def readArgs(args: Array[String]): (Int, String, Int, Boolean) = {
    var port = 2552
    var workers = 4
    var masterHost = "localhost"
    var i = 0
    var skip = false
    for (arg <- args) {
      if (skip) {
        skip = false
      } else {
        arg match {
          case "--help" =>
            println(usage())
            return (workers, masterHost, port, true)

          case "--port" =>
            port = args(i + 1).toInt
            skip = true

          case "--workers" =>
            workers = args(i + 1).toInt
            skip = true

          case "--masterHost" =>
            masterHost = args(i + 1).trim()
            skip = true
        }
      }
      i += 1
    }
    return (workers, masterHost, port, false)
  }
}