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
import akka_microbench.local.Ping._

;

/**
 * Receives ping messages and sends out another ping, decreasing the hop counter at receive.
 */
class WorkerProtoBuf(coordRef: ActorRef) extends Actor with IpDefinition {

//  self.dispatcher = Dispatchers.newThreadBasedDispatcher(self)
  self.dispatcher = Dispatcher.dispatcher

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

class MasterProtobuf(title: String, numMessages: Int, numHops: Int, repetitions: Int)
        extends Master(title, numMessages, numHops, repetitions)  {

  override def sendMessageToWorker(worker: ActorRef) {
    worker ! Ping.newBuilder().setHop(numHops).build()
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
    val coordService = actorOf(new MasterProtobuf(title, messages, hops, repetitions) )
    remote.register("coord-service", coordService)

    println("Actors currently registered: " + registry.actors.map(_.toString()).reduceLeft((a, b) => a + ", " + b))
    // return an actorRef to the coordService
    coordService
  }
}

object Dispatcher {
  val dispatcher = Dispatchers.newExecutorBasedEventDrivenDispatcher("dispatcher")
          .withNewThreadPoolWithLinkedBlockingQueueWithUnboundedCapacity
          .build
}

object OneVMTest {

  def main(args: Array[String]): Unit = {
    val workers = 64
    val messages = 1000
    val hops = 100
    val repetitions = 3
    val coordRef = RemoteRandomPingProtoBufMaster.createCoordinator(2551, messages, hops, repetitions, "akka 1.3-SNAPSHOT eventy1")

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