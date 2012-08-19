package play2.tools.file

import play.api.libs.concurrent.Akka
import akka.actor.{Actor, Props}
import akka.util.duration._
import java.nio.file.{Path, FileSystems}
import collection.JavaConversions._
import play.api.libs.iteratee._
import play.api.libs.iteratee.Concurrent._
import java.util.concurrent.{ExecutorService}
import scala.concurrent.ExecutionContext

import play.api.Play.current

import play.api.{Application, Plugin, Logger, PlayException}

/**
 * Plugin managing the WatchActor in Play app.
 */
class WatchPlugin(app: Application) extends Plugin {
  import play2.tools.file.PlayImplicits._

  lazy val watchActor = {
    Logger("play").info("Starting Watch Actor.")
    WatchActor()
  }

  override def onStart() {
    watchActor.start
  }

  override def onStop() {
    watchActor.stop
  }

}

object WatchPlugin {

  def watchActor(implicit app :Application) = {
    current.watchActor
  }

  /** 
    * returns the current instance of the plugin. 
    */
  def current(implicit app :Application): WatchPlugin = app.plugin[WatchPlugin] match {
    case Some(plugin) => plugin
    case _ => throw PlayException("WatchPlugin Error", "The WatchPlugin has not been initialized! Please edit your conf/play.plugins file and add the following line: '400:play.modules.mongodb.WatchPlugin' (400 is an arbitrary priority and may be changed to match your needs).")
  }
  
}

sealed trait WatchActor {
  def enumerator: Enumerator[Array[Byte]]
  def register(dirPath: Path)
  def start(implicit ctx: ExecutionContext, ex: ExecutorService)
  def stop
}

object WatchActor {
  
  def apply(): WatchActor = {
    val osName = System.getProperty("os.name")

    if(osName.toLowerCase.contains("mac")) {
      MacWatchActor
    } else {
      DefaultWatchActor
    }
  }

}

sealed trait Msg 
case object Tick extends Msg
//case class Register(watchKey: WatchKey, path: Path) extends Msg
case class LaunchRead(path: Path) extends Msg

case object DefaultWatchActor extends WatchActor {
  import java.nio.file.{WatchService, WatchKey, StandardWatchEventKinds}

  private var watchService: Option[WatchService] = None
  private var channel: Option[Channel[Array[Byte]]] = None
  private var scheduler: Option[akka.actor.Cancellable] = None
  private var readActor: Option[akka.actor.ActorRef] = None
  private var watchActor: Option[akka.actor.ActorRef] = None

  override val enumerator = unicast[Array[Byte]](
    onStart = { c => channel = Some(c) },
    onComplete = { },
    onError = { (s, e) => ()}
  )

  override def start(implicit ctx: ExecutionContext, ex: ExecutorService) = {
    watchService = Some(newWatchService)
    readActor = Some(Akka.system.actorOf(Props(new Actor {

      var files: Map[Path, FileChannel] = Map()
      var positions: Map[Path, Long] = Map()

      def receive = {
        case LaunchRead(path) =>
          files.get(path).orElse{ 
            val f = FileChannel.open(path)
            files += path -> f
            positions += path -> 0L
            Some(f)
          }.map { f =>
            // gets lines only
            // pushes to channel
            channel.map{ channel => 
              val pos = positions(path)
              f.enumeratorFrom(pos) |>>> Iteratee.foreach{ t: Array[Byte] => 
                positions += path -> (pos + t.size)
                channel.push(t)
              } 
            }
          }
      }
    })))

    watchActor = Some(Akka.system.actorOf(Props(new Actor {
      //var map = Map[WatchKey, Path]()

      def receive = {
        case Tick =>
          if(!watchService.isDefined) println("No WatchService")
          watchService.foreach{ ws => 
            Option(ws.poll).map{ key => 
              val events = key.pollEvents

              events.toList.foreach{ t => 
                t.kind match {
                  case StandardWatchEventKinds.ENTRY_MODIFY => 
                    readActor.map( _ ! LaunchRead(t.context.asInstanceOf[Path]) )
                  case _ =>
                }
              }

              key.reset
            }
          }

        //case Register(watchKey: WatchKey, path: Path) => 
        // println("registered %s %s".format(watchKey, path))
        // map += watchKey -> path
      }
    })))

    watchActor.map{ actor => 
      scheduler = Some(Akka.system.scheduler.schedule(0 milliseconds,
        1000 milliseconds,
        actor,
        Tick
      ))
    }

    println("WatchActor started")
  }

  override def register(path: Path) = {
    watchService.map{ ws =>
      val watchKey = path.register(ws, StandardWatchEventKinds.ENTRY_MODIFY)
      watchKey.reset
    }
  }

  override def stop = {
    watchService.map(_.close)
    scheduler.map(_.cancel())
    Akka.system.shutdown()
    Akka.system.awaitTermination()
    println("Stopped WatchActor")
  }

  private def newWatchService = {
    FileSystems.getDefault.newWatchService
  }

}


case object MacWatchActor extends WatchActor {
  import com.barbarysoftware.watchservice.{WatchService, StandardWatchEventKinds, Utils}

  private var watchService: Option[WatchService] = None
  private var channel: Option[Channel[Array[Byte]]] = None
  private var scheduler: Option[akka.actor.Cancellable] = None
  private var readActor: Option[akka.actor.ActorRef] = None
  private var watchActor: Option[akka.actor.ActorRef] = None

  override def register(path: Path) = {
    watchService.foreach{ ws => 
      val watchKey = Utils.register(path, ws, StandardWatchEventKinds.ENTRY_MODIFY)
      watchKey.reset
      println("registered %s".format(path))
    }
  }

  override val enumerator = unicast[Array[Byte]](
    onStart = { c => channel = Some(c) },
    onComplete = { },
    onError = { (s, e) => ()}
  )

  override def start(implicit ctx: ExecutionContext, ex: ExecutorService) = {
    watchService = Some(newWatchService)
    
    readActor = Some(Akka.system.actorOf(Props(new Actor {

      var files: Map[Path, FileChannel] = Map()
      var positions: Map[Path, Long] = Map()

      def receive = {
        case LaunchRead(path) =>
          files.get(path).orElse{ 
            val f = FileChannel.open(path)
            files += path -> f
            positions += path -> 0L
            Some(f)
          }.map { f =>
            // gets lines only
            // pushes to channel
            println("Reading %s".format(path))
            channel.map{ channel => 
              val pos = positions(path)
              f.enumeratorFrom(pos) |>>> Iteratee.foreach { t: Array[Byte] => 
                positions += path -> (pos + t.size)
                channel.push(t)
              } 
            }
          }
      }
    })))

    watchActor = Some(Akka.system.actorOf(Props(new Actor {
      //var map = Map[WatchKey, Path]()

      def receive = {
        case Tick =>
          if(!watchService.isDefined) println("No WatchService")
          watchService.foreach{ ws => 
            Option(ws.poll).map{ key => 
              val events = key.pollEvents

              events.toList.foreach{ t => 
                t.kind match {
                  case StandardWatchEventKinds.ENTRY_MODIFY => 
                    readActor.map( _ ! LaunchRead(t.context.asInstanceOf[Path]) )
                  case _ =>
                }
              }

              key.reset
            }
          }

        //case Register(watchKey: WatchKey, path: Path) => 
        // println("registered %s %s".format(watchKey, path))
        // map += watchKey -> path
      }
    })))

    watchActor.map { actor =>
      scheduler = Some(Akka.system.scheduler.schedule(0 milliseconds,
        1000 milliseconds,
        actor,
        Tick
      ))
    }

    println("Started WatchActor")
  }

  override def stop = {
    watchService.map(_.close)
    scheduler.map( _.cancel() )
    Akka.system.shutdown()
    Akka.system.awaitTermination()
    println("Stopped WatchActor")
  }


  
  private def newWatchService(implicit es: ExecutorService) = {
    import com.barbarysoftware.watchservice.{MacOSXPollingWatchService, MacOSXListeningWatchService}
    
    val osVersion = System.getProperty("os.version")
    val osName = System.getProperty("os.name")
    
    println("Detected osName:%s osVersion:%s".format(osName, osVersion))
    if(osName.toLowerCase.contains("mac")) {
      val minorVersion = Integer.parseInt(osVersion.split("\\.")(1))
      if (minorVersion < 5) {
          // for Mac OS X prior to Leopard (10.5)
          new MacOSXPollingWatchService
      } else {
          // for Mac OS X Leopard (10.5) and upwards
          //new MacOSXListeningWatchService
          play2.tools.watchservice.MacWatchService(es)
      }
    }else {
      //FileSystems.getDefault.newWatchService
      throw new RuntimeException("Mac Watch Actor works only for MAC")
    } 
  }

}

