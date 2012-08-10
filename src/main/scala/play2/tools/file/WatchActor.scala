package play2.tools.file

import play.api.libs.concurrent.Akka
import akka.actor.{Actor, Props}
import akka.util.duration._
import java.nio.file.{Path, FileSystems}
import collection.JavaConversions._
import play.api.libs.iteratee._
import play.api.libs.iteratee.Concurrent._

import play.api.Play.current

import play2.tools.iteratee.RichEnumerator

import play.api.{Application, Plugin, Logger, PlayException}

/**
 * Plugin managing the WatchActor in Play app.
 */
class WatchPlugin(app: Application) extends Plugin {

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

  def watchActor = current.watchActor

  /** 
    * returns the current instance of the plugin. 
    */
  def current(implicit app :Application): WatchPlugin = current(app)

  /** 
   * returns the current instance of the plugin (from a [[play.Application]] - Scala's [[play.api.Application]] equivalent for Java). 
   */
  def current(app :play.Application): WatchPlugin = app.plugin(classOf[WatchPlugin]) match {
    case plugin if plugin != null => plugin
    case _ => throw PlayException("WatchPlugin Error", "The WatchPlugin has not been initialized! Please edit your conf/play.plugins file and add the following line: '400:play.modules.mongodb.WatchPlugin' (400 is an arbitrary priority and may be changed to match your needs).")
  }
}

sealed trait WatchActor {
  def enumerator: Enumerator[Array[Byte]]
  def register(dirPath: Path)
  def start
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

  private val watchService: WatchService = newWatchService
  private var channel: Option[Channel[Array[Byte]]] = None
  private var scheduler: Option[akka.actor.Cancellable] = None
  private var readActor: Option[akka.actor.ActorRef] = None
  private var watchActor: Option[akka.actor.ActorRef] = None

  override val enumerator = unicast[Array[Byte]](
    onStart = { c => channel = Some(c) },
    onComplete = { },
    onError = { (s, e) => ()}
  )

  override def start = {
    readActor = Some(Akka.system.actorOf(Props(new Actor {
      import play2.tools.file.DefaultImplicits._

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
              f.enumerateFrom(pos).foreach{ t: Array[Byte] => 
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
          //println("HELLO")
          Option(watchService.poll).map{ key => 
            val events = key.pollEvents

            events.toList.foreach{ t => 
              t.kind match {
                case StandardWatchEventKinds.ENTRY_MODIFY => 
                println("detected modif on "+t.context.asInstanceOf[Path])
                  readActor.map( _ ! LaunchRead(t.context.asInstanceOf[Path]) )
                case _ =>
              }
            }

            key.reset
          }

        //case Register(watchKey: WatchKey, path: Path) => 
        // println("registered %s %s".format(watchKey, path))
        // map += watchKey -> path
      }
    })))

    watchActor.map{ actor => 
      scheduler = Some(Akka.system.scheduler.schedule(0 milliseconds,
        500 milliseconds,
        actor,
        Tick
      ))
    }
  }

  override def register(path: Path) = {
    val watchKey = path.register(watchService, StandardWatchEventKinds.ENTRY_MODIFY)
    watchKey.reset
  }

  override def stop = {
    watchService.close()
    scheduler.map(_.cancel())
    Akka.system.shutdown()
    Akka.system.awaitTermination()
  }

  private def newWatchService = {
    FileSystems.getDefault.newWatchService
  }

}


case object MacWatchActor extends WatchActor {
  import com.barbarysoftware.watchservice.{WatchService, StandardWatchEventKinds, Utils}

  private val watchService: WatchService = newWatchService
  private var channel: Option[Channel[Array[Byte]]] = None
  private var scheduler: Option[akka.actor.Cancellable] = None
  private var readActor: Option[akka.actor.ActorRef] = None
  private var watchActor: Option[akka.actor.ActorRef] = None

  override def register(path: Path) = {
    val watchKey = Utils.register(path, watchService, StandardWatchEventKinds.ENTRY_MODIFY)
    watchKey.reset
  }

  override val enumerator = unicast[Array[Byte]](
    onStart = { c => channel = Some(c) },
    onComplete = { },
    onError = { (s, e) => ()}
  )

  override def start = {
    readActor = Some(Akka.system.actorOf(Props(new Actor {
      import play2.tools.file.DefaultImplicits._

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
              f.enumerateFrom(pos).foreach{ t: Array[Byte] => 
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
          //println("HELLO")
          Option(watchService.poll).map{ key => 
            val events = key.pollEvents

            events.toList.foreach{ t => 
              t.kind match {
                case StandardWatchEventKinds.ENTRY_MODIFY => 
                println("detected modif on "+t.context.asInstanceOf[Path])
                  readActor.map( _ ! LaunchRead(t.context.asInstanceOf[Path]) )
                case _ =>
              }
            }

            key.reset
          }

        //case Register(watchKey: WatchKey, path: Path) => 
        // println("registered %s %s".format(watchKey, path))
        // map += watchKey -> path
      }
    })))

    watchActor.map { actor =>
      scheduler = Some(Akka.system.scheduler.schedule(0 milliseconds,
        500 milliseconds,
        actor,
        Tick
      ))
    }
  }

  override def stop = {
    watchService.close()
    scheduler.map( _.cancel() )
    Akka.system.shutdown()
    Akka.system.awaitTermination()
  }


  
  private def newWatchService = {
    import com.barbarysoftware.watchservice.{MacOSXPollingWatchService, MacOSXListeningWatchService}
    
    val osVersion = System.getProperty("os.version")
    val osName = System.getProperty("os.name")
    
    println("osName:%s osVersion:%s".format(osName, osVersion))
    if(osName.toLowerCase.contains("mac")) {
      val minorVersion = Integer.parseInt(osVersion.split("\\.")(1))
      if (minorVersion < 5) {
          // for Mac OS X prior to Leopard (10.5)
          new MacOSXPollingWatchService
      } else {
          // for Mac OS X Leopard (10.5) and upwards
          new MacOSXListeningWatchService
      }
    }else {
      //FileSystems.getDefault.newWatchService
      throw new RuntimeException("Mac Watch Actor works only for MAC")
    } 
  }

}

