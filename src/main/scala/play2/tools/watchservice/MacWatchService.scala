package play2.tools.watchservice

import com.barbarysoftware.watchservice._
import java.nio.file.{Path, Files, Paths}
import java.nio.file.attribute.FileTime
import java.io.IOException
import com.sun.jna.NativeLong
import com.sun.jna.Pointer


import play.api.Play.current
import play.api.libs.concurrent.Akka
import akka.actor.{Actor, Props}
import akka.util.duration._

import com.barbarysoftware.jna._

import scala.collection.JavaConverters._
import java.util.concurrent.ExecutorService

sealed trait Msg 
case object Scan extends Msg


object MacWatchService {
  def apply(implicit executor: ExecutorService) = new MacWatchService(executor)
}

/**
 * Scala implementation of JDK WatchService
 */
class MacWatchService(executor: ExecutorService) extends AbstractWatchService {
  var runnables: List[CarbonRunnable] = List()
  
  class CarbonRunnable(_stream: FSEventStreamRef) extends java.lang.Runnable {
    lazy val runLoop = CarbonAPI.INSTANCE.CFRunLoopGetCurrent()
    lazy val stream = _stream
    override def run():Unit = {
      val runLoopMode = CFStringRef.toCFString("kCFRunLoopDefaultMode")
      CarbonAPI.INSTANCE.FSEventStreamScheduleWithRunLoop(stream, runLoop, runLoopMode)
      CarbonAPI.INSTANCE.FSEventStreamStart(stream)
      CarbonAPI.INSTANCE.CFRunLoopRun()
    }
  }

  override def register(path: Path,
    events: Array[WatchEvent.Kind[_]] ,
    modifers: WatchEvent.Modifier*): WatchKey = {

    val file = path.toFile()
    var lastModifiedMap = createLastModifiedMap(path)
    val s = path.toAbsolutePath().toString()
    val values = Array(CFStringRef.toCFString(s).getPointer())
    val pathsToWatch = CarbonAPI.INSTANCE.CFArrayCreate(null, values, CFIndex.valueOf(1), null)
    val watchKey = new MacOSXWatchKey(this, new WatchablePath(path), events)

    val latency = 1.0 /* Latency in seconds */
    val kFSEventStreamEventIdSinceNow = -1L //  this is 0xFFFFFFFFFFFFFFFF
    val kFSEventStreamCreateFlagNoDefer = 0x00000002
    val callback = new MacOSXListeningCallback(watchKey, lastModifiedMap)
    //callbackList.add(callback)
 
    val stream = CarbonAPI.INSTANCE.FSEventStreamCreate(
      Pointer.NULL,
      callback,
      Pointer.NULL,
      pathsToWatch,
      kFSEventStreamEventIdSinceNow,
      latency,
      kFSEventStreamCreateFlagNoDefer
    )

    val runnable = new CarbonRunnable(stream)
    executor.submit(runnable)

    runnables = runnables :+ runnable

    watchKey
  }

  def createLastModifiedMap(path: Path): Map[Path, FileTime] = {
    var lastModifiedMap = Map[Path, FileTime]()
    try {
      recursiveListFiles(path).foreach{ child =>
        lastModifiedMap += child.toRealPath() -> Files.getLastModifiedTime(child)
      }
    } catch {
      case ex: IOException =>
        ex.printStackTrace()
    }

    lastModifiedMap
  }

    def recursiveListFiles(path: Path): Set[Path] = {
        var paths = Set[Path]()
        paths += path
        try {
            if (Files.isDirectory(path)) {
                for (child <- Files.newDirectoryStream(path).asScala) {
                    paths ++= recursiveListFiles(child)
                }
            }
        } catch {
          case ex: IOException =>
            ex.printStackTrace()
        }
        paths
    }

    @throws(classOf[IOException])
    override def implClose() = {
      runnables.foreach { r =>
        CarbonAPI.INSTANCE.CFRunLoopStop(r.runLoop)
        CarbonAPI.INSTANCE.FSEventStreamStop(r.stream)
      }
      executor.shutdownNow()

        /*for (CFRunLoopThread thread : threadList) {
            CarbonAPI.INSTANCE.CFRunLoopStop(thread.getRunLoop());
            CarbonAPI.INSTANCE.FSEventStreamStop(thread.getStreamRef());
        }*/
        /*threadList.clear();
        callbackList.clear();*/
    }

    class MacOSXListeningCallback(
      val watchKey: MacOSXWatchKey,
      var lastModifiedMap: Map[Path, FileTime] 
    ) extends CarbonAPI.FSEventStreamCallback {

      def invoke(streamRef: FSEventStreamRef, 
            clientCallBackInfo: Pointer, 
            numEvents: NativeLong, 
            eventPaths: Pointer, 
            /* array of unsigned int */ eventFlags: Pointer, 
            /* array of unsigned long */ eventIds: Pointer) = {
            val length = numEvents.intValue()
            try {
                for (folderName <- eventPaths.getStringArray(0, length)) {
                    val filesOnDisk = recursiveListFiles(Paths.get(folderName))
                    
                    val createdFiles = findCreatedFiles(filesOnDisk)
                    val modifiedFiles = findModifiedFiles(filesOnDisk)
                    val deletedFiles = findDeletedFiles(folderName, filesOnDisk)

                    for (path <- createdFiles) {
                      if (watchKey.isReportCreateEvents()) {
                          watchKey.signalEvent(StandardWatchEventKinds.ENTRY_CREATE, path)
                      }
                      lastModifiedMap += path -> Files.getLastModifiedTime(path)
                    }

                    for (path <- modifiedFiles) {
                      println("detected modif %s".format(path))
                      if (watchKey.isReportModifyEvents()) {
                          watchKey.signalEvent(StandardWatchEventKinds.ENTRY_MODIFY, path)
                      }
                      lastModifiedMap += path -> Files.getLastModifiedTime(path)
                    }

                    for (path <- deletedFiles) {
                      if (watchKey.isReportDeleteEvents()) {
                          watchKey.signalEvent(StandardWatchEventKinds.ENTRY_DELETE, path);
                      }
                      lastModifiedMap -= path
                    }
                }
            } catch {
              case ex:IOException =>
                ex.printStackTrace()
                // TODO manage this error
            }
        }

        def findModifiedFiles(filesOnDisk: Set[Path]) = {
            var modifiedFileList = List[Path]()
            try {
                for (path <- filesOnDisk) {
                    val lastModified = lastModifiedMap.get(path)
                    //println("modifications known:%s real:%s".format(lastModified, Files.getLastModifiedTime(path)))
                    lastModified.foreach{ lm => if(lm != Files.getLastModifiedTime(path)) {
                        modifiedFileList = modifiedFileList :+ path
                    } }
                }
            } catch{
              case ex: IOException =>
                ex.printStackTrace()
                // TODO manage this error
            }
            modifiedFileList
        }

        def findCreatedFiles(filesOnDisk: Set[Path]) = {
            var createdFileList = List[Path]()
            for (file <- filesOnDisk) {
                if (!lastModifiedMap.isDefinedAt(file)) {
                    createdFileList :+ file
                }
            }
            createdFileList
        }

        def findDeletedFiles(folderName: String, filesOnDisk: Set[Path]) = {
            var deletedFileList = List[Path]()
            for (file <- lastModifiedMap.keys) {
                if (file.toAbsolutePath().startsWith(folderName) && !filesOnDisk.contains(file)) {
                    deletedFileList :+ file
                }
            }
            deletedFileList
        }
    }
}