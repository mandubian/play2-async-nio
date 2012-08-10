package play2.tools.file

import java.nio.file._
import java.nio.ByteBuffer
import java.nio.channels.{AsynchronousFileChannel, FileLock}
import scala.collection.JavaConversions._
import java.util.concurrent.ExecutorService
import java.nio.file.attribute.FileAttribute
import java.nio.channels.CompletionHandler
import scala.concurrent.{Future, Promise, ExecutionContext}
import scala.collection.generic.CanBuildFrom

import play.api.libs.iteratee._
import play.api.libs.concurrent._

import play2.tools.iteratee._

trait FileChannel {
  implicit def context: ExecutionContext

  def path: Path
  def channel: Option[AsynchronousFileChannel]
  def error: Option[Throwable]
  def openOptions: Set[OpenOption] = Set()

  def write[T](enumerator: Enumerator[T])(implicit converter: Converter[T, Array[Byte]]): Future[Long] = {
    enumerator &> Enumeratee.map( converter.convert(_) ) |>>> iteratee()
  }

  def append[T](enumerator: Enumerator[T])(implicit converter: Converter[T, Array[Byte]]): Future[Long] = {
    enumerator &> Enumeratee.map( converter.convert(_) ) |>>> iteratee()
  }

  def enumerate(chunkSize: Int = 1024 * 8): RichEnumerator[Array[Byte]] = RichEnumerator(channel
    .map(FileChannel.enumerate(_, chunkSize))
    .getOrElse(Enumerator.eof)
  )

  def enumerateFrom(p: Long, chunkSize: Int = 1024 * 8): RichEnumerator[Array[Byte]] = RichEnumerator(channel
    .map(FileChannel.enumerate(_, chunkSize, p))
    .getOrElse(Enumerator.eof)
  )

  def reader(chunkSize: Int = 1024 * 8) = enumerate(chunkSize)

  def iteratee(appending: Boolean = false): Iteratee[Array[Byte], Long] = channel
    .map(FileChannel.iteratee(_, appending || openOptions.contains(StandardOpenOption.APPEND) ))
    .getOrElse(FileChannel.ignore[Array[Byte]])

  def writer(appending: Boolean = false) = iteratee(appending)

  def hasError: Boolean = error.isDefined
  def exists: Boolean = Files.exists(path)
  def isReadable: Boolean = !hasError && Files.isReadable(path) && openOptions.contains(StandardOpenOption.READ)
  def isWritable: Boolean = !hasError && Files.isWritable(path) && openOptions.contains(StandardOpenOption.WRITE)
  def isRegularFile: Boolean = Files.isRegularFile(path)
  def isDirectory: Boolean = Files.isDirectory(path)

  def close: Future[Unit] = {
    try{
      if(channel.isDefined){
        channel.get.close()
      }
      Future.successful[Unit]()
    } catch {
      case ex: java.io.IOException =>Future.failed[Unit](ex)
    }
    
  }
}

case class FileChannelBuilder(path: Path, options: Set[StandardOpenOption] = Set(StandardOpenOption.READ)) {
  def mode(_options: StandardOpenOption *) = {
    // APPEND is not supported under Linux so simulating APPEND by WRITE
    if(_options.contains(StandardOpenOption.APPEND) )
      this.copy( options = options ++ _options ++ Set(StandardOpenOption.WRITE) )
    else this.copy( options = options ++ _options)
  }

  def open(implicit _context: ExecutionContext, service: ExecutorService) = FileChannel.open(path, options.map{ o => o: OpenOption })
  def create(implicit _context: ExecutionContext, service: ExecutorService) = mode(StandardOpenOption.CREATE).open
  def reading = mode(StandardOpenOption.READ)
  def writing = mode(StandardOpenOption.WRITE)
  def appending = mode(StandardOpenOption.APPEND, StandardOpenOption.WRITE)
}

/*sealed trait FileOpenOption

case object CREATE extends FileOpenOption
case object WRITE extends FileOpenOption*/

object FileChannel {
  val readOptions = Set(StandardOpenOption.READ: OpenOption)

  def withFile[A](name: String, options: StandardOpenOption *)(f: FileChannel => A)
      (implicit _context: ExecutionContext, service: ExecutorService) : A = {
    withFile(FileChannel(name, options: _*).open)(f)
  }

  def withFile[A](path: Path, options: StandardOpenOption *)(f: FileChannel => A)
      (implicit _context: ExecutionContext, service: ExecutorService) : A = {
    withFile(FileChannel(path, options: _*).open)(f)
  }

  def withFile[A](fc: FileChannel)(f: FileChannel => A)
      (implicit _context: ExecutionContext, service: ExecutorService): A = {
    try {
      f(fc)
    } finally {
      fc.close
    }
  }

  def apply(fileName: String, options: StandardOpenOption *): FileChannelBuilder = apply(java.nio.file.Paths.get(fileName), options: _*)
  def apply(path: Path, options: StandardOpenOption *): FileChannelBuilder = FileChannelBuilder(path).mode(options: _*)

  /**
   * @return an [[play.api.libs.iteratee.Iteratee]] which just ignores its input
   */
  def ignore[E]: Iteratee[E, Long] = Iteratee.fold[E, Long](-1L)((_, _) => -1L)

  private def failedChannel(thePath: Path, t: Throwable)(implicit _context: ExecutionContext) = new FileChannel {
    override implicit val context = _context
    override val path = thePath
    override val channel = None
    override val error = Some(t)
  }

  def open(thePath: Path, options: Set[OpenOption], attrs: FileAttribute[_]*)(implicit _context: ExecutionContext, service: ExecutorService): FileChannel = {
    try {
      new FileChannel {
        override implicit val context = _context
        override val path = thePath
        override val channel = Some(AsynchronousFileChannel.open(
          thePath, 
          options.map{ t => if(t == StandardOpenOption.APPEND) StandardOpenOption.WRITE else t }, // APPEND is not supported on UNIX so removing it and replacing by WRITE and try simulating
          service, attrs: _*))
        override val openOptions = options
        override val error = None
      }
    } catch {
      case ex: java.lang.Exception => failedChannel(thePath, ex)
    }  
  }

  def open(path: Path, attrs: FileAttribute[_]*)(implicit _context: ExecutionContext, service: ExecutorService): FileChannel = {
    open(path, readOptions, attrs: _*)
  }

  def open(fileName: String, options: Set[OpenOption], attrs: FileAttribute[_]*)(implicit _context: ExecutionContext, service: ExecutorService): FileChannel = {
    open(java.nio.file.Paths.get(fileName), options, attrs: _*)
  }

  def open(fileName: String, attrs: FileAttribute[_]*)(implicit _context: ExecutionContext, service: ExecutorService): FileChannel = {
    open(java.nio.file.Paths.get(fileName), readOptions, attrs: _*)
  }

  def unfoldM[S,E](s:S)(f: S => Future[Option[(S, Input[E])]] )(implicit context: ExecutionContext): Enumerator[E] = Enumerator.checkContinue1(s)(new Enumerator.TreatCont1[E,S]{

    def apply[A](loop: (Iteratee[E,A],S) => Future[Iteratee[E,A]], s:S, k: Input[E] => Iteratee[E,A]):Future[Iteratee[E,A]] = f(s).flatMap {
      case Some((newS, e)) => loop(k(e),newS)
      case None => loop(k(Input.Empty),s) //play.api.libs.concurrent.Promise.pure(Cont(k))
    }
  })

  case class FileCursor(position: Long)
  
  def enumerate(channel: AsynchronousFileChannel, chunkSize: Int = 1024 * 8, position: Long = 0L, tailable: Boolean = false)(implicit context: ExecutionContext): Enumerator[Array[Byte]] = {
    
    /**
     * triggers async file read and completes/fails promise depending on async result
     */
    def stepRead(p: Promise[Option[(FileCursor, Input[Array[Byte]])]], fileCursor: FileCursor, chunkSize: Int) = {
      val buffer = ByteBuffer.allocate(chunkSize)
      //println("try reading")
      channel.read(
        buffer,
        fileCursor.position,
        fileCursor,
        new CompletionHandler[java.lang.Integer, FileCursor] {
          override def completed(result: java.lang.Integer, attachment: FileCursor) = result.toInt match {
            case -1 => 
              // file position is greater than current file size but not an error: just keep reading at same position
              // if tailable, doesn't stop enumerator
              if(tailable) {
                //println("tailing")
                p.success( None )
              }
              // if not, sends EOF
              else {
                //println("EOF")
                p.success( Some( attachment -> Input.EOF ))
              }
              
            case read => 
              // doesn't keep trailing 0x00 if not enough to fill chunk
              if(read < chunkSize) {
                val b = new Array[Byte](read)
                Array.copy(buffer.array, 0, b, 0, read)
                p.success( Some( FileCursor(attachment.position + read) -> Input.El(b) ))
              }
              else p.success( Some( FileCursor(attachment.position + read) -> Input.El(buffer.array()) ))
          }

          override def failed(exc: Throwable, attachment: FileCursor) = {
            // simply fails in this case for the time being 
            // TODO : examine all IO exceptions
            println("failure %s".format(exc.getMessage))
            p.failure( exc )
          }
        }
      )
    }

    unfoldM(FileCursor(position)) { fileCursor =>
      val p = Promise[Option[(FileCursor, Input[Array[Byte]])]]()
      stepRead(p, fileCursor, chunkSize)

      p.future
    }
  }

  def iteratee(channel: AsynchronousFileChannel, appending: Boolean = false)(implicit context: ExecutionContext): Iteratee[Array[Byte], Long] = {
    val initPos = if(appending) channel.size else 0
    Iteratee.fold1(FileCursor( initPos )){ (fc: FileCursor, array: Array[Byte]) =>
      val buffer = ByteBuffer.wrap(array)
      val p = Promise[FileCursor]()

      channel.write(
        buffer,
        fc.position,
        fc,
        new CompletionHandler[java.lang.Integer, FileCursor] {
          override def completed(result: java.lang.Integer, attachment: FileCursor) = result.toInt match {
            case read => p.success( FileCursor(attachment.position + read) )
          }

          override def failed(exc: Throwable, attachment: FileCursor) = {
            // simply fails in this case for the time being 
            // TODO : examine all IO exceptions
            p.failure( exc )
          }
        }
      )
      p.future
    }.map( fc => fc.position - initPos)
  }

  /** NOT USEFUL 
   * Locks are not good because:
   *  - in appending mode, a lock can't be acquired on the growing region of the file
   *  - In the Java doc [[http://docs.oracle.com/javase/7/docs/api/java/nio/channels/AsynchronousFileChannel.html#lock(long, long, boolean, A, java.nio.channels.CompletionHandler)]]
   *  "File locks are held on behalf of the entire Java virtual machine. They are not suitable for controlling access to a file by multiple threads within the same virtual machine."
   */
  def lockIteratee(channel: AsynchronousFileChannel, appending: Boolean = false)(implicit context: ExecutionContext): Iteratee[Array[Byte], Long] = {
    val initPos = if(appending) channel.size else 0
    Iteratee.fold1(FileCursor( initPos )){ (fc: FileCursor, array: Array[Byte]) =>
      val buffer = ByteBuffer.wrap(array)
      val p = Promise[FileCursor]()

      val realFc = FileCursor( if(appending) channel.size else fc.position )

      // acquires lock
      channel.lock(
        realFc.position, 
        array.size, 
        false, 
        realFc,
        new CompletionHandler[FileLock, FileCursor] {
          override def completed(lock: FileLock, attachment: FileCursor) = {
            val position = lock.position
            val size = lock.size

            // now writes on locked region
            channel.write(
              buffer,
              position,
              attachment,
              new CompletionHandler[java.lang.Integer, FileCursor] {
                override def completed(result: java.lang.Integer, attachment: FileCursor) = result.toInt match {
                  case read => 
                    lock.close
                    p.success( FileCursor(attachment.position + read) )
                }

                override def failed(exc: Throwable, attachment: FileCursor) = {
                  // simply fails in this case for the time being 
                  // TODO : examine all IO exceptions
                  lock.close
                  p.failure( exc )
                }
              }
            )
          }

          override def failed(exc: Throwable, attachment: FileCursor) = {
            // simply fails in this case for the time being 
            // TODO : examine all IO exceptions
            p.failure( exc )
          }
        }
      )
      
      p.future
    }.map( fc => fc.position - initPos)
  }
}

object DefaultImplicits extends Context with Converters
object PlayImplicits extends PlayContext with Converters

trait PlayContext extends Context {
  import play.api.Play.current

  object Int {
    def unapply(s : String) : Option[Int] = try {
      Some(s.toInt)
    } catch {
      case _ : java.lang.NumberFormatException => None
    }
  }

  override val (min, num, max) = (
    current.configuration.getInt("file.concurrent.context.minThreads"),
    current.configuration.getString("file.concurrent.context.numThreads").flatMap{
      case Int(i) => Some(i)
      case s if s.charAt(0) == 'x' => Some((Runtime.getRuntime.availableProcessors * s.substring(1).toDouble).ceil.toInt)
      case _ => None
    },
    current.configuration.getInt("file.concurrent.context.maxThreads")
  )

}


trait Context {
  def min: Option[Int] = None
  def num: Option[Int] = None
  def max: Option[Int] = None

  def defaultReporter: Throwable => Unit = (t: Throwable) => t.printStackTrace()

  implicit lazy val defaultExecutionContext: FileExecutionContext = new FileExecutionContext(defaultReporter, min, num, max)
  implicit lazy val defaultExecutorService: ExecutorService = defaultExecutionContext.executorService

}
