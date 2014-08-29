package play2.tools.file

import java.nio.file._
import java.nio.ByteBuffer
import java.nio.channels.{AsynchronousFileChannel}
import scala.collection.JavaConversions._
import java.util.concurrent.ExecutorService
import java.nio.file.attribute.FileAttribute
import java.nio.channels.CompletionHandler
import scala.concurrent.{Future, Promise, ExecutionContext}
import scala.collection.generic.CanBuildFrom

import play.api.libs.iteratee._
import play.api.libs.concurrent._

import play2.tools.iteratee._

/**
 * Main Trait wrapping JDK7 NIO2 API :
 *  - File opening/creating/deleting
 *  - async/non-blocking enumerator to read from file
 *  - async/non-blocking iteratee to write to file
 * 
 * Please note: 
 *  - FileChannel is immutable and all side-effect functions (open/close/delete) creates new instance of FileChannel.
 *  - FileChannel wraps AsynchronousFileChannel which is thread-safe so FileChannel can re-used safely.
 *  - enumerator() is an async/non-blocking/monadic reader based on Play2 enumerators + Scala Futures
 *  - iteratee() is an async/non-blocking/monadic writer based on Play2 enumerators + Scala Futures
 *  - FileChannel manages IO errors but won't throw any exception. In case of errors, IO operations will just have NO effect. So if you want to manage errors, use ''FileChannel.hasError''.
 *  - FileChannel wraps ExecutionContext used by Scala Futures and corresponding ExecutorService used by Async JDK7 File Apis
 * 
 * '''All functions having side-effects (open/close/delete/enumerator/iteratee) must use empty parenthesis.'''
 *
 * '''All functions with NO side-effects (hasError/isXXX) omit empty parenthesis.'''
 *
 * ''FileChannel is just a trait and can't be instantiated without using helpers.''
 */
trait FileChannel {
  /**
   * The ExecutionContext used by Scala Futures.
   *
   * A default context is provided and can be imported using: 
   *  {{{import play2.tools.file.DefaultImplicits._}}}
   */
  implicit def context: ExecutionContext

  implicit def service: ExecutorService

  /**
   * The JDK7 File [[http://docs.oracle.com/javase/7/docs/api/java/nio/file/Path.html Path]].
   */
  def path: Path

  /**
   * The JDK7 [[http://docs.oracle.com/javase/7/docs/api/java/nio/channels/AsynchronousFileChannel FileChannel]].
   *
   * ''If the FileChannel hasn't been opened, channel is set to None.''
   */
  def channel: Option[AsynchronousFileChannel] = None

  /**
   * Wraps errors (Throwable) that can be thrown by IO operations, when files are not found or whatever.
   *
   * ''The error is set to None if there are no error.''
   *
   * You can verify there are errors using {{{fileChannel.hasError}}}
   * You can get the error using {{{fileChannel.error}}}
   */
  def error: Option[Throwable] = None

  /**
   * File open options from JDK7 [[http://docs.oracle.com/javase/7/docs/api/java/nio/file/OpenOption.html OpenOptions]].
   */
  def openOptions: Set[OpenOption] = Set()

  /**
   * Opens File from Path using provided [[http://docs.oracle.com/javase/7/docs/api/java/nio/file/OpenOption.html Open Options]] 
   * and [[http://docs.oracle.com/javase/7/docs/api/java/nio/file/attribute/FileAttribute.html File Attributes]].
   * 
   * Please note:
   *  - it creates a new FileChannel instance to keep everything immutable.''
   *  - if the channel was already opened, it is firstly closed
   * 
   * @param attrs the FileAttribute from Java API
   * @return new FileChannel with opened java channel
   */
  def open(attrs: FileAttribute[_]*): FileChannel = {
    try{
      if(channel.isDefined && !hasError){
        this
      } else {
        if(channel.isDefined){
          channel.get.close()
        }

        FileChannel.open(path, openOptions, attrs:_*)
      }
    } catch {
      case ex: java.io.IOException =>
        FileChannel.failedChannel(path, openOptions, Some(ex))
    }
  }

  /**
   * Closes FileChannel
   *
   * @return closed FileChannel (which can be re-used or re-opened)
   */
  def close(): FileChannel = {
    try{
      if(channel.isDefined){
        channel.get.close()
      }
      //Future.successful[Unit]()
      FileChannel.failedChannel(path, openOptions, None)
    } catch {
      case ex: java.io.IOException =>
        FileChannel.failedChannel(path, openOptions, Some(ex))
    }
  }

  /**
   * Deletes FileChannel
   *
   * @return closed FileChannel (which can be re-used or re-opened)
   */
  def delete(): FileChannel = {
    try{
      if(channel.isDefined){
        channel.get.close()
      }
      //Future.successful[Unit]()
      FileChannel.delete(path, openOptions)
    } catch {
      case ex: java.io.IOException =>
        //Future.failed[Unit](ex)
        FileChannel.failedChannel(path, openOptions, Some(ex))
    }
  }

  /**
   * Builds a read enumerator of Array[Byte]
   *
   * The file is read from beginnign by default.
   * This is a normal Enumerator[Array[Byte]] which can composed with any other Enumeratee or Iteratee.
   *
   * @param chunkSize the chunk read at each iteration. By default, it is 8Ko
   * @return Enumerator[Array[Byte]] is the file can be read or Enumerator.eof if the channel was not opened
   */
  def enumerator(chunkSize: Int = 1024 * 8): Enumerator[Array[Byte]] = { 
    channel.map(FileChannel.enumerate(_, chunkSize)).getOrElse(Enumerator.eof)
  }

  /**
   * An Alias for enumerator()
   */
  def reader(chunkSize: Int = 1024 * 8) = enumerator(chunkSize)

  /**
   * Builds a read enumerator beginning at a given position in bytes
   *
   * @param position the position where enumerator begins reading
   * @param chunkSize the chunk read at each iteration. By default, it is 8Ko
   * @return Enumerator[Array[Byte]] is the file can be read or Enumerator.eof if the channel was not opened
   * 
   */
  def enumeratorFrom(position: Long, chunkSize: Int = 1024 * 8): Enumerator[Array[Byte]] = {
    channel.map(FileChannel.enumerate(_, chunkSize, position)).getOrElse(Enumerator.eof)
  }

  /**
   * An Alias for enumeratorFrom
   */
  def readerFrom(position: Long, chunkSize: Int = 1024 * 8) = enumeratorFrom(position, chunkSize)

  /**
   * Builds a writing iteratee writing Array[Byte] and returning the number of Bytes written.
   *
   * This iteratee writes from beginning of file. You can append at the end of the file using ''appending'' flag
   *
   * @param appending a boolean to trigger writing
   * @return an Iteratee[Array[Byte], Long]
   */
  def iteratee(appending: Boolean = false): Iteratee[Array[Byte], Long] = channel
    .map(FileChannel.iteratee(_, 0L, appending || openOptions.contains(StandardOpenOption.APPEND) ))
    .getOrElse(FileChannel.ignore[Array[Byte]])

  /**
   * An Alias for iteratee
   */
  def writer(appending: Boolean = false) = iteratee(appending)

  /**
   * Builds a writing iteratee writing Array[Byte] at the given position and returning the number of Bytes written.
   * 
   * @param position the position to write from
   * @return an Iteratee[Array[Byte], Long] or an ignore iteratee doing nothing if the channel is not writeable
   */
  def iterateeFrom(position: Long): Iteratee[Array[Byte], Long] = channel
    .map(FileChannel.iteratee(_, position))
    .getOrElse(FileChannel.ignore[Array[Byte]])

  /**
   * An Alias for iterateeFrom
   */
  def writerFrom(position: Long) = iterateeFrom(position)

  /**
   * Indicates if there was any IO error dealing with this channel.
   */
  def hasError: Boolean = error.isDefined

  /**
   * Shortcut to Files.exists function
   */
  def exists: Boolean = Files.exists(path)

  /**
   * Shortcut to Files.isReadable function
   */
  def isReadable: Boolean = !hasError && Files.isReadable(path) && openOptions.contains(StandardOpenOption.READ)

  /**
   * Shortcut to Files.isWritable function
   */
  def isWritable: Boolean = !hasError && Files.isWritable(path) && openOptions.contains(StandardOpenOption.WRITE)

  /**
   * Shortcut to Files.isRegularFile function
   */
  def isRegularFile: Boolean = Files.isRegularFile(path)

  /**
   * Shortcut to Files.isDirectory function
   */
  def isDirectory: Boolean = Files.isDirectory(path)

  // hiding these functions which are too imperative with respect to other features of the API
  /*def write[T](enumerator: Enumerator[T], pos: Long = 0L)(implicit converter: Converter[T, Array[Byte]]): Future[Long] = {
    enumerator &> Enumeratee.map( converter.convert(_) ) |>>> iterateeFrom(pos)
  }

  def append[T](enumerator: Enumerator[T])(implicit converter: Converter[T, Array[Byte]]): Future[Long] = {
    enumerator &> Enumeratee.map( converter.convert(_) ) |>>> iteratee(appending = true)
  }*/

}

private[file] sealed trait SideAction {
  def action(builder: FileChannelBuilder)(implicit _context: ExecutionContext, service: ExecutorService): FileChannel
}

private[file] case object DeleteFirst extends SideAction {
  override def action(builder: FileChannelBuilder)(implicit _context: ExecutionContext, service: ExecutorService): FileChannel = {
    builder.delete()
  }
}

/**
 * Allows to build FileChannel by adding OpenOptions and finally opening/creating the File channel
 *
{{{
// opens a file for reading (reading is not necessary)
FileChannel("the/path/to/file").reading.open()

// opens a file for writing
FileChannel("the/path/to/file").writing.open()

// creates a file for reading/writing (reading is not necessary)
FileChannel("the/path/to/file").reading.writing.create()

// deletes a file
FileChannel("the/path/to/file").delete()

// deletes a file first and creates it for writing
FileChannel("the/path/to/file").deleteFirst.writing.create()
}}} 
 */
case class FileChannelBuilder(path: Path, openOptions: Set[StandardOpenOption] = Set(StandardOpenOption.READ), action: Option[SideAction] = None) {
  
  /**
   * Opens File for reading by default and creates FileChannel using path, openOptions and given File Attributes
   *
   * If the file cannot be opened, it returns a close FileChannel containing the error
   *
   * @return opened FileChannel
   */
  def open(attrs: FileAttribute[_]*)(implicit _context: ExecutionContext, service: ExecutorService) = {
    if(action.isDefined) action.get.action(this).open(attrs:_*)
    else FileChannel.open(path, openOptions.map{ o => o: OpenOption }, attrs:_*)
  }
  
  /**
   * Creates File for reading by default and creates FileChannel using path, openOptions and given File Attributes
   *
   * If the file cannot be created, it returns a close FileChannel containing the error
   *
   * @return created FileChannel
   */
  def create(attrs: FileAttribute[_]*)(implicit _context: ExecutionContext, service: ExecutorService) = {
    mode(StandardOpenOption.CREATE).open(attrs:_*)
  }
  
  /**
   * Creates File for reading by default if not already existing and creates FileChannel using path, openOptions and given File Attributes
   * (This uses StandardOpenOption.CREATE_NEW option)
   * ''If the file cannot be created, it returns a close FileChannel containing the error''
   *
   * @return created FileChannel
   */
  def createIfNew(attrs: FileAttribute[_]*)(implicit _context: ExecutionContext, service: ExecutorService) = {
    mode(StandardOpenOption.CREATE_NEW).open(attrs:_*)
  }
  
  /**
   * Deletes File and creates closed FileChannel using path, openOptions and given File Attributes
   *
   * ''If the file cannot be created, it returns a close FileChannel containing the error''
   * {{{val f = FileChannel("the/path").delete()}}}
   *
   * @return closed FileChannel
   */
  def delete()(implicit _context: ExecutionContext, service: ExecutorService) = {
    FileChannel.delete(path, openOptions.map{ o => o: OpenOption })
  }

  /**
   * Sets a "delete first" side action which will delete file before re-opening/creating it
   *
   * {{{val f = FileChannel("the/path").deleteFirst.open()}}}
   */
  def deleteFirst(implicit _context: ExecutionContext, service: ExecutorService) = {
    action(DeleteFirst)
  }

  /**
   * Adds StandardOpenOption.READ open option
   */
  def reading = mode(StandardOpenOption.READ)

  /**
   * Adds StandardOpenOption.WRITE open option
   */
  def writing = mode(StandardOpenOption.WRITE)

  /**
   * Adds StandardOpenOption.APPEND & StandardOpenOption.WRITE open option
   */
  def appending = mode(StandardOpenOption.APPEND, StandardOpenOption.WRITE)

  /**
   * Adds StandardOpenOption
   * @param _options an Array of StandardOpenOption
   */
  def mode(_options: StandardOpenOption *) = {
    // APPEND is not supported under Linux so simulating APPEND by WRITE
    if(_options.contains(StandardOpenOption.APPEND) )
      this.copy( openOptions = openOptions ++ _options ++ Set(StandardOpenOption.WRITE) )
    else this.copy( openOptions = openOptions ++ _options)
  }

  /**
   * Adds a Side Action (only existing one is DeleteFirst)
   */
  def action(_action: SideAction) = {
    this.copy( action = Some(_action) )
  }

}

/**
 * FileChannel companion object providing constructors or helpers
 */
object FileChannel {
 
  /**
   * Builds a FileChannelBuilder from fileName
   *
   * @param fileName the file name
   * @param options some [[java.nio.file.StandardOpenOption]]
   * @return a FileChannelBuilder
   */
  def apply(fileName: String, options: StandardOpenOption *): FileChannelBuilder = apply(java.nio.file.Paths.get(fileName), options: _*)
  
  /**
   * Builds a FileChannelBuilder from [[java.nio.file.Path]]
   *
   * @param path the file path
   * @param options some [[java.nio.file.StandardOpenOption]]
   * @return a FileChannelBuilder
   */
  def apply(path: Path, options: StandardOpenOption *): FileChannelBuilder = FileChannelBuilder(path).mode(options: _*)

  /**
   * A helper taking care of asynchronously closing file when Future actions are completed.
   * This helper is based on Scala Future onComplete function
   */
  def withFileFuture[A](fc: FileChannel)(f: FileChannel => Future[A])
      (implicit _context: ExecutionContext, service: ExecutorService): Future[A] = {
    val p = Promise[A]()

    f(fc).onComplete {
      case result =>
        try {
          result match {
            case Left(t)  => p failure t
            case Right(r) => p success r
          } 
        } finally {
          fc.close
        }
    }(_context)

    p.future
  }

  /**
   * A helper taking care of closing file.
   * This function is non-asynchronous so you must take care of what you do because the file 
   * could be closed before your asynchronous action is fulfilled.
   */
  def withFile[A](fc: FileChannel)(f: FileChannel => A)
      (implicit _context: ExecutionContext, service: ExecutorService): A = {
    try {
      f(fc)
    } finally {
      fc.close
    }
  }



  /**
   * @return an [[play.api.libs.iteratee.Iteratee]] which just ignores its input
   */
  private[file] def ignore[E]: Iteratee[E, Long] = Iteratee.fold[E, Long](-1L)((_, _) => -1L)

  private[file] val readOptions = Set(StandardOpenOption.READ: OpenOption)

  private[file] def failedChannel(thePath: Path, options: Set[OpenOption], theError: Option[Throwable])(implicit _context: ExecutionContext, _service: ExecutorService) = new FileChannel {
    override implicit val context = _context
    override implicit val service = _service
    override val path = thePath
    override val channel = None
    override val error = theError
    override val openOptions = options
  }

  private[file] def open(thePath: Path, options: Set[OpenOption], attrs: FileAttribute[_]*)(implicit _context: ExecutionContext, _service: ExecutorService): FileChannel = {
    try {
      new FileChannel {
        override implicit val context = _context
        override implicit val service = _service
        override val path = thePath
        override val channel = Some(AsynchronousFileChannel.open(
          thePath, 
          options.map{ t => if(t == StandardOpenOption.APPEND) StandardOpenOption.WRITE else t }, // APPEND is not supported on UNIX so removing it and replacing by WRITE and try simulating
          service, attrs: _*))
        override val openOptions = options
        override val error = None
      }
    } catch {
      case ex: java.lang.Exception => failedChannel(thePath, options, Some(ex))
    }  
  }

  private[file] def open(path: Path, attrs: FileAttribute[_]*)(implicit _context: ExecutionContext, _service: ExecutorService): FileChannel = {
    open(path, readOptions, attrs: _*)
  }

  private[file] def open(fileName: String, options: Set[OpenOption], attrs: FileAttribute[_]*)(implicit _context: ExecutionContext, service: ExecutorService): FileChannel = {
    open(java.nio.file.Paths.get(fileName), options, attrs: _*)
  }

  private[file] def open(fileName: String, attrs: FileAttribute[_]*)(implicit _context: ExecutionContext, service: ExecutorService): FileChannel = {
    open(java.nio.file.Paths.get(fileName), readOptions, attrs: _*)
  }

  private[file] def delete(thePath: Path, options: Set[OpenOption])(implicit _context: ExecutionContext, _service: ExecutorService): FileChannel = {
    try {
      new FileChannel {
        override implicit val context = _context
        override implicit val service = _service
        override val path = thePath
        override val channel = None
        override val openOptions = options
        override val error = if(!Files.deleteIfExists(thePath)) Some(new RuntimeException("could delete file because it doesn't exist")) else None
      }
    } catch {
      case ex: java.lang.Exception => failedChannel(thePath, options, Some(ex))
    }  
  }

  private[file] def unfoldM[S,E](s:S)(f: S => Future[Option[(S, Input[E])]] )(implicit context: ExecutionContext): Enumerator[E] = Enumerator.checkContinue1(s)(new Enumerator.TreatCont1[E,S]{

    def apply[A](loop: (Iteratee[E,A],S) => Future[Iteratee[E,A]], s:S, k: Input[E] => Iteratee[E,A]):Future[Iteratee[E,A]] = f(s).flatMap {
      case Some((newS, e)) => loop(k(e),newS)
      case None => loop(k(Input.Empty),s) //play.api.libs.concurrent.Promise.pure(Cont(k))
    }
  })

  private[file] case class FileCursor(position: Long)
  
  private[file] def enumerate(channel: AsynchronousFileChannel, chunkSize: Int = 1024 * 8, position: Long = 0L)(implicit context: ExecutionContext): Enumerator[Array[Byte]] = {
    
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
              p.success( Some( attachment -> Input.EOF ))
              
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

  private[file] def iteratee(channel: AsynchronousFileChannel, position: Long = 0L, appending: Boolean = false)(implicit context: ExecutionContext): Iteratee[Array[Byte], Long] = {
    val initPos = if(appending) channel.size else position
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
  /*def lockIteratee(channel: AsynchronousFileChannel, appending: Boolean = false)(implicit context: ExecutionContext): Iteratee[Array[Byte], Long] = {
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
  }*/
}

