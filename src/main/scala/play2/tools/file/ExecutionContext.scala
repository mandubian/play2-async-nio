package play2.tools.file

import java.util.concurrent.{ LinkedBlockingQueue, Callable, Executor, ExecutorService, Executors, ThreadFactory, TimeUnit, ThreadPoolExecutor }
import java.util.Collection
import scala.concurrent.forkjoin1._
import scala.concurrent.{ BlockContext, ExecutionContext, Awaitable, CanAwait, ExecutionContextExecutor, ExecutionContextExecutorService }
import scala.concurrent.util.Duration
import scala.util.control.NonFatal
import play.api.Configuration

// just shamefully copying scala implementation and simplifying it :D
class FileExecutionContext(reporter: Throwable => Unit, min: Option[Int], num: Option[Int], max: Option[Int]) extends ExecutionContextExecutor {

  lazy val executor: Executor = executorService
  lazy val executorService: ExecutorService = createExecutorService

  // Implement BlockContext on FJP threads
  class DefaultThreadFactory(daemonic: Boolean) extends ThreadFactory with ForkJoinPool.ForkJoinWorkerThreadFactory { 
    def wire[T <: Thread](thread: T): T = {
      thread.setDaemon(daemonic)
      //Potentially set things like uncaught exception handler, name etc
      thread
    }

    def newThread(runnable: Runnable): Thread = wire(new Thread(runnable))

    def newThread(fjp: ForkJoinPool): ForkJoinWorkerThread = wire(new ForkJoinWorkerThread(fjp) with BlockContext {
      override def blockOn[T](thunk: =>T)(implicit permission: CanAwait): T = {
        var result: T = null.asInstanceOf[T]
        ForkJoinPool.managedBlock(new ForkJoinPool.ManagedBlocker {
          @volatile var isdone = false
          override def block(): Boolean = {
            result = try thunk finally { isdone = true }
            true
          }
          override def isReleasable = isdone
        })
        result
      }
    })
  }

  def createExecutorService: ExecutorService = {
    println("Creating ExecutionContext with thread(min:%s num:%s max:%s)".format(min, num, max))
    def range(floor: Int, desired: Int, ceiling: Int): Int =
      if (ceiling < floor) range(ceiling, desired, floor) else scala.math.min(scala.math.max(desired, floor), ceiling)

    lazy val desiredParallelism = {
      /*val (min, num, max) = (
        configuration.getInt("file.concurrent.context.minThreads").getOrElse(0),
        configuration.getString("file.concurrent.context.numThreads").flatMap{
          case Int(i) => Some(i)
          case s if s.charAt(0) == 'x' => Some((Runtime.getRuntime.availableProcessors * s.substring(1).toDouble).ceil.toInt)
          case _ => None
        }.getOrElse(Runtime.getRuntime.availableProcessors),
        configuration.getInt("file.concurrent.context.maxThreads").getOrElse(Runtime.getRuntime.availableProcessors)
      )*/

      range(
        min.getOrElse(0), 
        num.getOrElse(Runtime.getRuntime.availableProcessors), 
        max.getOrElse(Runtime.getRuntime.availableProcessors)
      ) 
    }

    val threadFactory = new DefaultThreadFactory(daemonic = true)
    
    try {
      new ForkJoinPool(
        desiredParallelism,
        threadFactory,
        null, //FIXME we should have an UncaughtExceptionHandler, see what Akka does
        true) // Async all the way baby
    } catch {
      case NonFatal(t) =>
        System.err.println("Failed to create ForkJoinPool for the default ExecutionContext, falling back to ThreadPoolExecutor")
        t.printStackTrace(System.err)
        val exec = new ThreadPoolExecutor(
          desiredParallelism,
          desiredParallelism,
          5L,
          TimeUnit.MINUTES,
          new LinkedBlockingQueue[Runnable],
          threadFactory
        )
        exec.allowCoreThreadTimeOut(true)
        exec
    }
  }

  def execute(runnable: Runnable): Unit = {
    executor match {
    case fj: ForkJoinPool =>
      Thread.currentThread match {
        case fjw: ForkJoinWorkerThread if fjw.getPool eq fj =>
          (runnable match {
            case fjt: ForkJoinTask[_] => fjt
            case _ => ForkJoinTask.adapt(runnable)
          }).fork
        case _ => fj.execute(runnable)
      }
    case generic => 
      generic execute runnable
  }}

  def reportFailure(t: Throwable) = reporter(t)
}