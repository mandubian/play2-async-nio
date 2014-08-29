package play2.tools.file

import play2.tools.iteratee.Converter
import scala.collection.generic.CanBuildFrom
import java.util.concurrent.ExecutorService

/**
 * DefaultImplicits to import when using play2-async-nio outside Play2 to have implicit ExecutionContext and ExecutorService
 *
 * {{{import play2.tools.file.DefaultImplicits._}}}
 */ 
object DefaultImplicits extends Context with Converters

/**
 * DefaultImplicits to import when using play2-async-nio with Play2 to have implicit ExecutionContext and ExecutorService
 *
 * {{{import play2.tools.file.PlayImplicits._}}}
 * You can customize threadpool by adding following configuration in conf/application.conf:
 {{{
 file.concurrent.context.minThreads=0
 
 file.concurrent.context.numThreads=4 
 or 
 file.concurrent.context.numThreads=x2 (to have a multiplier of nb of cores)
 
 file.concurrent.context.maxThreads=10
 }}}
 */ 
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

/**
 * A Context containing the implicit ExecutionContext and ExecutorService
 * It creates an executioncontext based on a pool of threads defined by 3 parameters:
 *  - min : the minimum number of threads in the pool
 *  - num : the expected number of threads in the pool or a multiplier of the nb of cores (2x, 4x)
 *  - max : the maximum number of threads in the pool
 */ 
trait Context {
  /**
   * the minimum number of threads in the pool
   */ 
  def min: Option[Int] = None

  /**
   * the expected number of threads in the pool or a multiplier of the nb of cores (2x, 4x)
   */ 
  def num: Option[Int] = None

  /**
   * the maximum number of threads in the pool
   */ 
  def max: Option[Int] = None

  def defaultReporter: Throwable => Unit = (t: Throwable) => t.printStackTrace()

  /**
   * implicit ExecutionContext
   */
  implicit lazy val defaultExecutionContext: FileExecutionContext = new FileExecutionContext(defaultReporter, min, num, max)

  /**
   * implicit ExecutorService
   */
  implicit lazy val defaultExecutorService: ExecutorService = defaultExecutionContext.executorService

}

/**
 * Implicit converters
 */
object ImplicitConverters extends Converters 

trait Converters {

  implicit object ArrayToString extends Converter[Array[Byte], String] {
    def convert(a: Array[Byte], options: Map[String, String] = Map()): String = {
      options.get("charset").map{ charset => new String(a, charset) }.getOrElse(new String(a))
    }
  }

  implicit object ArrayToArray extends Converter[Array[Byte], Array[Byte]] {
    def convert(a: Array[Byte], options: Map[String, String] = Map()): Array[Byte] = a
  }

  implicit object StringToArray extends Converter[String, Array[Byte]] {
    def convert(s: String, options: Map[String, String] = Map()): Array[Byte] = {
      options.get("charset").map{ charset => s.getBytes(charset) }.getOrElse( s.getBytes )
    }
  }

  implicit object StringToString extends Converter[String, String] {
    def convert(s: String, options: Map[String, String] = Map()): String = s
  }

  implicit def traversableConverter[M[_] <: Traversable[_], T, V](implicit cbft: CanBuildFrom[M[_], T, M[T]], cbfv: CanBuildFrom[M[_], V, M[V]], converter: Converter[T, V]) = {
    new Converter[M[T], M[V]] {
      def convert(mt: M[T], options: Map[String, String] = Map()): M[V] = {
        mt.foldLeft(cbfv.apply){ case (builder, t: T) => builder += converter.convert(t, options) }.result
      }
    }
  }

}
