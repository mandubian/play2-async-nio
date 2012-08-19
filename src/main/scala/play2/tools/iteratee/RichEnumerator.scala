package play2.tools.iteratee

import scala.concurrent.{Future, Promise, ExecutionContext}
import scala.collection.generic.CanBuildFrom
import scala.util.parsing.combinator.Parsers

import play.api.libs.iteratee._
import play.api.libs.concurrent._

/**
 * Immutable Typeclass Converter to convert data from a type to another one using options.
 * Used to create implicit converters for Enumerators
 */
trait Converter[From, To]{
  def convert(from: From, options: Map[String, String] = Map()): To

  def options(_options: Map[String, String]): Converter[From, To] = new Converter[From, To] {
    def convert(from: From, options: Map[String, String] = Map()): To = {
      this.convert( from, options ++ _options )
    }
  }
}

/**
 * Play2 [[http://www.playframework.org/documentation/api/2.0/scala/play/api/libs/iteratee/Iteratee.html Iteratee]] enhancements
 */
object RichIteratee {

  /**
   * Builds an Iteratee traversing the enumerator and concatenating all chunks in a collection
   *
   * {{{val l: Promise[List[String]] = Enumerator("alpha", "beta", "delta").run(RichIteratee.traverse[List, String]) }}}
   */
  def traverse[M[_], T](implicit cbf: CanBuildFrom[M[_], T, M[T]]): Iteratee[T, M[T]] = Iteratee.fold(cbf.apply){ (builder, e: T) => builder += e }.map(_.result)
  
  /**
   * Simpler version building an Iteratee traversing the enumerator and concatenating all chunks in a collection
   *
   * {{{val l: Promise[List[String]] = Enumerator("alpha", "beta", "delta").run(RichIteratee.traverse[List]) }}}
   */
  def traverse[M[_]] = new {
    def apply[T]()(implicit cbf: CanBuildFrom[M[_], T, M[T]]): Iteratee[T, M[T]] = traverse[M, T](cbf)
  }

  /**
   * a shortcut for traverse[List]
   */
  def toList = traverse[List]

  /**
   * a shortcut for traverse[Seq]
   */
  def toSeq = traverse[Seq]

  /**
   * a shortcut for traverse[Set]
   */
  def toSet = traverse[Set]
}

/**
 * Play2 [[http://www.playframework.org/documentation/api/2.0/scala/play/api/libs/iteratee/Enumeratee.html Iteratee]] enhancements
 */
object RichEnumeratee {

  /**
   * Builds an Enumeratee converting a Enumerator[Collection1[T1]] into an Enumerator[Collection2[T2]]
  {{{
  val e: Enumerator[List[Array[Byte]]] = ...
  e.through(RichEnumeratee.map[List[Array[Byte]], Seq[String], String, String])
  }}}
   */
  def map[T, M[V], X, V]()(implicit p: T => scala.collection.TraversableLike[X, T], 
    cbft: CanBuildFrom[T, X, T], cbfv: CanBuildFrom[M[_], V, M[V]], 
    converter: Converter[X, V] ): Enumeratee[T, M[V]] = {

    Enumeratee.map{ t: T => 
      val builder = cbfv.apply
      p(t).foreach{ x:X => builder += converter.convert(x) } 
      builder.result
    }
  }

  /**
   * Builds an Enumeratee converting a Enumerator[T] into an Enumerator[V]
  {{{
  val e: Enumerator[List[Array[Byte]]] = ...
  e.through(RichEnumeratee.map[Array[Byte], String])
  }}}
   */
  def map[T, V]()(implicit converter: Converter[T, V]): Enumeratee[T, V] = {
    Enumeratee.map{ converter.convert(_) }
  }

  /**
   * Builds an Enumeratee that transforms each chunk through an enumeratee
   {{{
  // splits CSV lines
  val e = Enumerator("alpha,beta,gamma", "alpha1,beta1,gamma1", "alpha2,beta2,gamma2")
  val e2: Enumerator[List[String]] = e.through(RichIteratee.mapThrough[String, List[String]](RichIteratee.split(",")))
   }}}
   */
  def mapThrough[T, V](enumeratee: Enumeratee[T, V]) = {
    Enumeratee.mapFlatten[T]{ t: T => Enumerator.enumInput(Input.El(t)) &> enumeratee }
  }

  /**
   * A shorter version that builds an Enumeratee that transforms each chunk through an enumeratee
  {{{
  // splits CSV lines
  val e = Enumerator("alpha,beta,gamma", "alpha1,beta1,gamma1", "alpha2,beta2,gamma2")
  val e2: Enumerator[List[String]] = e.through(RichIteratee.mapThrough[List](RichIteratee.split(",")))
  }}}
   */
  def mapThrough[M[_]] = new {
    def apply[T, V](enumeratee: Enumeratee[T, V])(implicit cbf: CanBuildFrom[M[_], V, M[V]]): Enumeratee[T, M[V]] = {
      Enumeratee.mapM{ t: T => (Enumerator.enumInput(Input.El(t)) &> enumeratee) |>>> RichIteratee.traverse[M]() }
    }
  }

  /**
   * builds an Enumeratee converter from Enumerator[T] into Enumerator[Array[Byte]]
   * Needs an implicit Converter[T, Array[Byte]]
  {{{e.through(RichEnumeratee.binarize())}}}
   */
  def binarize[T]()(implicit converter: Converter[T, Array[Byte]]): Enumeratee[T, Array[Byte]] = map[T, Array[Byte]]  

  /**
   * builds an Enumeratee converter from Enumerator[T] into Enumerator[String]
   * Needs an implicit Converter[T, String]
  {{{e.through(RichEnumeratee.stringify())}}}
   */
  def stringify[T]()(implicit converter: Converter[T, String]): Enumeratee[T, String] = {
    Enumeratee.map{ t: T => converter.convert(t) }
  }

  /**
   * builds an Enumeratee converter from Enumerator[T] into Enumerator[String] using charset
   * Needs an implicit Converter[T, String]
  {{{e.through(RichEnumeratee.stringify("utf8"))}}}
   *
   * @param charset the String charset
   */
  def stringify[T](charset: String)(implicit converter: Converter[T, Array[Byte]]): Enumeratee[T, String] = {
    binarize[T]()(converter.options(Map("charset" -> charset))) ><> Enumeratee.map{ a: Array[Byte] => new String(a, charset) }
  }

  /**
   * Builds an Enumeratee splitting chunks & aggregating them
  {{{
  // splitting/aggregating an enumerator of Array[Byte] line by line
  val e: Enumerator[Array[Byte]] = ...
  val e2: Enumerator[Array[Byte]] = e.through(RichEnumeratee.split("\n".getBytes))
  }}}
   * @param s the bytes to find for splitting
   */
  def split[T](s: Array[Byte])(implicit converterIn: Converter[T, Array[Byte]], 
    converterOut: Converter[Array[Byte], T]): Enumeratee[T, T] = {

    binarize[T] ><>
      Parsing.search(s) ><> 
      Enumeratee.grouped(lineBuilder(s)) ><>
      //Enumeratee.filterNot{ t => t.count( _ == 0x00 ) == t.length } ><> // filters all empty arrays
      Enumeratee.map{ t => converterOut.convert(t) }
  }

  /**
   * A shortcut that splits by a String
  {{{
    val e2: Enumerator[Array[Byte]] = e.through(RichEnumeratee.split("\n"))
  }}}
   * @param s the string to find for splitting
   */
  def split[T](s: String)(implicit in: Converter[T, Array[Byte]], 
    out: Converter[Array[Byte], T]): Enumeratee[T, T] = split(s.getBytes)

  /**
   * A shortcut that splits by a String and a charset
  {{{
    val e2: Enumerator[Array[Byte]] = e.through(RichEnumeratee.split("\n"))
  }}}
   * @param s the string to find for splitting
   * @param charset the charset
   */
  def split[T](s: String, charset: String)(implicit in: Converter[T, Array[Byte]], 
    out: Converter[Array[Byte], T]): Enumeratee[T, T] = split(s.getBytes(charset))(in.options(Map("charset" -> charset)), out)


  
  def parse[T, U, V](f: U => Option[V])(implicit converter: Converter[T, U]): Enumeratee[T, V] = {
    map[T, U] ><> 
      Enumeratee.map{ u: U => f(u) } ><> 
      Enumeratee.filter{ _.isDefined } ><> 
      Enumeratee.map( _.get )
  }

  def parseString[T, V](f: String => Option[V])(implicit converter: Converter[T, String]) = parse(f)

  private def lineBuilder(s: Array[Byte]): Iteratee[Parsing.MatchInfo[Array[Byte]], Array[Byte]] = (
      Enumeratee.breakE[Parsing.MatchInfo[Array[Byte]]](_.isMatch) ><>
      Enumeratee.collect{ case Parsing.Unmatched(bytes) => bytes } &>>
      Iteratee.consume() 
    ).flatMap{r => Iteratee.head.map(_ => r) }

  /*def subsplit[M[_], T, V](s: Array[Byte])(implicit in: Converter[T, Array[Byte]], 
    out: Converter[Array[Byte], V], 
    cbf: CanBuildFrom[M[_], V, M[V]]): Enumeratee[T, M[V]] = {

    val enumeratee = binarize[T] ><>
      Parsing.search(s) ><>
      Enumeratee.grouped(lineBuilder(s)) ><>
      //Enumeratee.filterNot{ t => t.count( _ == 0x00 ) == t.length } ><> // filters all empty arrays
      Enumeratee.map( out.convert(_) )

    Enumeratee.mapM{ t: T => (Enumerator.enumInput(Input.El(t)) &> enumeratee) |>>> RichIteratee.traverse[M]() }
  }

  def subsplit[M[_], T, V](s: String)(implicit in: Converter[T, Array[Byte]], 
    out: Converter[Array[Byte], V], 
    cbf: CanBuildFrom[M[_], V, M[V]]): Enumeratee[T, M[V]]  = subsplit(s.getBytes)

  def subsplit[M[_], T, V](s: String, charset: String)(implicit in: Converter[T, Array[Byte]], 
    out: Converter[Array[Byte], V], 
    cbf: CanBuildFrom[M[_], V, M[V]]): Enumeratee[T, M[V]]  = subsplit(s.getBytes(charset))(in.options(Map("charset" -> charset)), out, cbf)
  */

}


