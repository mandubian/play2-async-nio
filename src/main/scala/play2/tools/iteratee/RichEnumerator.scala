package play2.tools.iteratee

import scala.concurrent.{Future, Promise, ExecutionContext}
import scala.collection.generic.CanBuildFrom
import scala.util.parsing.combinator.Parsers

import play.api.libs.iteratee._
import play.api.libs.concurrent._
import play2.tools.file.utils.Converters


trait Converter[From, To]{
  def convert(from: From): To
}

trait RichEnumerator[T] extends Enumerator[T] {
  
  def foreach(f: T => Unit): Unit = this |>> Iteratee.foreach(f)

  def mapTo[M[V], X, V](implicit p: T => scala.collection.TraversableLike[X, T], cbft: CanBuildFrom[T, X, T], cbfv: CanBuildFrom[M[_], V, M[V]], converter: Converter[X, V] ): RichEnumerator[M[V]] = {
    RichEnumerator((this &> Enumeratee.map{ t: T => 
      val builder = cbfv.apply
      p(t).foreach{ x:X => builder += converter.convert(x) } 
      builder.result
    }))
  }

  def mapTo[V](implicit converter: Converter[T, V]): RichEnumerator[V] = {
    RichEnumerator((this &> Enumeratee.map{ converter.convert(_) }))
  }

  def stringify(implicit converter: Converter[T, String]): RichEnumerator[String] = mapTo[String]  

  def traverseIteratee[M[_], T](implicit cbf: CanBuildFrom[M[_], T, M[T]]) = Iteratee.fold(cbf.apply){ (builder, e: T) => builder += e }.map(_.result)

  def traverse[M[_]]()(implicit cbf: CanBuildFrom[M[_], T, M[T]]): Future[M[T]] = {
    this |>>> traverseIteratee
  }

  def traverse[M[_]](nb: Int)(implicit cbf: CanBuildFrom[M[_], T, M[T]]): Future[M[T]] = {
    this &> Enumeratee.take(nb) |>>> traverseIteratee
  }

  def toList: Future[List[T]] = traverse[List]()

  def lineBuilder(s: Array[Byte]): Iteratee[Parsing.MatchInfo[Array[Byte]], Array[Byte]] = (
      Enumeratee.breakE[Parsing.MatchInfo[Array[Byte]]](_.isMatch) ><>
      Enumeratee.collect{ case Parsing.Unmatched(bytes) => bytes } &>>
      Iteratee.consume() 
    ).flatMap{r => Iteratee.head.map(_ => r) }

  def split(s: Array[Byte])(implicit converter: Converter[T, Array[Byte]], converterOut: Converter[Array[Byte], T]): RichEnumerator[T] = {
    val enumeratee = Enumeratee.map{ a: T => converter.convert(a) } ><>
      Parsing.search(s) ><> 
      Enumeratee.grouped(lineBuilder(s)) ><>
      //Enumeratee.filterNot{ t => t.count( _ == 0x00 ) == t.length } ><> // filters all empty arrays
      Enumeratee.map{ t => converterOut.convert(t) }

    RichEnumerator( (this &> enumeratee))
  }

  def split(s: String)(implicit in: Converter[T, Array[Byte]], out: Converter[Array[Byte], T]): RichEnumerator[T] = split(s.getBytes)

  def subsplit[M[_]](s: Array[Byte])
    (implicit in: Converter[T, Array[Byte]], 
              out: Converter[Array[Byte], T], 
              cbf: CanBuildFrom[M[_], T, M[T]]): RichEnumerator[M[T]] = {
    val enumeratee = Enumeratee.map{ a: T => in.convert(a) } ><>
      Parsing.search(s) ><> 
      Enumeratee.grouped(lineBuilder(s)) ><>
      //Enumeratee.filterNot{ t => t.count( _ == 0x00 ) == t.length } ><> // filters all empty arrays
      Enumeratee.map( out.convert(_) )

    RichEnumerator((this &> 
      Enumeratee.mapM { e => (Enumerator.enumInput(Input.El(e)) &> enumeratee) |>>> traverseIteratee })
    )
  }

  def subsplit[M[_]](s: String)
    (implicit in: Converter[T, Array[Byte]], 
              out: Converter[Array[Byte], T], 
              cbf: CanBuildFrom[M[_], T, M[T]]): RichEnumerator[M[T]] = subsplit(s.getBytes)


  def parse[U, V](f: U => Option[V])(implicit converter: Converter[T, U]): RichEnumerator[V] = {
    RichEnumerator(this.mapTo[U] &> 
      ( Enumeratee.map{ u: U => f(u) } ><> Enumeratee.filter{ _.isDefined } ><> Enumeratee.map( _.get ) )
    )
  }

  def parseString[V](f: String => Option[V])(implicit converter: Converter[T, String]): RichEnumerator[V] = parse(f)
}

object RichEnumerator {
  def apply[E](inner: Enumerator[E]) = new RichEnumerator[E] {
     def apply[A](i: Iteratee[E, A]): Future[Iteratee[E, A]] = inner(i)
  }

}

object ImplicitConverters extends Converters

trait Converters {
  implicit object ArrayToString extends Converter[Array[Byte], String] {
    def convert(a: Array[Byte]): String = new String(a)
  }

  implicit object ArrayToArray extends Converter[Array[Byte], Array[Byte]] {
    def convert(a: Array[Byte]): Array[Byte] = a
  }

  implicit object StringToArray extends Converter[String, Array[Byte]] {
    def convert(s: String): Array[Byte] = s.getBytes
  }

  implicit object StringToString extends Converter[String, String] {
    def convert(s: String): String = s
  }

  implicit def traversableConverter[M[_] <: Traversable[_], T, V](implicit cbft: CanBuildFrom[M[_], T, M[T]], cbfv: CanBuildFrom[M[_], V, M[V]], converter: Converter[T, V]) = {
    new Converter[M[T], M[V]] {
      def convert(mt: M[T]): M[V] = {
        mt.foldLeft(cbfv.apply){ case (builder, t: T) => builder += converter.convert(t) }.result
      }
    }
  }

  /*implicit def identity[T] = new Converter[T, T] {
    def convert(t: T): T = t
  }*/
}