import org.specs2.mutable._
import play.api.test._
import play.api.test.Helpers._
import play.api.libs.json._
import org.junit.runner.RunWith
import org.specs2.runner.JUnitRunner

import java.nio.file._
import java.nio.ByteBuffer

import play2.tools.file._
import play.api.libs.iteratee._
import play2.tools.iteratee._
import scala.concurrent.Await._

import play2.tools.file.DefaultImplicits._

class AsyncFileSpec extends Specification {
  "AsyncFile" should {

    "read file" in {
      val f = FileChannel("/Volumes/PVO/workspaces/workspace_mandubian/play2-async-nio/src/test/resources/test.txt").open
      f.error must beNone
      await(
        f.enumerate().split("\n").stringify.toList.map( _ must containAllOf( List("alpha", "beta", "gamma", "delta") )),
        1000
      )
    }

    "read file, split by lines, subsplit in each line" in {
      val f = FileChannel("/Volumes/PVO/workspaces/workspace_mandubian/play2-async-nio/src/test/resources/test2.txt").open
      f.error must beNone
      await(
        f.enumerate().stringify.split("\n").subsplit[List](",").toList.map( _ must containAllOf( 
          List(
            List("alpha","alpha1","alpha2"), 
            List("beta","beta1","beta2"), 
            List("gamma","gamma1","gamma2")
          )
        )),
        1000
      )
    }

    "read file with for comprehension" in {
      val f = FileChannel("/Volumes/PVO/workspaces/workspace_mandubian/play2-async-nio/src/test/resources/test.txt").open
      val l = for( el <- f.enumerate().split("\n").stringify) yield(el)
      println("l:"+l)
      f.close
      success
    }

    "retrieve a FileChannel with error on notfound file" in {
      val nf = FileChannel.open("notfound.txt")
      nf.hasError must beTrue
      nf.exists must beFalse
      nf.close
      success
    }

    "read file using withFile" in {
      FileChannel.withFile("/Volumes/PVO/workspaces/workspace_mandubian/play2-async-nio/src/test/resources/test.txt") { f =>
        await(
          f.enumerate().split("\n").stringify.toList.map( _ must containAllOf( List("alpha", "beta", "gamma", "delta") )),
          1000
        )
      }
    }

    "write file with iteratee" in {
      FileChannel.withFile(FileChannel("/tmp/testwrite.txt").writing.create) { f =>
        await(
          RichEnumerator(Enumerator("test111", "test211", "test311")).mapTo[Array[Byte]] |>>> f.iteratee(),
          1000
        )

        await(
          f.enumerate().stringify.traverse[List].map{ _ must containAllOf(List("test111test211test311")) },
          1000
        )
      }
    }

    /*"watch file modification" in {
      running(FakeApplication()){
        val watchActor = WatchActor()
        watchActor.start
        watchActor.register(java.nio.file.Paths.get("/tmp/test"))
        //await(RichEnumerator(watchActor.enumerator).stringify.split("\n") |>>> Iteratee.foreach{ t => println("RES:'%s'".format(t)) }, 30000)

        println(
          await(RichEnumerator(watchActor.enumerator).stringify.split("\n").toList, 10000)
        )
        watchActor.stop
        success
      }
    }*/

      //f2.enumerate().split("\n").stringify.subsplit[List](",").mapTo[List[String]].foreach(println(_))

      //val fw = FileChannel.open("/tmp/testwrite.txt", 
      //  Set(StandardOpenOption.CREATE: OpenOption, StandardOpenOption.WRITE: OpenOption)
      //)
      /*val fw = FileChannel("/tmp/testwrite.txt").writeable.create
      if(fw.isWritable)
        await(RichEnumerator(Enumerator("test111", "test211", "test311")).mapTo[Array[Byte]] |>>> fw.iteratee(), 1000)
      else println("NOT WRITABLE")

      fw.close*/

      /*val fw2 = FileChannel("/tmp/testwrite2.txt").create

      val fw3 = FileChannel("/tmp/testwrite2.txt").appending.open
      if(fw3.hasError) println("ERRORS:"+fw3.error)
      else RichEnumerator(Enumerator("11", "22", "33")).mapTo[Array[Byte]] |>>> fw3.iteratee()

      val fw4 = FileChannel("/tmp/testwrite2.txt").writing.open
      if(fw4.hasError) println("ERRORS:"+fw4.error)
      else await(RichEnumerator(Enumerator("44", "55", "66")).mapTo[Array[Byte]] |>>> fw4.iteratee(), 1000)
      
      val fw5 = FileChannel("/tmp/testwrite3.txt").appending.open
      fw5.write(Enumerator("AAAA", "BBBB", "CCCC"))
      val fw6 = FileChannel("/tmp/testwrite3.txt").appending.open
      fw6.write(Enumerator("DDDD", "EEEE", "FFFF")).map( _ must equalTo(12L))
      */
      
      // Dangerous, you don't know what will be written and where!
      /*val fw7 = FileChannel("/tmp/testwrite4.txt").appending.open
      val fw8 = FileChannel("/tmp/testwrite4.txt").appending.open
      val fw9 = FileChannel("/tmp/testwrite4.txt").appending.open
      RichEnumerator(Enumerator("alpha", "beta", "gamma", "\n")).mapTo[Array[Byte]] |>>> fw7.iteratee()
      RichEnumerator(Enumerator("delta", "epsilon", "zeta", "\n")).mapTo[Array[Byte]] |>>> fw8.iteratee()
      RichEnumerator(Enumerator("eta", "theta", "iota", "\n")).mapTo[Array[Byte]] |>>> fw9.iteratee()*/

      //fw3.close
      //println("WRITE RESULT:"+await(fw2.enumerate().split("\n").stringify.toList, 1000))

      //fw.close
      //f must beRight
      //f.right.map( _.enumerate().split("\n").stringify |>> Iteratee.consume() )
      
      /*val p = f.right.map( _.enumerate().split("\n").stringify.toList ).right.get
      p.onSuccess{ case t => println(t) }

      val f2 = FileChannel.open("/Volumes/PVO/workspaces/workspace_mandubian/play2-async-file/src/test/resources/test2.txt")
      f2 must beRight
      
      val p2 = f2.right.map( _.enumerate().split("\n").stringify.subsplit[List](",").toList ).right.get
      await(p2.map{ t => t must containAllOf(
        List(
          List("alpha", "alpha1", "alpha2"), 
          List("beta", "beta1", "beta2"), 
          List("gamma", "gamma1", "gamma2")
        )
      ) }, 1000)*/

      //fw5.close
      //fw6.close
      //fw2.close
      //fw3.close


  }
}