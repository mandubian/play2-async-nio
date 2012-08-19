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

import scala.concurrent.util.Duration

import play2.tools.file.DefaultImplicits._

class FileChannelSpec extends Specification {
  "FileChannel" >> {
    "FileChannel Reader" should {
      "read all file" in {
        val f = FileChannel("src/test/resources/test.txt").appending.create()
        f.error must beNone
        result(
           f.enumerator()
            .through(RichEnumeratee.stringify())
            .run(Iteratee.consume())
            .map( _ must equalTo("alpha\nbeta\ngamma\ndelta") ),
          Duration(1000, "millis")
        )
      }


      "read file and split by line" in {
        val f = FileChannel("src/test/resources/test.txt").open()
        f.error must beNone
        result(
           f.enumerator()
            .through(RichEnumeratee.split("\n")) 
            .through(RichEnumeratee.stringify())
            .run(RichIteratee.traverse[List]())
            .map( 
              _ must contain("alpha", "beta", "gamma", "delta").only.inOrder 
            ),
          Duration(1000, "millis")
        )
      }

      "read file and split by line with ops" in {
        val f = FileChannel("src/test/resources/test.txt").open()
        f.error must beNone
        result(
          f.enumerator()
            &> RichEnumeratee.stringify()
            &> RichEnumeratee.split("\n")
            |>>> RichIteratee.traverse[List]()
            map ( 
              _ must contain("alpha", "beta", "gamma", "delta").only.inOrder 
            ),
          Duration(1000, "millis")
        )
      }

      "read file, split by lines, subsplit in each line" in {
        val f = FileChannel("src/test/resources/test2.txt").open()
        f.error must beNone
        result(
          f.enumerator()
           &> RichEnumeratee.split("\n")
           &> RichEnumeratee.stringify()
           &> RichEnumeratee.mapThrough[List]( RichEnumeratee.split(",") )
           |>>> RichIteratee.toList()
           map ( _ must containAllOf( 
            List(
              List("alpha","alpha1","alpha2"), 
              List("beta","beta1","beta2"), 
              List("gamma","gamma1","gamma2")
            )
          )),
          Duration(1000, "millis")
        )
      }

      "retrieve a FileChannel with error on notfound file" in {
        val nf = FileChannel("notfound.txt").open()
        nf.hasError must beTrue
        nf.exists must beFalse
        nf.close
        success
      }

      "read file using withFileFuture" in {
        result(
          FileChannel.withFileFuture(FileChannel("src/test/resources/test.txt").open()) { f =>          
            f.enumerator()
             .through(RichEnumeratee.split("\n"))
             .through(RichEnumeratee.stringify())
             .run(RichIteratee.toList())
          }.map( _ must containAllOf( List("alpha", "beta", "gamma", "delta") )),
          Duration(1000, "millis")
        )
      }
    }

    "FileChannel Deleter" should {
      "delete file" in {
        result(
          FileChannel.withFileFuture(FileChannel("/tmp/testwrite.txt").writing.create()) { f =>
            Enumerator("test1", "test2", "test3").through(RichEnumeratee.binarize()).run(f.iteratee())
          }
          ,Duration(1000, "millis")
        )

        FileChannel("/tmp/testwrite.txt").delete()

        FileChannel.withFile(FileChannel("/tmp/testwrite.txt").open()) { f =>
          f.hasError must beTrue
          f.exists must beFalse
        }
      }
    }

    "FileChannel Writer" should {
      "write file with iteratee" in {
        result(
          FileChannel.withFileFuture(FileChannel("/tmp/testwrite.txt").deleteFirst.writing.create()) { f =>
            f.hasError must beFalse
            f.exists must beTrue
            Enumerator("test1", "test2", "test3")
              .through(RichEnumeratee.binarize())
              .run(f.iteratee())
              .flatMap { sz => 
                f.enumerator() &> 
                RichEnumeratee.stringify() |>>> 
                RichIteratee.traverse[List]()
              }
          }.map { _ must containAllOf(List("test1test2test3")) }, Duration(1000, "millis")
        )
      }

      "append file" in {
        result(
          FileChannel.withFileFuture(FileChannel("/tmp/testwrite2.txt").deleteFirst.writing.create()) { f =>
            Enumerator("test1", "test2", "test3")
              .through(RichEnumeratee.binarize())
              .run(f.iteratee())
          },
          Duration(1000, "millis")
        )

        result(
          FileChannel.withFileFuture(FileChannel("/tmp/testwrite2.txt").appending.open()) { f =>
            Enumerator("test4")
              .through(RichEnumeratee.binarize())
              .run(f.iteratee())
              .flatMap { sz =>
                f.enumerator() &> RichEnumeratee.stringify() |>>> RichIteratee.traverse[List]()
              }
          }.map { _ must containAllOf(List("test1test2test3test4")) }, 
          Duration(1000, "millis")
        )

      }

      "loop append file" in {
        var idx = 0
        (0 to 100).par.foreach { i =>  
          val f = FileChannel("/tmp/testwrite2.txt").writing.open()
          val str = "%10d\n".format(i)
          Enumerator(str)
            .through(RichEnumeratee.binarize())
            .run(f.iterateeFrom(11*i)).onComplete( e => f.close )
        }
        success
      }

      import scala.concurrent.Future
      import play.api.libs.concurrent.Promise

      "copy file" in {
        var i = 0
        val fileGenerator = Enumerator.fromCallback( () => 
          if(i<1000){ i+=1; Future.successful(Some("%15d\n".format((new java.util.Date).getTime))) } else Future(None) 
        )

        val f = FileChannel("/tmp/testwrite.txt").deleteFirst.writing.create()
        val f2 = FileChannel("/tmp/testwrite2.txt").deleteFirst.writing.create()

        result(
          fileGenerator                                     // generates data
            .through(RichEnumeratee.binarize())             // binarizes data to write into File
            .run(f.writer())                                // writes into file
            .flatMap{ sz => f.reader().run(f2.writer()) }  // when written, reads file and writes into dest file
            .map{ sz => println("wrote %d".format(sz)); sz must beEqualTo (16 * 1000) } // when finished, tells how many bytes were written
          , Duration(1000, "millis")
        )
      }
  }

    /*"watch file modification" in {
      running(FakeApplication()){
        val watchActor = WatchActor()
        watchActor.start
        watchActor.register(java.nio.file.Paths.get("/tmp/logs"))
        await(RichEnumerator(watchActor.enumerator).stringify.split("\n") |>>> Iteratee.foreach{ t => println("RES:'%s'".format(t)) }, 30000)

        /*println(
          await(RichEnumerator(watchActor.enumerator).stringify.split("\n").toList, 10000)
        )*/
        watchActor.stop
        success
      }
    }*/



  }
}