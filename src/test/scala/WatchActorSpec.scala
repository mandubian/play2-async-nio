import org.specs2.mutable._
import play.api.test._
import play.api.test.Helpers._
import org.junit.runner.RunWith
import org.specs2.runner.JUnitRunner

import java.nio.file._
import java.nio.ByteBuffer

import play2.tools.file._
import play.api.libs.iteratee._
import play2.tools.iteratee._
import scala.concurrent.Await._

import scala.concurrent.util.Duration
import scala.concurrent.{Scheduler, Promise}
import scala.concurrent.Await

import play2.tools.file.DefaultImplicits._

class WatchActorSpec extends Specification {
  "WatchActor" should {
    "watch directory" in {
      running(FakeApplication()){

        val watchActor = WatchActor()
        watchActor.start
        watchActor.register(java.nio.file.Paths.get("/tmp/logs"))
        
        try {
        Await.result(
          watchActor.enumerator &> 
            RichEnumeratee.stringify() &> 
            RichEnumeratee.split("\n") |>>> 
            Iteratee.foreach{ t => println("RES:'%s'".format(t)) },
          Duration(5, "seconds")
        )
        } catch {
          case e => println("caught exception: %s".format(e))
        } finally {
        
        /*play.api.libs.concurrent.Promise.timeout({
          watchActor.stop
        }, 5000)*/

          watchActor.stop
          
        }

        success
      }
    }
  }
}