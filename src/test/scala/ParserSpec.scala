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
import scala.util.parsing.combinator._

import java.util.Date
import java.text.SimpleDateFormat
import java.util.Calendar

import play2.tools.file.DefaultImplicits._

object SysLogParser extends RegexParsers {
  override val skipWhitespace = false

  def date: Parser[Date] = """[a-zA-Z]{3} (\d\d) (\d\d):(\d\d):(\d\d)""".r ^^ { s =>
    val cal = Calendar.getInstance()
    val year = cal.get(Calendar.YEAR)
    
    cal.setTime(new java.text.SimpleDateFormat("MMM dd HH:mm:ss").parse(s))
    cal.set(Calendar.YEAR, year)

    cal.getTime
  }
 
  def host: Parser[String] = """(\S+)""".r

  def app: Parser[String] = """([^\s\[]+)""".r

  def pid: Parser[Int] = """(\d+)""".r ^^ { _.toInt }
  
  def msg: Parser[String] = """(.*)$""".r

  def spaces = """(\s+)""".r

  def logParser = ( date ~ (spaces ~> host) ~ (spaces ~> app) ~ ("[" ~> pid <~ "]:") ~ (spaces ~> msg) ) ^^ {
    case date ~ host ~ app ~ pid ~ msg => (date, host, app, pid, msg)
  }

  def apply(input: String): Option[(Date, String, String, Int, String)] = {
    println("parsing "+input)
    parse(logParser, input) match {
    case Success(result, _) => Some(result)
    case failure : NoSuccess => None /*scala.sys.error(failure.msg)*/
  }}
}


class ParserSpec extends Specification {
  "AsyncFile" should {

    /*"do it" in {    
      val l = """Aug 10 09:04:44 pvomac SubmitDiagInfo[60247]: Removed expired file file://localhost/Users/pvo/Library/Logs/DiagnosticReports/java_2012-07-10-233053_pvomac.crash"""
      val res = SysLogParser(l)
      println("res %s".format(res))
      success
    }*/

    "do it 2" in {
      val f = FileChannel("/tmp/system.log").reading.open()

      println(result(
        f.enumerator() 
          .through(RichEnumeratee.split("\n")) 
          .through(RichEnumeratee.parseString(SysLogParser.apply))
          .run(RichIteratee.toList()),
        Duration(1000, "millis")
      ))

      success
    }

  }

}