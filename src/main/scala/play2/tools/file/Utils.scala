package play2.tools.file.utils

/** Common functions */
object Converters {
  private val HEX_CHARS :Array[Char] = "0123456789abcdef".toCharArray();

  /** Turns an array of Byte into a String representation in hexadecimal. */
  def hex2Str(bytes: Array[Byte]) :String = {
    val hex = new Array[Char](2 * bytes.length)
    var i = 0
    while(i < bytes.length) {
      hex(2 * i) = HEX_CHARS((bytes(i) & 0xF0) >>> 4)
      hex(2 * i + 1) = HEX_CHARS(bytes(i) & 0x0F)
      i = i + 1
    }
    new String(hex)
  }

  /** Turns a hexadecimal String into an array of Byte. */
  def str2Hex(str: String) :Array[Byte] = {
    val bytes = new Array[Byte](str.length / 2)
    var i = 0
    while(i < bytes.length) {
      bytes(i) = Integer.parseInt(str.substring(2*i, 2*i+2), 16).toByte
      i += 1
    }
    bytes
  }

  /** Computes the MD5 hash of the given String. */
  def md5(s: String) = java.security.MessageDigest.getInstance("MD5").digest(s.getBytes)

  /** Computes the MD5 hash of the given Array of Bytes. */
  def md5(array: Array[Byte]) = java.security.MessageDigest.getInstance("MD5").digest(array)

  /** Computes the MD5 hash of the given String and turns it into a hexadecimal String representation. */
  def md5Hex(s: String) :String = hex2Str(md5(s))
}

object ArrayUtils {
  /** Concats two array - fast way */ // TODO
  def concat[T](a1: Array[T], a2: Array[T])(implicit m: Manifest[T]) :Array[T] = {
    var i, j = 0
    val result = new Array[T](a1.length + a2.length)
    while(i < a1.length) {
      result(i) = a1(i)
      i = i + 1
    }
    while(j < a2.length) {
      result(i + j) = a2(j)
      j = j + 1
    }
    result
  }
}