package zpark.zuoye

import java.io.PrintWriter

import scala.io.{BufferedSource, Source}

/**
 * @author ys
 *         data 2020/7/1 10:05
 */
object ShiXun3ReadUrlUtil {
    def main(args: Array[String]) {
      //获取抽样参数的url
      val url="http://v.juhe.cn/toutiao/index?type=top&key=698c21dd69894b36d683a1da79b548b2"
      //链接url，获取返回值
      val fileContent = Source.fromURL(url,"utf-8").mkString
      //写入本地磁盘
      val pw = new PrintWriter("in/totio.json")
      pw.write(fileContent)
      //pw.write("\r\n")
      pw.flush
      pw.close
    }

    //可以从InputStream中读取
    def inputToString(is: java.io.InputStream): String = {
      val lines: Iterator[String] = scala.io.Source.fromInputStream(is, "utf-8").getLines()
      val sb = new StringBuilder()
      lines.foreach(sb.append(_))
      sb.toString()
    }

    //将输入流写入文件(test.txt)中
    //参数f---> val file = new File("F:/test.txt")
    def inputToFile(is: java.io.InputStream, f: java.io.File) {
      val in: BufferedSource = scala.io.Source.fromInputStream(is)
      val out = new java.io.PrintWriter(f)
      try {
        in.getLines().foreach(out.print(_))//等价write，只是多了一句if(s==null)s="null"
      }
      finally {
        out.close
      }
    }





}
