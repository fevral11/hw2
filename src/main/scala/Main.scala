import org.apache.hadoop.fs.Path

object Main {
  def main(args: Array[String]): Unit = {
   HDFSFileService.getFile("/stage/date=2020-12-01/part-0000.csv")
  }
}
