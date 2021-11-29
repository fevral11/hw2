import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs._

import java.io.{BufferedInputStream, File, FileInputStream}
import java.net.URI

object HDFSFileService {
  val conf = new Configuration()
  val fileSystem = FileSystem.get(new URI("hdfs://localhost:9000"), conf)
  val path = new Path("/stage/date=2020-12-01/part-0000.csv")
  val newPath = new Path("/ods")



  def saveFile(filepath: String) = {
    val file = new File(filepath)
    val out = fileSystem.create(new Path(file.getName))
    val in = new BufferedInputStream(new FileInputStream(file))
    var b = new Array[Byte](1024)
    var numBytes = in.read(b)
    while (numBytes > 0) {
      out.write(b, 0, numBytes)
      numBytes = in.read(b)
    }
    in.close()
    out.close()
  }

  def removeFile(filename: String) = {
    val path = new Path(filename)
    fileSystem.delete(path, true)
  }


  def getFile(filename: String) = {
    val path = new Path(filename)
    fileSystem.open(path)
  }

  def createFolder(folderPath: String) = {
    val path = new Path(folderPath)
    if (!fileSystem.exists(path)) {
      fileSystem.mkdirs(path)
    }
  }

  def createFolders() = {
    val listFiles = fileSystem.copyFromLocalFile(path, newPath)

  }
}
