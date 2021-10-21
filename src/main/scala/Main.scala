import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, FileUtil, Path}
import org.apache.spark.sql.catalyst.util.DateFormatter
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

import java.io.File
import java.time.format.DateTimeFormatter
import java.time.{LocalDate, LocalDateTime, LocalTime}

object Main extends App {

  val spark = SparkSession.builder().master("local[*]").appName("Writing to single file").getOrCreate()

  def process(implicit spark: SparkSession): Unit = {

    spark.sparkContext.setLogLevel("ERROR")
    spark.sparkContext.setCheckpointDir("/tmp")

    import spark.implicits._

    val df1 = Seq(("Java", "20000"), ("Python", "100000"), ("Scala", "3000"), ("R", "14000") ).toDF("Course", "Level")

    writeFile(df1, "final")
    writeTable(df1, "singlefilesbt")

    //readTable(spark, "singlefilesbt")


  }

  process(spark)

  spark.stop()

  def writeFile(df: DataFrame, fileName: String): Unit = {

    df.coalesce(1).write.mode(SaveMode.Overwrite).option("header", "true").csv(fileName)

    val timestamp = LocalDateTime.now().format( DateTimeFormatter.ofPattern("yyyyMMdd"))

    val hadoopConfig = new Configuration()
    val hdfs = FileSystem.get(hadoopConfig)

    val srcPath = new Path(fileName)
    val destFile = "1002_" + fileName + "_" + timestamp + ".csv"
    val destPath = new Path("output/" + destFile)

    val srcFile = FileUtil.listFiles(new File(fileName)).filter( file => file.getPath.endsWith(".csv"))(0)

    val tempPath = new Path(srcFile.toString)

    //FileUtil.copy(srcFile, hdfs, destPath, true, hadoopConfig)
    FileUtil.copy(hdfs, tempPath, hdfs, destPath, true, true, hadoopConfig)

    val crcFile = "." + destFile + ".crc"

    hdfs.delete(new Path("output/" + crcFile), true)

    hdfs.delete(srcPath, true)

  }

  def writeTable(df: DataFrame, tablename: String): Unit = {

    df.write
      .format("jdbc")
      .options( Map("url" -> "jdbc:postgresql://localhost:5432/sakksoftware", "user" -> "sakksoftware", "password" -> "", "dbtable" -> s"app.$tablename") )
      .save()

  }

  def readTable (spark: SparkSession, tablename: String): DataFrame = {

    spark.read
      .format("jdbc")
      .option("url", "jdbc:postgresql://localhost:5432/sakksoftware")
      .option("dbtable", s"app.$tablename")
      .option("user", "sakksoftware")
      .option("password", "")
      .load()

  }

}
