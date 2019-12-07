package spark

import org.apache.spark.SparkConf
import org.apache.spark.sql.SQLContext
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.DataFrame

import org.apache.log4j.Logger
import org.apache.log4j.Level

import java.util.Calendar
import scala.collection.mutable.HashMap
import org.apache.spark.SparkConf

object SparkApp2 {
  var mapa = scala.collection.mutable.Map[String, Object]()

  /**
   * Configura o Spark e coloca variáveis em um mapa
   */
  def config(): Unit = {
    System.setProperty("hadoop.home.dir", "C:/spark")

    Logger.getLogger("org").setLevel(Level.ERROR)
    Logger.getLogger("akka").setLevel(Level.ERROR)

    this.configSparkConf()
    
    this.configSparkContext(mapa("conf").asInstanceOf[SparkConf])

    this.configSession()

  }
  
  def configSparkConf():SparkConf={
    val conf = new SparkConf()
    conf.setAppName("Test").setMaster("local[*]")
    conf.set("spark.driver.allowMultipleContexts", "true")
    mapa("conf") = conf
    return conf
  }

  /**
   * Configura o Contexto do Spark
   */
  def configSparkContext(conf: SparkConf): SparkContext = {
    var sc = new SparkContext(conf)
    mapa("sc") = sc
    return sc
  }

  /**
   * Configura a Sessão do Spark e colocal em um mapa para reutilização
   */
  def configSession(): SparkSession = {
    val session = org.apache.spark.sql.SparkSession.builder
      .master("local")
      .appName("Spark CSV Reader")
      .getOrCreate
    mapa("session") = session
    return session
  }

  def getSession(): SparkSession = {
    return mapa("session").asInstanceOf[SparkSession]
  }

  def stopApp() {
    Logger.getLogger(getClass.getName).info("Stopping Spark")

    mapa("sc").asInstanceOf[SparkContext].stop()
  }

  def load_tsv(fileName: String, nameView: String): DataFrame = {
    var spark = getSession()

    val df = spark.read.format("csv").option("header", "true")
      .option("mode", "DROPMALFORMED")
      .option("delimiter", "\t")
      .load(fileName)

    df.createOrReplaceTempView(nameView)
    return df
  }

}