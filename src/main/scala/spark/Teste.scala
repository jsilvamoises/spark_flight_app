package spark

import org.apache.spark.SparkConf
import org.apache.spark.sql.SQLContext
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import scala.util.control.Exception.Catch

import org.apache.log4j.Logger
import org.apache.log4j.Level

import java.util.Calendar

object Teste {
  var inicio = Calendar.getInstance().getTime()
  def main(args: Array[String]) {

    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    System.setProperty("hadoop.home.dir", "C:/spark-3.0.0-preview-bin-hadoop2.7")
    
    
    val conf = new SparkConf().setAppName("Test").setMaster("local[*]")
    conf.set("spark.driver.allowMultipleContexts", "true")

    val sc = new SparkContext(conf)

    val spark = org.apache.spark.sql.SparkSession.builder
      .master("local")
      .appName("Spark CSV Reader")
      .getOrCreate;

    val tsvfile = spark.read.format("csv").option("header", "true")
      .option("mode", "DROPMALFORMED")
      .option("delimiter", "\t")
      .load("2004-2019.tsv")

    tsvfile.createOrReplaceTempView("gas")

    var df4 = spark.sqlContext.sql("select (*)  from gas").show(100)

    print("contagem")
    var df46 = spark.sqlContext.sql("select COUNT(*)  from gas").show()
    
     print("inicia com s")
    var df = spark.sqlContext.sql("Select * from gas where estado like 'S%'").orderBy("estado").show(5)
    


    // Exercicios 1
     print("todos limite")
    var todos_limite_5 = spark.sqlContext.sql("Select * from gas").show(5)

    // Exercicio 2
     print("sao paulo")
    var sao_paulo = spark.sqlContext.sql("Select * from gas where ESTADO like UPPER('SAO PAULO')").show()

    // Exercicio 3
    var registros = spark.sqlContext.sql("Select count(*) from gas").show()

    // Exercicio 4
     print("Centro Oeste")
    var total_centro_oeste = spark.sqlContext.sql("Select REGIAO,Count(1) from gas group by 1").show()

    
     print("preço médio por estado")
    var df2 = spark.sqlContext.sql("select ano,avg(PRECO_MEDIO_DISTRIBUICAO) as preco_medio from gas GROUP BY ANO").show()

    
    
    var df3 = spark.sqlContext.sql("select (sum(NUMERO_DE_POSTOS_PESQUISADOS) / sum(PRECO_MEDIO_REVENDA)) AS MEDIA from gas").show()
    print("distinct")
    var df44 = spark.sqlContext.sql("select DISTINCT *   from gas").show()

    var df6 = spark.sqlContext.sql("select ESTADO,MAX(PRECO_MEDIO_REVENDA)  from gas GROUP BY 1").show(27)
    var df10 = spark.sqlContext.sql("select REGIAO,MAX(PRECO_MEDIO_REVENDA)  from gas GROUP BY 1").show()

    var df7 = spark.sqlContext.sql("select DISTINCT DATA_INICIAL,DATA_FINAL  from gas").count()

    var df8 = spark.sqlContext.sql("select PRODUTO, MAX(PRECO_MEDIO_REVENDA),MIN(PRECO_MEDIO_REVENDA)  from gas GROUP BY 1").show()

    val df11 = spark.sqlContext.sql("select SUM(NUMERO_DE_POSTOS_PESQUISADOS) from gas").show()
    
    var fim = Calendar.getInstance().getTime()
    var tempo = fim.getSeconds() - inicio.getSeconds()
    print(tempo)
    sc.stop()
  }

}