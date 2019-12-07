package spark

object Process {
  def main(args: Array[String]) {
    var app = SparkApp2
    app.config()
    var df = app.load_tsv("2004-2019.TSV", "gas")

    var sql = app.getSession()

    //sql.sqlContext.sql("select * from gas").show(5)

    //sql.sqlContext.sql("select count(*) from gas").show()
    // sql.sqlContext.sql("select * from gas where UPPER(ESTADO) LIKE 'SAO PAULO'").show()

    // sql.sqlContext.sql("select count(*) from gas").show()

    // sql.sqlContext.sql("select REGIAO, count(1) from gas where REGIAO LIKE 'CENTRO OESTE' GROUP BY 1").show()
    //sql.sqlContext.sql("select REGIAO, SUM(NUMERO_DE_POSTOS_PESQUISADOS) AS NR_POSTOS from gas GROUP BY 1").show()

    // sql.sqlContext.sql("select ANO, AVG(PRECO_MEDIO_DISTRIBUICAO) AS PRECO_MEDIO from gas GROUP BY 1 ORDER BY ANO").show()

    //sql.sqlContext.sql("select SUM(NUMERO_DE_POSTOS_PESQUISADOS) AS TT_POSTOS,(SUM(PRECO_MEDIO_REVENDA)/SUM(NUMERO_DE_POSTOS_PESQUISADOS)) AS PRECO_MEDIO from gas").show()
    //sql.sqlContext.sql("select count(*) from (select distinct * from gas)").show()
    //sql.sqlContext.sql("select distinct * from gas").show()
    // sql.sqlContext.sql("select ESTADO,MAX(PRECO_MEDIO_REVENDA) from gas GROUP BY 1 ORDER BY 1").show()
    // sql.sqlContext.sql("select REGIAO,MAX(PRECO_MEDIO_REVENDA) from gas GROUP BY 1 ORDER BY 1").show()

    /*sql.sqlContext.sql("""
     select SUM(DATEDIFF(DATA_FINAL,DATA_INICIAL)) AS TOTAL_DIAS FROM(
           select DISTINCT DATA_INICIAL, DATA_FINAL from gas) 
     """).show()
     
     sql.sqlContext.sql("""     
           select DISTINCT DATA_INICIAL, DATA_FINAL from gas
     """).show()
    app.stopApp()*/
    /*
    sql.sqlContext.sql("""
      select 
          PRODUTO,MAX(PRECO_MEDIO_REVENDA) AS PRECO_MAXIMO, 
          MIN(PRECO_MEDIO_REVENDA) AS PRECO_MINIMO 
      from gas 
      GROUP BY 1 
      ORDER BY 1
      """).show()
      
      */
      sql.sqlContext.sql("""
      select 
          MES,MAX(PRECO_MEDIO_REVENDA) AS PRECO_MAXIMO, 
          MIN(PRECO_MEDIO_REVENDA) AS PRECO_MINIMO 
      from gas 
      GROUP BY 1 
      ORDER BY 1
      """).show()
  }
}