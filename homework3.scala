/*
Usage:
spark-shell -i /home/alexey/ETL/seminar3/GeekbrainsETL/homework3.scala --conf "spark.driver.extraJavaOptions=-Dfile.encoding=utf-8"
*/
import org.apache.spark.internal.Logging
import org.apache.spark.sql.functions.{col, collect_list, concat_ws}
import org.apache.spark.sql.{DataFrame, SparkSession}
val t1 = System.currentTimeMillis()
if(1==1){
    var df1 = spark.read.format("com.crealytics.spark.excel")
        .option("sheetName", "Sheet1")
        .option("useHeader", "false")
        .option("treatEmptyValuesAsNulls", "false")
        .option("inferSchema", "true").option("addColorColumns", "true")
		.option("usePlainNumberFormat","true")
        .option("startColumn", 0)
        .option("endColumn", 99)
        .option("timestampFormat", "MM-dd-yyyy HH:mm:ss")
        .option("maxRowsInMemory", 20)
        .option("excerptSize", 10)
        .option("header", "true")
        .format("excel")
        .load("/home/alexey/ETL/seminar3/GeekbrainsETL/s3.xlsx")
	df1.show()
    val mysql_conn = "jdbc:mysql://localhost:3306/spark?user=alexey&password=astron93"
    val driver = "com.mysql.cj.jdbc.Driver"
	df1.write.format("jdbc").option("url", mysql_conn)
        .option("driver", driver).option("dbtable", "tasketl3a")
        .mode("overwrite").save()
    val my_query = s"""
WITH tb AS (SELECT 
    ID, 
    FROM_UNIXTIME(status_time, '%d.%m.%y %H:%i') status_time, 
    (LEAD(status_time) OVER(PARTITION BY ID ORDER BY status_time) - status_time)/3600 
    Status_duration,
CASE WHEN status IS NULL THEN
        @PREV1
    ELSE
        @PREV1 := status
    END  AS status,
CASE WHEN grp IS NULL THEN
        @PREV2
    ELSE
        @PREV2 := grp
    END  AS grp,
    assignment 
FROM 
(SELECT ID, status_time,  
        CASE 
            WHEN status LIKE 'Зарегистрирован' THEN 'ЗАР'
            WHEN status LIKE 'Назначен' THEN 'НЗН'
            WHEN status LIKE 'В работе' THEN 'РАБ'
            WHEN status LIKE 'Решен' THEN 'РЕШ'
        END AS status, 
        IF(ROW_NUMBER() OVER(PARTITION BY ID ORDER BY status_time) = 1 AND assignment IS NULL, '', grp)  grp, assignment  
FROM
(SELECT a.objectid ID, a.restime status_time, 
    status, grp, assignment, 
    (SELECT @PREV1:=''), (SELECT @PREV2:='')
    FROM
(SELECT DISTINCT restime,objectid  FROM spark.tasketl3a  
WHERE  fieldname IN ('GNAME2','Status')) a
LEFT JOIN 
(SELECT DISTINCT restime,objectid,fieldvalue status  FROM spark.tasketl3a  
WHERE  fieldname IN ('Status')) a1
ON a.objectid = a1.objectid AND a.restime = a1.restime
LEFT JOIN 
(SELECT DISTINCT restime,objectid,fieldvalue grp, 1 assignment  FROM spark.tasketl3a  
WHERE  fieldname IN ('GNAME2')) a2
ON a.objectid = a2.objectid AND a.restime = a2.restime) b1) b2),
tb2 AS (SELECT ID, CONCAT(status_time,'\t',status,'\t',grp) con FROM tb)
SELECT ID, GROUP_CONCAT(con SEPARATOR '\r\n') con FROM tb2
GROUP BY 1"""

    val df = spark.read.format("jdbc") 
    .option("url", mysql_conn) 
    .option("driver", "com.mysql.cj.jdbc.Driver") 
    .option("query", my_query) 
    .load()
    df.show(truncate=false)
    df.write.format("jdbc").option("url", mysql_conn)
        .option("driver", driver).option("dbtable", "result")
        .mode("overwrite").save()

	println("Homework 3")
}
val s0 = (System.currentTimeMillis() - t1)/1000
val s = s0 % 60
val m = (s0/60) % 60
val h = (s0/60/60) % 24
println("%02d:%02d:%02d".format(h, m, s))
System.exit(0)
