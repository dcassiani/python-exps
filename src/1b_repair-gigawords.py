import sys
from pyspark import SparkContext
from pyspark.sql import HiveContext
from pyspark.sql.functions import col, coalesce, lit

sc = SparkContext(appName="BugfixStage1")
sqlContext = HiveContext(sc)

partition = sys.argv[1]
print(partition)
contagem_max = sys.argv[2]
print(contagem_max)

table_name = "dwr_db.leituras_duplicadas"
#partition = "20250128"

query = "SELECT * FROM {} WHERE partition='{}'".format(table_name, partition)
print("Running query: " + query)
sqlContext.sql("SET hive.exec.dynamic.partition=true")
sqlContext.sql("SET hive.exec.dynamic.partition.mode=nonstrict")
df = sqlContext.sql(query)

# Debug HML only
df.printSchema()
print(df.count())

# Remove duplicatas
cleanDf = df.where( 
    (coalesce(col("INPUT_COUNTER"), lit(0)) < contagem_max) & 
    (coalesce(col("OUTPUT_COUNTER"), lit(0)) < contagem_max))

cleanDf.printSchema()
print(cleanDf.count())

sqlContext.sql(" ALTER TABLE dwr_db.leituras_duplicadas_bkp DROP IF EXISTS PARTITION(partition='"+partition+"') ")

cleanDf.write.partitionBy("partition").mode("overwrite").insertInto("dwr_db.leituras_duplicadas_bkp")

print("Partition gravada: {}".format(partition))

sc.stop()
