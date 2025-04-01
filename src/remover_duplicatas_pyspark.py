from pyspark.sql import SparkSession

table_name = "dwr_db.leituras_duplicadas"
partitions = [
    #"partition=20241107",
    "partition=20241108",
    # "partition=20241109",
    # "partition=20241110",
    # "partition=20241111",
    # "partition=20241112",
    # "partition=20241113",
    # "partition=20241114",
    # "partition=20241115",
    # "partition=20241116",
    # "partition=20241117",
    # "partition=20241118",
    # "partition=20241119",

]
clone_base_path = "/dwr_work/backup" #criar e dar permissao no HDFS

# Colunas para remover duplicatas
# partition_cols = ["partition]  # nomes das colunas de partition
# unique_cols = ["device_id", "session", "timestamp"]  # confirmar colunas unique se necessario para achar distinct

# Inicializa SparkSession
spark = SparkSession.builder \
    .appName("RemoveDuplicatesByPartition") \
    .enableHiveSupport() \
    .getOrCreate()

# sparkSession = (SparkSession.builder
#                 .appName("RemoveDuplicatesByPartition")
#                 .config("spark.master", "yarn")
#                 .config("spark.submit.deployMode", "cluster")
#                 .getOrCreate())
#
# sparkSession.sql("set hive.exec.dynamic.partition.mode=nonstrict")

for partition in partitions:
    query = f"SELECT * FROM {table_name} WHERE {partition.replace('=', '=\'')}\''"
    df = spark.sql(query)

    #debugs - verifica se dados teste estao presente
    df.printSchema()
    df.where('timestamp = 1731067806').orderBy('acct_session_id').show(50, False)

    # df_distinct = df.dropDuplicates(partition_cols + unique_cols)
    df_distinct = df.dropDuplicates()

    # debugs - verifica se dados teste foram apagados, restando apenas um item 
    df_distinct.printSchema()
    df_distinct.where('timestamp = 1731067806').orderBy('acct_session_id').show(50, False)

    # salva a partition-clone no HDFS
    clone_path = f"{clone_base_path}/{partition}"
    df_distinct.write.mode("overwrite").parquet(clone_path)

    print(f"Partition gravada: {partition} - {clone_path}" )

spark.stop()
