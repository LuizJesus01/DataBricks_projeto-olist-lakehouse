# Databricks notebook source
from pyspark.sql import SparkSession
import pyspark.sql.functions as F

# COMMAND ----------

caminho_s3 = "s3://udemy-dbt-aws-v1/ProjectOlistAwsDbt/raw/customers/olist_customers_dataset.csv"

df = spark.read.csv(caminho_s3, header=True, inferSchema=True)


# COMMAND ----------

df.limit(2).show()

# COMMAND ----------

df_silver_customers = (
    df
    .withColumnRenamed("customer_id", "id_cliente_pedido")
    .withColumnRenamed("customer_unique_id", "id_unico_cliente")
    .withColumnRenamed("customer_zip_code_prefix", "cliente_cep_prefixo")
    .withColumn("customer_city", F.initcap("customer_city"))
    .withColumnRenamed("customer_city", "cliente_cidade")
    .withColumn("customer_state", F.upper(F.col("customer_state")))
    .withColumnRenamed("customer_state", "cliente_estado")
)

# COMMAND ----------

df.printSchema()

# COMMAND ----------

(
    df_silver_customers
    .write.format("delta")
    .mode("overwrite")
    .option("path", "s3://udemy-dbt-aws-v1/ProjectOlistAwsDbt/silver/silver_customers/")
    .saveAsTable("silver_customers")
)

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS silver_customers;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE EXTENDED silver_customers;