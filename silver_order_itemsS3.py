# Databricks notebook source
from pyspark.sql import SparkSession
import pyspark.sql.functions as F

# COMMAND ----------

df = spark.read.csv("s3://udemy-dbt-aws-v1/ProjectOlistAwsDbt/raw/order_items/olist_order_items_dataset.csv", header=True, inferSchema=True)

# COMMAND ----------

df.printSchema()

# COMMAND ----------

df_order_items = (
    df
    .withColumnRenamed("order_id", "id_pedido")
    .withColumnRenamed("product_id", "id_produto")
    .withColumnRenamed("seller_id", "id_vendedor")
    .withColumnRenamed("order_item_id", "item_sequencial")
    .withColumnRenamed("shipping_limit_date", "data_limite_envio")
    .withColumnRenamed("price", "preco_unitario")
    .withColumnRenamed("freight_value","valor_frete")
)

# COMMAND ----------

(
    df_order_items
    .write.format("delta")
    .mode("overwrite")
    .option("path", "s3://udemy-dbt-aws-v1/ProjectOlistAwsDbt/silver/silver_order_items/")
    .saveAsTable("silver_order_items")
)

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS silver_order_items;

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE EXTENDED silver_products;