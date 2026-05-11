# Databricks notebook source
from pyspark.sql import SparkSession
import pyspark.sql.functions as F

# COMMAND ----------

df = spark.read.csv("s3://udemy-dbt-aws-v1/ProjectOlistAwsDbt/raw/orders/olist_orders_dataset.csv", header=True, inferSchema=True)

# COMMAND ----------

df.printSchema()

# COMMAND ----------

df_orders = (
    df
    .withColumnRenamed("order_id", "id_pedido")
    .withColumnRenamed("customer_id", "id_cliente")
    .withColumnRenamed("order_status", "pedido_status")
    .withColumnRenamed("order_purchase_timestamp", "pedido_data_compra")
    .withColumnRenamed("order_approved_at", "pedido_data_aprovado")
    .withColumnRenamed("order_delivered_carrier_date", "pedido_data_entrega_transportadora")
    .withColumnRenamed("order_delivered_customer_date", "pedido_data_entrega_cliente")
    .withColumnRenamed("order_estimated_delivery_date", "pedido_data_entrega_estimada")
)

# COMMAND ----------

(
    df_orders
    .write.format("delta")
    .mode("overwrite")
    .option("path", "s3://udemy-dbt-aws-v1/ProjectOlistAwsDbt/silver/silver_orders/")
    .saveAsTable("silver_orders")
)

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS silver_order_items;