# Databricks notebook source
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from delta.tables import DeltaTable

# COMMAND ----------

df_raw_orders = spark.read.csv("s3://udemy-dbt-aws-v1/ProjectOlistAwsDbt/raw/orders/olist_orders_dataset.csv", header=True, inferSchema=True)
df_silver_order = DeltaTable.forName(spark, "silver_orders")

# COMMAND ----------

df_merge_orders = (
  df_silver_order.alias("target")
    .merge(
        df_raw_orders.alias("source"),
        "target.id_pedido = source.order_id"
    )
    .whenMatchedUpdate(
        condition = "target.pedido_status <> source.order_status",
        set = {
            "pedido_status": "source.order_status",
            "pedido_data_entrega_cliente": F.col("source.order_delivered_customer_date").cast("timestamp"),
            "pedido_data_aprovado": F.current_timestamp()
        }
    )
    .whenNotMatchedInsert(
        values = {
            "id_pedido": "source.order_id",
            "id_cliente": "source.customer_id",
            "pedido_status": "source.order_status",
            "pedido_data_compra": F.col("source.order_purchase_timestamp").cast("timestamp"),
            "pedido_data_aprovado": F.current_timestamp()
        }
    )
    .execute()
).show()
