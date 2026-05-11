# Databricks notebook source
from pyspark.sql import SparkSession
import pyspark.sql.functions as F

# COMMAND ----------

df_orders = spark.read.table("silver_orders")
df_orders_items = spark.read.table("silver_order_items")

# COMMAND ----------

df_total_items_per_order = (
    df_orders_items
    .groupBy("id_pedido")
    .agg(F.sum("preco_unitario").alias("total_itens"),
        F.sum("valor_frete").alias("total_frete"),
        F.sum(F.col("preco_unitario")+F.col("valor_frete")).alias("valor_total_pedido"),
        F.count("id_pedido").alias("qtd_itens_total"))
    
)

# COMMAND ----------

df_int_detailed_orders = (
    df_orders
    .join(df_total_items_per_order, on="id_pedido", how="inner")
    .select("id_pedido",
            "id_cliente",
            "pedido_status",
            "pedido_data_compra",
            "pedido_data_entrega_cliente",
            F.date_diff("pedido_data_entrega_cliente","pedido_data_compra").alias("dias_para_entrega"),
            "total_itens",
            "total_frete",
            "valor_total_pedido",
            "qtd_itens_total"
            )
)

# COMMAND ----------

(
    df_int_detailed_orders
    .write.format("delta")
    .mode("overwrite")
    .option("path", "s3://udemy-dbt-aws-v1/ProjectOlistAwsDbt/silver/silver_int_detailed_orders/")
    .saveAsTable("silver_int_detailed_orders")
)

# COMMAND ----------

df_int_detailed_orders.show(3,False)