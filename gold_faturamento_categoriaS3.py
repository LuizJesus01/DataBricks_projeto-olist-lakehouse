# Databricks notebook source
from pyspark.sql import SparkSession
import pyspark.sql.functions as F

# COMMAND ----------

df_produto = spark.read.table("silver_products")
df_order_items = spark.read.table("silver_order_items")

# COMMAND ----------

df_gold_faturamento_categoria = (
    df_order_items
    .join(df_produto, on="id_produto", how="inner")
    .groupBy("categoria_nome")
    .agg(
        F.round(F.sum("preco_unitario"),2).alias("faturamento_total"),
        F.count("id_pedido").alias("total_vendas")
    )
)

# COMMAND ----------

(
    df_gold_faturamento_categoria
    .write.format("delta")
    .mode("overwrite")
    .option("path", "s3://udemy-dbt-aws-v1/ProjectOlistAwsDbt/gold/gold_faturamento_categoria/")
    .saveAsTable("gold_faturamento_categoria")
)

# COMMAND ----------

df_produto.show(2,False)

# COMMAND ----------

df_order_items.show(2,False)