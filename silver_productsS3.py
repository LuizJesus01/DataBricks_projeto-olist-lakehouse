# Databricks notebook source
from pyspark.sql import SparkSession
import pyspark.sql.functions as F

# COMMAND ----------

df = spark.read.csv("s3://udemy-dbt-aws-v1/ProjectOlistAwsDbt/raw/products/olist_products_dataset.csv", header=True, inferSchema=True)

# COMMAND ----------

df.printSchema()

# COMMAND ----------

df_silver_products = (
    df
    .withColumnRenamed("product_id", "id_produto")
    .withColumn("product_category_name", F.coalesce(F.col("product_category_name"), F.lit("nao_informado")))
    .withColumnRenamed("product_category_name", "categoria_nome")
    .withColumnRenamed("product_weight_g", "peso_gramas")
    .withColumnRenamed("product_length_cm", "comprimento_cm")
    .withColumnRenamed("product_height_cm", "altura_cm")
    .withColumnRenamed("product_width_cm", "largura_cm")
)

# COMMAND ----------

(
    df_silver_products
    .write.format("delta")
    .mode("overwrite")
    .option("path", "s3://udemy-dbt-aws-v1/ProjectOlistAwsDbt/silver/silver_products/")
    .saveAsTable("silver_products")
)

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS silver_products;

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS `workspace`.`default`.`silver_products`;