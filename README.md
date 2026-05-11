# 🛒 Olist Data Lakehouse: AWS & Databricks 🚀

[![Databricks](https://img.shields.io/badge/Platform-Databricks-orange?style=flat-square&logo=databricks)](https://www.databricks.com/)
[![AWS](https://img.shields.io/badge/Cloud-AWS-232F3E?style=flat-square&logo=amazon-aws)](https://aws.amazon.com/)
[![Delta Lake](https://img.shields.io/badge/Format-Delta_Lake-00ADD8?style=flat-square&logo=delta-lake)](https://delta.io/)

Este projeto demonstra a construção de um **Data Lakehouse** ponta a ponta, utilizando o dataset público da Olist. O objetivo foi aplicar meus 12 anos de experiência em SQL na transição para arquiteturas de Big Data escaláveis.

## 🏗️ Arquitetura do Projeto

O pipeline segue a **Medallion Architecture** (Arquitetura Medalhão), garantindo qualidade e governança em cada etapa:

1.  **Raw Layer (S3):** Ingestão de dados brutos em formato CSV.
2.  **Silver Layer (Delta):** Limpeza, tipagem de dados e normalização utilizando **PySpark** e **Spark SQL**.
3.  **Gold Layer (Delta):** Tabelas agregadas e prontas para consumo de Business Intelligence (BI).

---

## 🛠️ Tecnologias Utilizadas

* **Cloud:** AWS (S3, IAM, Glue, Athena)
* **Processamento:** Databricks Community Edition
* **Engine:** Apache Spark (PySpark & Spark SQL)
* **Storage Format:** Delta Lake (Transações ACID e Time Travel)

---

## 🚀 Implementações Técnicas de Destaque

### 1. Governança e Transacionalidade com Delta Lake
Diferente de um Data Lake comum, utilizei o formato **Delta** para permitir:
* **Transações ACID:** Garantia de integridade nas operações de escrita.
* **Time Travel:** Capacidade de consultar versões anteriores dos dados para auditoria ou rollback.

### 2. Carga Incremental com MERGE (Upsert)
Em vez de reprocessar toda a tabela (`Overwrite`), implementei a lógica de **MERGE**. Isso permite atualizar o status de pedidos existentes e inserir novos registros em uma única operação atômica, otimizando custo e performance.

```Phyton
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
