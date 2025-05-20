import os
from pyspark.sql import SparkSession
import requests
from tqdm import tqdm
import tarfile
import logging
import time
from pyspark.sql.functions import explode, from_json, col
from pyspark.sql.types import ArrayType, StructType, StructField, StringType, DoubleType, IntegerType, BooleanType, TimestampType
import sys

# Configurações globais (ex: logging)
logging.basicConfig(
    level=logging.INFO,
    format='[%(levelname)s] %(message)s'
)

# Funções utilitárias


def download_files(name, url, dst):

    if os.path.exists(dst):
        logging.info(f"Alredy exist: {name} ")
        return

    try:
        logging.info(f"Downloading: {name} ")
        resposta = requests.get(url, stream=True)
        resposta.raise_for_status()
        with open(dst, "wb") as f:
            for chunk in tqdm(resposta.iter_content(chunk_size=8192), desc=f"⬇{name}"):
                f.write(chunk)
        logging.info(f"Downloaded: {name} ")

    except Exception as e:
        logging.error(f"Download error: {name} : {e}")

def extract_tar_gz(file_path, target_dir="data"):
    try:
        with tarfile.open(file_path, "r:gz") as tar:
            all_members = tar.getnames()
            logging.info("Files in archive .tar:")
            for name in all_members:
                logging.info(f"  → {name}")

            visible_names = [name for name in all_members if not name.startswith("._")]
            visible_members = [member for member in tar if member.name in visible_names]

            tar.extractall(path=target_dir, members=visible_members)
            logging.info("Extraction completed successfully.")

    except Exception as e:
        logging.error(f"Failed to extract {file_path}: {e}")

def load_dataframe(spark, file_path, file_type="csv", has_header=True, schema=None):
    try:
        if file_type == "json":
            reader = spark.read.schema(schema) if schema else spark.read
            return reader.json(file_path)
        elif file_type == "csv":
            reader = spark.read.option("header", has_header)
            reader = reader.schema(schema) if schema else reader
            return reader.csv(file_path)
        else:
            raise ValueError(f"Not supported: {file_type}")
    except Exception as e:
        logging.error(f"Load fail {file_path}: {e}")
        return None


def save_dataframe_as_parquet(df, output_path, overwrite=True):
    try:
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        mode = "overwrite" if overwrite else "error"

        start = time.time()
        df.repartition(1).write.mode(mode).parquet(output_path)
        duration = round(time.time() - start, 2)

        logging.info(f"File saved: {output_path} in {duration}s")
    except Exception as e:
        logging.error(f"Error {output_path}: {e}")

order_schema = StructType([
    StructField("cpf", StringType(), True),
    StructField("customer_id", StringType(), True),
    StructField("customer_name", StringType(), True),
    StructField("delivery_address_city", StringType(), True),
    StructField("delivery_address_country", StringType(), True),
    StructField("delivery_address_district", StringType(), True),
    StructField("delivery_address_external_id", StringType(), True),
    StructField("delivery_address_latitude", DoubleType(), True),
    StructField("delivery_address_longitude", DoubleType(), True),
    StructField("delivery_address_state", StringType(), True),
    StructField("delivery_address_zip_code", StringType(), True),
    StructField("items", StringType(), True),
    StructField("merchant_id", StringType(), True),
    StructField("merchant_latitude", DoubleType(), True),
    StructField("merchant_longitude", DoubleType(), True),
    StructField("merchant_timezone", StringType(), True),
    StructField("order_created_at", TimestampType(), True),
    StructField("order_id", StringType(), True),
    StructField("order_scheduled", BooleanType(), True),
    StructField("order_total_amount", DoubleType(), True),
    StructField("origin_platform", StringType(), True),
    StructField("order_scheduled_date", TimestampType(), True)
])

consumer_schema = StructType([
    StructField("customer_id", StringType(), True),
    StructField("language", StringType(), True),
    StructField("created_at", TimestampType(), True),
    StructField("active", BooleanType(), True),
    StructField("customer_name", StringType(), True),
    StructField("customer_phone_area", StringType(), True),
    StructField("customer_phone_number", StringType(), True)
])

restaurant_schema = StructType([
    StructField("id", StringType(), True),
    StructField("created_at", TimestampType(), True),
    StructField("enabled", BooleanType(), True),
    StructField("price_range", IntegerType(), True),
    StructField("average_ticket", DoubleType(), True),
    StructField("takeout_time", IntegerType(), True),
    StructField("delivery_time", DoubleType(), True),   
    StructField("minimum_order_value", DoubleType(), True),
    StructField("merchant_zip_code", StringType(), True),
    StructField("merchant_city", StringType(), True),
    StructField("merchant_state", StringType(), True),
    StructField("merchant_country", StringType(), True)
])

abtest_schema = StructType([
    StructField("customer_id", StringType(), True),
    StructField("is_target", StringType(), True)

])

#Configuração do Ambiente
os.environ["PYSPARK_SUBMIT_ARGS"] = "--conf spark.driver.memory=4g pyspark-shell"


spark = SparkSession.builder \
    .appName("ifood_ab_test_ingestion") \
    .getOrCreate()

os.makedirs("download_data", exist_ok=True)

data_urls = {
    "order.json.gz": "https://data-architect-test-source.s3-sa-east-1.amazonaws.com/order.json.gz",
    "consumer.csv.gz": "https://data-architect-test-source.s3-sa-east-1.amazonaws.com/consumer.csv.gz",
    "restaurant.csv.gz": "https://data-architect-test-source.s3-sa-east-1.amazonaws.com/restaurant.csv.gz",
    "ab_test_ref.tar.gz": "https://data-architect-test-source.s3-sa-east-1.amazonaws.com/ab_test_ref.tar.gz"
}

for name, url in data_urls.items():
    path = f"download_data/{name}"
    download_files(name,url,path)

extract_tar_gz("download_data/ab_test_ref.tar.gz", target_dir="download_data")


# Leitura com schema aplicado
df_orders = load_dataframe(spark, "download_data/order.json.gz", file_type="json", schema=order_schema)
df_users = load_dataframe(spark, "download_data/consumer.csv.gz", file_type="csv", schema=consumer_schema)
df_restaurants = load_dataframe(spark, "download_data/restaurant.csv.gz", file_type="csv", schema=restaurant_schema)
df_abtest = load_dataframe(spark, "download_data/ab_test_ref.csv", file_type="csv", schema=abtest_schema)
logging.info("Leitura completa.")


df_orders = df_orders.dropna(subset=["customer_id", "order_id", "order_total_amount"])
df_abtest = df_abtest.filter(df_abtest.is_target.isNotNull())
logging.info("pré-process feito!")

item_schema = ArrayType(StructType([
    StructField("external_id", StringType(), True),
    StructField("name", StringType(), True),
    StructField("price", DoubleType(), True),
    StructField("quantity", IntegerType(), True),
    StructField("total_price", DoubleType(), True)
]))

orders_with_items = df_orders.withColumn("items_array", from_json(col("items"), item_schema))

df_item_orders = orders_with_items.select(
    "order_id",
    "customer_id",
    explode("items_array").alias("item")
)

df_item_orders = df_item_orders.select(
    "order_id",
    "customer_id",
    "item.name",
    "item.price",
    "item.quantity",
    "item.total_price",
    "item.external_id"
)

os.makedirs("trusted_data", exist_ok=True)

output_path_orders = "trusted_data/processed_orders.parquet"
save_dataframe_as_parquet(df_orders, output_path_orders)

output_path_items = "trusted_data/processed_item_orders.parquet"
save_dataframe_as_parquet(df_item_orders,output_path_items)

output_path_users = "trusted_data/processed_users.parquet"
save_dataframe_as_parquet(df_users,output_path_users)

output_path_restaurants = "trusted_data/processed_restaurants.parquet"
save_dataframe_as_parquet(df_restaurants,output_path_restaurants)

output_path_abtest = "trusted_data/processed_abtest.parquet"
save_dataframe_as_parquet(df_abtest,output_path_abtest)

logging.info("Fim do script.")