import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode

# Step 1: Create a Spark session
spark = SparkSession.builder.appName("Gousto Project").getOrCreate()

# Step 2: Read the JSON file into a DataFrame
json_filepath = "../src/fake_web_events.json"

# Use DROPMALFORMED to skip corrupted records
df = spark.read.option("mode", "DROPMALFORMED").json(json_filepath)

# Step 3: Inspect the schema and show some rows
df.printSchema()
df.show(truncate=False)

# Step 4: Flatten the nested structure if schema is as expected
flattened_df = df.select(
    "event_id",
    "user.user_id",
    "user.location.country",
    "user.location.city",
    "user.device_type",
    "event_details.event_type",
    "event_details.page_info.page_url",
    "event_details.page_info.page_category",
    "event_details.order_info.order_id",
    "event_details.order_info.order_value",
    explode("event_details.order_info.items").alias("item")
)

# Step 5: Select the individual fields from the exploded item
final_df = flattened_df.select(
    "event_id",
    "user_id",
    "country",
    "city",
    "device_type",
    "event_type",
    "page_url",
    "page_category",
    "order_id",
    "order_value",
    "item.item_id",
    "item.name",
    "item.quantity"
)

# Show the final flattened DataFrame
final_df.show(truncate=False)
