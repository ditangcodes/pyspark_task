import pyspark
import json
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode
from pyspark.sql.types import StructType, ArrayType
import pyspark.sql.functions as F


# Step 1: Create a Spark session
spark = SparkSession.builder.appName("Gousto Project").getOrCreate()

# Step 2: Read the JSON file into a DataFrame
df = spark.read.option("multiline","true").json('fake_web_events.json')
df.printSchema()
df.show(truncate=False)

# Step 3: Flatten the nested structure
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
    explode("event_details.order_info.items").alias("item") # Explode items array
)

# Step 4: Select the individual fields from the exploded item
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

# Count the number of events per user
def count_events_per_user(flattened_df):
    # Group by user_id and count the number of events
    results_df = flattened_df.groupBy("user_id").agg(F.count("*").alias("event_count"))
    results_df.show()

# Call the function
count_events_per_user(final_df)

# Find the most viewed recipe pages
def most_viewed_recipes(df):
    # Step 1: Filter the DataFrame for 'page_view' events
    most_viewed_df = df.filter(df["event_type"] == "page_view")

    # Step 2: Group by 'page_url' and count the number of occurrences
    most_viewed_recipe_df = most_viewed_df.groupBy("page_url").agg(F.count("*").alias("view_count"))
    # Step 3: Order by view count in descending order
    most_viewed_recipe_df = most_viewed_recipe_df.orderBy(F.desc("view_count"))

    # Step 4: Show the result
    most_viewed_recipe_df.show(truncate=False)

# Call the function
most_viewed_recipes(final_df)

# Calculate Total order
def count_total_order(df):
    total_count_df = df.groupBy("order_id").agg(F.count("*").alias("Total Order Count"))
    total_count_df.show()

# Call the function
count_total_order(final_df)

# Task 4: Aggregate the number of clicks and page views per device type.
def total_number_of_clicks(df):
    #Filtering the Dataframe
    page_views_df = df.filter(df["event_type"] == "page_view")
    total_clicks_df = df.filter(df["event_type"] == "click")
    # Aggregating the total number of clicks and page views
    page_view_count_df = page_views_df.groupBy("device_type").agg(F.count("*").alias("Total Page Views"))
    total_clicks_df = total_clicks_df.groupBy("device_type").agg(F.count("*").alias("Total Clicks"))
    
    # Join Both dataframes on device type
    total_count_df = page_view_count_df.join(total_clicks_df, on="device_type", how='outer')
    
    total_count_df.show(truncate=False)
#Call the function
total_number_of_clicks(final_df)


def total_number_of_click(df):
    # Create flags for 'click' and 'page_view' events
    df_with_flags = df.withColumn("page_view", F.when(F.col("event_type") == "page_view", 1).otherwise(0)) \
                      .withColumn("click", F.when(F.col("event_type") == "click", 1).otherwise(0))

    # Aggregate the total number of clicks and page views by device type
    total_clicks_df = df_with_flags.groupBy("device_type").agg(
        F.sum("page_view").alias("Total Page Views"),
        F.sum("click").alias("Total Clicks")
    )
    
    total_clicks_df.show(truncate=False)

# Call the function
total_number_of_click(final_df)

