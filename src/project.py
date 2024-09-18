import pyspark
import json
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode
from pyspark.sql.types import StructType, ArrayType

# Start Spark Session
spark = SparkSession.builder \
    .appName("Gousto Pair Programming") \
    .config("spark.memory.offHeap.enabled", "true") \
    .config("spark.memory.offHeap.size", "10g") \
    .getOrCreate()

df = spark.read.option("multiline","true").json('fake_web_events.json')
df.printSchema()
df.show(truncate=False)

def flatten_json(data, prefix=''):
    """
    Recursively unnests a nested JSON structure into individual columns.
    
    Args:
        data (dict): The JSON data to unnest.
        prefix (str): Prefix for column names (used for recursion).
    
    Returns:
        dict: A flattened dictionary with unnested columns.
    """
    flattened_data = {}
    if isinstance(data, dict):
        for key, value in data.items():
            new_key = f"{prefix}{key}"
            if isinstance(value, dict):
                # Recurse into nested dictionaries
                flattened_data.update(flatten_json(value, prefix=new_key + "_"))
            elif isinstance(value, list):
                # Unnest lists (if needed)
                for i, item in enumerate(value):
                    flattened_data.update(flatten_json(item, prefix=new_key + str(i) + "_"))
            else:
                # Base case: scalar value
                flattened_data[new_key] = value
    elif isinstance(data, list):
        # Handle a list of dictionaries (e.g., multiple events)
        for i, event in enumerate(data):
            flattened_data.update(flatten_json(event, prefix=f"event_{i}_"))
    return flattened_data

# Read the JSON data from the file
with open("fake_web_events.json", "r") as json_file:
    json_content = json.load(json_file)

# Call the flatten_json function
flattened_result = flatten_json(json_content)

# Print the flattened dictionary
for key, value in flattened_result.items():
    print(f"{key}: {value}")

"""
The Code:
Show the function definition: def flatten_json(data, prefix=''):
Inside the function:
    Check if data is a dictionary or a list.
If its a dictionary:
    Iterate through key-value pairs.
    If the value is a dictionary, recurse with a modified prefix.
    If the value is a list, handle each item.
    If its a scalar value, add it to the flattened dictionary.
If its a list:
    Iterate through each dictionary (representing an event).
    Apply the same process recursively.
Return the flattened dictionary.
"""

# 5. Flatten the DataFrame
flattened_df = flatten_df(df)

# Show the flattened structure to verify
flattened_df.printSchema()
flattened_df.show(truncate=False)

# Count the number of events per user
def count_events_per_user(flattened_df):
    # Group by user_id and count the number of events
    results_df = flattened_df.groupBy("user_user_id").agg(F.count("*").alias("event_count"))
    results_df.show()

# 7. Call the function to count events per user
count_events_per_user(flattened_df)

def sum_order_values_per_user(flattened_df):
    results_df = flattened_df.groupBy('user_id').sum('order').collect()
    results_df.show()