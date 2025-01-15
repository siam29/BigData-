import timeit
from pyspark.sql import SparkSession, Row
from pyspark.sql.functions import (
    lit,
    array,
    collect_list,
    collect_set,
    initcap,
    transform,
    explode,
    regexp_replace,
)
import json


def initialize_spark_session(app_name="IcebergLocalDevelopment"):
    try:
        start_time = timeit.default_timer()
        spark = (
            SparkSession.builder.appName(app_name)
            .master("local[*]")
            .config(
                "spark.jars.packages",
                "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.2",
            )
            .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
            .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog")
            .config("spark.sql.catalog.local.type", "hadoop")
            .config("spark.sql.catalog.local.warehouse", "spark-warehouse/iceberg")
            .getOrCreate()
        )
        print("Spark session initialized successfully.")
        elapsed_time = timeit.default_timer() - start_time
        print(f"Spark session initialization time: {elapsed_time} seconds")
        return spark
    except Exception as e:
        print(f"Error initializing Spark session: {e}")
        return None


def load_json_file(spark, file_path, multiline=True):
    """
    Load a JSON file into a Spark DataFrame and record the time taken.

    Args:
        spark (SparkSession): Spark session instance.
        file_path (str): Path to the JSON file.
        multiline (bool): Whether the JSON file is multiline.

    Returns:
        DataFrame or None: The loaded DataFrame, or None if an error occurred.
    """
    try:
        start_time = timeit.default_timer()
        df = spark.read.option("multiline", str(multiline).lower()).json(file_path)
        elapsed_time = timeit.default_timer() - start_time
        print(f"Successfully loaded '{file_path}' in {elapsed_time:.4f} seconds.")
        return df
    except Exception as e:
        elapsed_time = timeit.default_timer() - start_time
        print(f"Error reading file '{file_path}' in {elapsed_time:.4f} seconds: {e}")
        return None



# Extract unique amenities
def extract_unique_amenities(df, column_name):
    try:
        return (
            df.select(column_name)
            .rdd.flatMap(lambda row: row[column_name] if row[column_name] else [])
            .flatMap(lambda amenities: amenities if isinstance(amenities, list) else [amenities])
            .map(lambda amenity: amenity.lower() if amenity else None)
            .distinct()
            .collect()
        )
    except Exception as e:
        print(f"Error extracting unique amenities from column '{column_name}': {e}")
        return []


# Map amenities to categories
def map_amenities_to_categories(amenities_list, category_mapping):
    try:
        mapped_amenities = []
        for amenity in amenities_list:
            if amenity in category_mapping:
                mapped_amenities.append([amenity, category_mapping[amenity]])
        return mapped_amenities
    except Exception as e:
        print(f"Error mapping amenities to categories: {e}")
        return []


# Create a DataFrame for categorized amenities
def create_amenities_dataframe(spark, combined_amenities, category_mapping, human_readable_values):
    try:
        category_amenities = {}
        for amenity in combined_amenities:
            if amenity in category_mapping:
                category = category_mapping[amenity]
                if isinstance(category, list):
                    for cat in category:
                        category_amenities.setdefault(cat, []).append(amenity)
                else:
                    category_amenities.setdefault(category, []).append(amenity)

        rows = [
            Row(amenities=amenities, amenities_count=len(amenities), amenity_category=category)
            for category, amenities in category_amenities.items()
        ]
        df = spark.createDataFrame(rows)
        df = df.withColumn("themes", lit(human_readable_values))
        df = df.withColumn("amenities", transform("amenities", lambda x: initcap(x)))
        return df
    except Exception as e:
        print(f"Error creating amenities DataFrame: {e}")
        return None


# Create Iceberg table and write data
def write_to_iceberg_table(spark, df, table_name="local.siamdb.iceberg_table"):
    try:
        spark.sql(f"DROP TABLE IF EXISTS {table_name}")
        print("-> Successfully dropped the existing Iceberg table (if it existed).")
        spark.sql(
            f"""
            CREATE TABLE IF NOT EXISTS {table_name} (
                amenities ARRAY<STRING>,
                amenities_count LONG,
                amenity_category STRING,
                themes ARRAY<STRING>
            )
            TBLPROPERTIES (
                'history.expire.max-snapshot-age-ms' = '60000',
                'write.format.default' = 'parquet'
            )
            """
        )
        print("-> Iceberg table created successfully.")
        df.write.format("iceberg").mode("append").saveAsTable(table_name)
        print("-> Data written to Iceberg table successfully.")
    except Exception as e:
        print(f"Error while writing data to the Iceberg table: {e}")


def main():
    # Initialize Spark Session
    spark = initialize_spark_session()
    if not spark:
        return

    # Load data
    df1 = load_json_file(spark, "amenities_sample_output.json")
    df2 = load_json_file(spark, "amenity_category.json")
    df3 = load_json_file(spark, "expedia-lodging-amenities-en_us-1-all.jsonl", multiline=False)

    # Extract unique amenities
    property_amenities = extract_unique_amenities(df3, "propertyAmenities")
    room_amenities = extract_unique_amenities(df3, "roomAmenities")
    combined_amenities = list(set(property_amenities + room_amenities))

    # Map amenities to categories
    with open("amenity_category.json", "r") as f:
        category_mapping = json.load(f)
    amenities_with_categories = map_amenities_to_categories(combined_amenities, category_mapping)

    # Create human-readable unique values
    exploded_df = df3.select(explode(df3.popularAmenities).alias("value"))
    transformed_df = exploded_df.select(
        initcap(regexp_replace("value", "_", "")).alias("readable_value")
    )
    human_readable_values = transformed_df.agg(collect_set("readable_value").alias("unique_list")).first()["unique_list"]

    # Create categorized amenities DataFrame
    amenities_df = create_amenities_dataframe(spark, combined_amenities, category_mapping, human_readable_values)
    amenities_df.show(truncate=False)

    # Write data to Iceberg table
    write_to_iceberg_table(spark, amenities_df)

    # Read back and display Iceberg table content
    df2 = spark.sql("SELECT * FROM local.siamdb.iceberg_table")
    df2.show(truncate=False)


if __name__ == "__main__":
    main()
