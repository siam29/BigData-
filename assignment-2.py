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
        elapsed_time = timeit.default_timer() - start_time
        print(f"Spark session initialized in {elapsed_time:.4f} seconds.")
        return spark
    except Exception as e:
        elapsed_time = timeit.default_timer() - start_time
        print(f"Error initializing Spark session in {elapsed_time:.4f} seconds: {e}")
        return None


def load_json_file(spark, file_path, multiline=True):
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


def extract_unique_amenities(df, column_name):
    try:
        start_time = timeit.default_timer()
        unique_amenities = (
            df.select(column_name)
            .rdd.flatMap(lambda row: row[column_name] if row[column_name] else [])
            .flatMap(lambda amenities: amenities if isinstance(amenities, list) else [amenities])
            .map(lambda amenity: amenity.lower() if amenity else None)
            .distinct()
            .collect()
        )
        elapsed_time = timeit.default_timer() - start_time
        print(f"Successfully extracted unique amenities from column '{column_name}' in {elapsed_time:.4f} seconds.")
        return unique_amenities
    except Exception as e:
        elapsed_time = timeit.default_timer() - start_time
        print(f"Error extracting unique amenities from column '{column_name}' in {elapsed_time:.4f} seconds: {e}")
        return []


def map_amenities_to_categories(amenities_list, category_mapping):
    try:
        start_time = timeit.default_timer()
        mapped_amenities = []
        for amenity in amenities_list:
            if amenity in category_mapping:
                mapped_amenities.append([amenity, category_mapping[amenity]])
        elapsed_time = timeit.default_timer() - start_time
        print(f"Successfully mapped amenities to categories in {elapsed_time:.4f} seconds.")
        return mapped_amenities
    except Exception as e:
        elapsed_time = timeit.default_timer() - start_time
        print(f"Error mapping amenities to categories in {elapsed_time:.4f} seconds: {e}")
        return []


def create_amenities_dataframe(spark, combined_amenities, category_mapping, human_readable_values):
    try:
        start_time = timeit.default_timer()
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
        elapsed_time = timeit.default_timer() - start_time
        print(f"Successfully created amenities DataFrame in {elapsed_time:.4f} seconds.")
        return df
    except Exception as e:
        elapsed_time = timeit.default_timer() - start_time
        print(f"Error creating amenities DataFrame in {elapsed_time:.4f} seconds: {e}")
        return None


def write_to_iceberg_table(spark, df, table_name="local.siamdb.iceberg_table"):
    try:
        start_time = timeit.default_timer()
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
        elapsed_time = timeit.default_timer() - start_time
        print(f"Data written to Iceberg table in {elapsed_time:.4f} seconds.")
    except Exception as e:
        elapsed_time = timeit.default_timer() - start_time
        print(f"Error while writing data to the Iceberg table in {elapsed_time:.4f} seconds: {e}")


def main():
    spark = initialize_spark_session()
    if not spark:
        return

    df1 = load_json_file(spark, "amenities_sample_output.json")
    df2 = load_json_file(spark, "amenity_category.json")
    df3 = load_json_file(spark, "expedia-lodging-amenities-en_us-1-all.jsonl", multiline=False)

    property_amenities = extract_unique_amenities(df3, "propertyAmenities")
    room_amenities = extract_unique_amenities(df3, "roomAmenities")
    combined_amenities = list(set(property_amenities + room_amenities))

    with open("amenity_category.json", "r") as f:
        category_mapping = json.load(f)
    amenities_with_categories = map_amenities_to_categories(combined_amenities, category_mapping)

    exploded_df = df3.select(explode(df3.popularAmenities).alias("value"))
    transformed_df = exploded_df.select(
        initcap(regexp_replace("value", "_", "")).alias("readable_value")
    )
    human_readable_values = transformed_df.agg(collect_set("readable_value").alias("unique_list")).first()["unique_list"]

    amenities_df = create_amenities_dataframe(spark, combined_amenities, category_mapping, human_readable_values)
    amenities_df.show(truncate=False)

    write_to_iceberg_table(spark, amenities_df)

    df2 = spark.sql("SELECT * FROM local.siamdb.iceberg_table")
    df2.show(truncate=False)


if __name__ == "__main__":
    main()
