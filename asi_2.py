import timeit
import os
import json
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import expr


def measure_time(func):
    """Decorator to measure the execution time of a function."""
    def wrapper(*args, **kwargs):
        start_time = timeit.default_timer()
        try:
            result = func(*args, **kwargs)
        except Exception as e:
            print(f"Error in {func.__name__}: {e}")
            raise
        end_time = timeit.default_timer()
        elapsed_time = end_time - start_time
        print(f"{func.__name__} executed in {elapsed_time:.2f} seconds")
        return result
    return wrapper


@measure_time
def initialize_spark():
    """Initialize and return a Spark session."""
    try:
        spark = SparkSession.builder \
            .appName("IcebergLocalDevelopment") \
            .master("local[*]") \
            .config('spark.jars.packages', 'org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.2') \
            .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
            .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog") \
            .config("spark.sql.catalog.local.type", "hadoop") \
            .config("spark.sql.catalog.local.warehouse", "spark-warehouse/iceberg") \
            .config("spark.sql.catalogImplementation", "hive") \
            .enableHiveSupport() \
            .getOrCreate()
        return spark
    except Exception as e:
        print(f"Error initializing Spark session: {e}")
        raise


@measure_time
def load_json_files(spark):
    """Load JSON files into DataFrames."""
    try:
        df1 = spark.read.option("multiline", "true").json("amenities_sample_output.json")
        df2 = spark.read.option("multiline", "true").json("amenity_category.json")
        df3 = spark.read.json("expedia-lodging-amenities-en_us-1-all.jsonl")
        return df1, df2, df3
    except Exception as e:
        print(f"Error loading JSON files: {e}")
        raise


def get_country_code_mapping():
    """Return a dictionary of country names to their corresponding codes."""
    try:
        return {
    "Bonaire Saint Eustatius and Saba": "BSES",
    "Paraguay": "PY",
    "Anguilla": "AI",
    "Macao": "MO",
    "U.S. Virgin Islands": "USV",
    "Senegal": "SN",
    "Sweden": "SE",
    "Guyana": "GY",
    "Philippines": "PH",
    "Jersey": "JE",
    "Eritrea": "ER",
    "Djibouti": "DJ",
    "Norfolk Island": "NF",
    "Tonga": "TO",
    "Singapore": "SG",
    "Malaysia": "MY",
    "Fiji": "FJ",
    "Turkey": "TR",
    "Malawi": "MW",
    "Germany": "DE",
    "Northern Mariana Islands": "MP",
    "Comoros": "KM",
    "Cambodia": "KH",
    "Maldives": "MV",
    "Ivory Coast": "IC",
    "Jordan": "JO",
    "Rwanda": "RW",
    "Palau": "PW",
    "France": "FR",
    "Turks and Caicos Islands": "TC",
    "Greece": "GR",
    "Sri Lanka": "LK",
    "Montserrat": "MS",
    "Taiwan": "TW",
    "Dominica": "DM",
    "British Virgin Islands": "VG",
    "Algeria": "DZ",
    "Togo": "TG",
    "Equatorial Guinea": "GQ",
    "Slovakia": "SK",
    "Reunion": "RE",
    "Argentina": "AR",
    "Belgium": "BE",
    "Angola": "AO",
    "San Marino": "SM",
    "Ecuador": "EC",
    "Qatar": "QA",
    "Lesotho": "LS",
    "Albania": "AL",
    "Madagascar": "MG",
    "Finland": "FI",
    "New Caledonia": "NC",
    "Ghana": "GH",
    "Myanmar": "MM",
    "Nicaragua": "NI",
    "Guernsey": "GG",
    "Peru": "PE",
    "Benin": "BJ",
    "Sierra Leone": "SL",
    "United States": "US",
    "India": "IN",
    "Bahamas": "BS",
    "China": "CN",
    "Curacao": "CUR",
    "Belarus": "BY",
    "Malta": "MT",
    "Kuwait": "KW",
    "Sao Tome and Principe": "ST",
    "Palestinian Territory": "PT",
    "Puerto Rico": "PR",
    "Chile": "CL",
    "Tajikistan": "TJ",
    "Martinique": "MQ",
    "Cayman Islands": "KY",
    "Isle of Man": "IM",
    "Croatia": "HR",
    "Burundi": "BI",
    "Nigeria": "NG",
    "Andorra": "AD",
    "Bolivia": "BO",
    "Gabon": "GA",
    "Italy": "IT",
    "Suriname": "SR",
    "Lithuania": "LT",
    "Norway": "NO",
    "Turkmenistan": "TM",
    "Spain": "ES",
    "Cuba": "CU",
    "Mauritania": "MR",
    "Guadeloupe": "GP",
    "Denmark": "DK",
    "Barbados": "BB",
    "Bangladesh": "BD",
    "Ireland": "IE",
    "Liechtenstein": "LI",
    "Swaziland": "SL",
    "Thailand": "TH",
    "Laos": "LA",
    "Christmas Island": "CX",
    "Bhutan": "BT",
    "Democratic Republic of the Congo": "DRC",
    "Morocco": "MA",
    "Monaco": "MC",
    "Panama": "PA",
    "Cape Verde": "CV",
    "Hong Kong": "HK",
    "Israel": "IL",
    "Iceland": "IS",
    "Saint Barthelemy": "SB",
    "Saint Kitts and Nevis": "KN",
    "Oman": "OM",
    "French Polynesia": "PF",
    "South Korea": "KR",
    "Cyprus": "CY",
    "Gibraltar": "GI",
    "Uruguay": "UY",
    "Mexico": "MX",
    "Aruba": "AW",
    "Montenegro": "ME",
    "Georgia": "GE",
    "Zimbabwe": "ZW",
    "Estonia": "EE",
    "Indonesia": "ID",
    "Saint Vincent and the Grenadines": "VC",
    "Guatemala": "GT",
    "Guam": "GU",
    "Mongolia": "MN",
    "Republic of the Congo": "CG",
    "Azerbaijan": "AZ",
    "Sint Maarten": "SIM",
    "Grenada": "GD",
    "Armenia": "AM",
    "Tunisia": "TN",
    "Liberia": "LR",
    "Honduras": "HN",
    "Trinidad and Tobago": "TT",
    "Saudi Arabia": "SA",
    "Uganda": "UG",
    "Wallis and Futuna": "WF",
    "French Guiana": "GF",
    "Namibia": "NA",
    "Mayotte": "YT",
    "Switzerland": "CH",
    "Zambia": "ZM",
    "Ethiopia": "ET",
    "Jamaica": "JM",
    "Latvia": "LV",
    "United Arab Emirates": "AE",
    "Brunei": "BR",
    "Saint Lucia": "LC",
    "Saint Martin": "SAM",
    "Aland Islands": "AI",
    "Guinea": "GN",
    "Canada": "CA",
    "Seychelles": "SC",
    "Kyrgyzstan": "KG",
    "Uzbekistan": "UZ",
    "Macedonia": "MD",
    "Faroe Islands": "FO",
    "Samoa": "WS",
    "Czech Republic": "CZ",
    "Mozambique": "MZ",
    "Cook Islands": "CK",
    "Brazil": "BR",
    "Belize": "BZ",
    "Kenya": "KE",
    "Gambia": "GM",
    "Lebanon": "LB",
    "Slovenia": "SI",
    "Antigua and Barbuda": "AG",
    "Dominican Republic": "DO",
    "Japan": "JP",
    "Tanzania": "TZ",
    "Botswana": "BW",
    "Luxembourg": "LU",
    "New Zealand": "NZ",
    "United States Minor Outlying Islands": "UM",
    "Bosnia and Herzegovina": "BA",
    "Greenland": "GL",
    "Haiti": "HT",
    "Poland": "PL",
    "Portugal": "PT",
    "Australia": "AU",
    "Cameroon": "CM",
    "Papua New Guinea": "PG",
    "Romania": "RO",
    "Guinea-Bissau": "GW",
    "Bulgaria": "BG",
    "Austria": "AT",
    "Nepal": "NP",
    "Egypt": "EG",
    "Costa Rica": "CR",
    "El Salvador": "SV",
    "Kazakhstan": "KZ",
    "Serbia": "RS",
    "South Africa": "ZA",
    "Burkina Faso": "BF",
    "Bermuda": "BM",
    "Bahrain": "BH",
    "Micronesia": "MC",
    "Colombia": "CO",
    "Hungary": "HU",
    "Pakistan": "PK",
    "Vanuatu": "VU",
    "Mauritius": "MU",
    "United Kingdom": "GB",
    "Moldova": "MD",
    "Vietnam": "VN",
    "Netherlands": "NL",
    "Mali": "ML",
    "Chad": "TD",
    "Svalbard and Jan Mayen": "SJ",
    "Sudan": "SD",
    "Niue": "NU",
    "Kiribati": "KI",
    "Iraq": "IQ",
    "American Samoa": "AS",
    "Saint Pierre and Miquelon": "PM",
    "Niger": "NE",
    "Solomon Islands": "SB"
        }
    except Exception as e:
        print(f"Error in get_country_code_mapping: {e}")
        raise


@measure_time
def process_data(spark, df3, country_code_mapping):
    """Process data to flatten, map country codes, and clean amenities."""
    try:
        country_code_df = spark.createDataFrame(country_code_mapping.items(), ["country", "country_code"])
        flattened_df = df3.select(
            F.initcap(F.concat_ws(", ", F.col("popularAmenities"))).alias("themes"),
            F.concat_ws(", ", *[
                F.col(f"propertyAmenities.{col_name}") for col_name in df3.schema["propertyAmenities"].dataType.fieldNames()
            ]).alias("property_amenities"),
            F.concat_ws(", ", *[
                F.col(f"roomAmenities.{col_name}") for col_name in df3.schema["roomAmenities"].dataType.fieldNames()
            ]).alias("room_amenities"),
            F.col("country")
        )
        flattened_df_with_code = flattened_df.join(country_code_df, flattened_df.country == country_code_df.country, "left") \
            .drop("country")
        combined_df = flattened_df_with_code.select(
            F.lower(F.concat_ws(", ", F.col("property_amenities"), F.col("room_amenities"))).alias("amenities"),
            F.size(F.split(F.lower(F.concat_ws(", ", F.col("property_amenities"), F.col("room_amenities"))), ", ")).alias(
                "amenities_count"),
            F.col("themes"),
            F.col("country_code")
        )
        result_df = combined_df.withColumn(
            "amenities",
            F.split(F.col("amenities"), ",\\s*")
        ).withColumn(
            "amenities",
            F.expr("""
        transform(
            amenities,
            x -> lower(
                regexp_replace(
                    regexp_replace(
                        regexp_replace(
                            regexp_replace(
                                regexp_replace(
                                    regexp_replace(
                                        trim(x), 
                                        r"\s*\([^)]*\)", 
                                        ""
                                    ),
                                    " - ", 
                                    "-"
                                ),
                                " ", "_"  
                            ),
                            "-", "_"
                        ),
                        "/", "_"
                    ),
                    '_+', '_'
                )
            )
        )
        """
    )
        )
        return result_df
    except Exception as e:
        print(f"Error in process_data: {e}")
        raise


def load_amenity_category(func):
    """Decorator to load the amenity category JSON file."""
    def wrapper(*args, **kwargs):
        try:
            with open("amenity_category.json", "r") as f:
                amenity_category_dict = json.load(f)
            kwargs['amenity_category_dict'] = amenity_category_dict
        except FileNotFoundError:
            print("Error: amenity_category.json file not found.")
            raise
        except json.JSONDecodeError as e:
            print(f"Error decoding amenity_category.json: {e}")
            raise
        return func(*args, **kwargs)
    return wrapper


@load_amenity_category
@measure_time
def map_amenities_to_categories(result_df, amenity_category_dict=None):
    """Map amenities to categories and ensure unique categories."""
    try:
        flat_amenity_category_dict = {
            amenity: category for amenity, categories in amenity_category_dict.items()
            for category in (categories if isinstance(categories, list) else [categories])
        }
        mapping_expr = F.create_map(*[F.lit(key_val) for key_val in sum(flat_amenity_category_dict.items(), ())])
        final_df = result_df.withColumn(
            "categories",
            F.array_distinct(
                F.transform(
                    F.col("amenities"),
                    lambda amenity: F.coalesce(F.element_at(mapping_expr, amenity), F.lit("Other"))
                )
            )
        )

        final_df = (
            final_df
            .withColumn("amenities", expr("""
                transform(
                    amenities,
                    x -> initcap(regexp_replace(x, '_', ' '))
                )
            """))
        )

        final_df.show(5,truncate=False)

        return final_df
    except Exception as e:
        print(f"Error in map_amenities_to_categories: {e}")
        raise


@measure_time
def write_to_iceberg_table(final_df):
    """Write the final DataFrame to an Iceberg table."""
    try:
        spark.sql("DROP TABLE IF EXISTS local.siamdb.transformed_table")
        spark.sql("""
            CREATE TABLE IF NOT EXISTS local.siamdb.transformed_table (
                themes STRING,
                amenities ARRAY<STRING>,
                amenities_count INT,
                categories ARRAY<STRING>,
                country_code STRING
            )
            USING iceberg
            TBLPROPERTIES (
                'history.expire.max-snapshot-age-ms' = '60000',
                'write.format.default' = 'parquet'
            )
            PARTITIONED BY (country_code)
        """)
        final_df.write \
            .format("iceberg") \
            .mode("append") \
            .saveAsTable("local.siamdb.transformed_table")
        print("Data written to Iceberg table successfully.")

        df = spark.sql("SELECT * FROM local.siamdb.transformed_table")
        df.show(truncate=False)
    except Exception as e:
        print(f"Error in write_to_iceberg_table: {e}")
        raise


if __name__ == "__main__":
    try:
        start_time = timeit.default_timer()
        spark = initialize_spark()
        df1, df2, df3 = load_json_files(spark)
        country_code_mapping = get_country_code_mapping()
        result_df = process_data(spark, df3, country_code_mapping)
        final_df = map_amenities_to_categories(result_df)
        final_df.show(truncate=False)
        write_to_iceberg_table(final_df)
        end_time = timeit.default_timer()
        elapsed_time = end_time - start_time
        print(f"Total executed in {elapsed_time:.2f} seconds")
    except Exception as e:
        print(f"Error in main execution: {e}")
