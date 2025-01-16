# Assignment 2: Spark-Based Amenity Extraction and Categorization

This project focuses on using Apache Spark to process and analyze amenity data from JSON files, categorize amenities, and write the results to an Iceberg table. The program performs various steps, including loading data, extracting unique amenities, mapping amenities to categories, and writing the data to a table for further analysis.

## Table of Contents
- Requirements

- Project Overview

- How to Run

- Key Features

- File Structure

- Output

### Requirements

Ensure you have the following installed:

- Python 3.8+

- Apache Spark 3.5.3

- Virtualenv (recommended for environment management)

Python Libraries:

- pyspark

- json

- timeit

### How to Run 
```
python3 main.py
```


### Project Overview
**This project performs the following tasks:**

- Initialize a Spark Session: Configures Spark with Iceberg and sets up the environment.

- Load JSON Files: Reads multiple JSON files containing amenity data.

- Extract Unique Amenities: Processes the propertyAmenities and roomAmenities columns to extract unique amenities.

- Map Amenities to Categories: Maps amenities to predefined categories using a JSON mapping file.

- Generate Human-Readable Values: Formats amenity names for better readability.

- Write to Iceberg Table: Writes the processed data into an Iceberg table for further analysis.


### Key Features
- Time Measurement: Each function logs the time taken for execution, enabling performance analysis.

- Error Handling: Proper exception handling ensures the program runs smoothly.

- Data Categorization: Maps amenities to predefined categories for structured output.

- Iceberg Integration: Outputs processed data into an Iceberg table for scalability and analysis.

### File Structure

```
Assignment-2/
├── main.py                 # Main script to run the assignment
├── amenities_sample_output.json # Sample JSON data
├── amenity_category.json   # JSON file for amenity-to-category mapping
├── expedia-lodging-amenities-en_us-1-all.jsonl # Input dataset
├── README.md               # Project documentation
├── practice.ipynb          # In jupyter notebook
├── env                     # create virtual environment
└── spark-warehouse/        # Directory for Iceberg table storage
 
```

### Output
- Extracted and categorized amenities are displayed in the console.

- Data is written into an Iceberg table named local.siamdb.iceberg_table.

#### Example of sample output

```
+------------------+----------------+-------------------+------------------+
| amenities        | amenities_count| amenity_category  | themes           |
+------------------+----------------+-------------------+------------------+
| [Pool, WiFi]     | 2              | Leisure           | [Popular Amenities]|
| [Parking, Gym]   | 2              | Facilities        | [Popular Amenities]|
+------------------+----------------+-------------------+------------------+
```
