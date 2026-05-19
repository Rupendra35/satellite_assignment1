# satellite_assigment

# Satellite Intelligence Data Engineering Assignment

## Overview

This project processes parcel-level satellite and sensor data using PySpark.

The pipeline:

* Reads parcel readings and parcel metadata datasets
* Performs data quality checks
* Cleans and standardizes inconsistent data
* Joins datasets into a single time-series dataset
* Computes NDVI trends before and after sowing dates

Tech Stack:

* Python
* PySpark
* Spark SQL

---

## Project Structure

.
├── data/
│   ├── parcel_readings.csv
│   └── parcel_metadata.csv
│
├── output/
│   ├── cleaned_parcel_timeseries.csv
│   └── crop_analysis_output.csv
│
├── src/
│   └── pipeline.py
│
└── README.md

---

## 1. Data Quality Audit

### Issues Identified

| Issue                                                         | Decision                                       | Justification                                                 |
| ------------------------------------------------------------- | ---------------------------------------------- | ------------------------------------------------------------- |
| Invalid NDVI values outside valid range [-1,1]                | Dropped rows                                   | NDVI values outside the valid range are physically impossible |
| Duplicate rows                                                | Removed duplicates                             | Prevented duplicate records from impacting aggregations       |
| String null values such as `null`, `NA`, and `NaN`            | Converted to actual null values                | Standardized missing value handling across the dataset        |
| Null or invalid date values                                   | Dropped rows                                   | Date is mandatory for time-series analysis                    |
| Inconsistent sensor status values (`OK`, `error`, `NA`, etc.) | Standardized to `good`, `bad`, and `unknown`   | Ensured reliable downstream filtering                         |
| Missing rainfall values                                       | Filled with 0                                  | Missing rainfall likely represented no rainfall               |
| Missing temperature values                                    | Imputed using average temperature              | Preserved continuity without major analytical distortion      |
| Null sensor status values                                     | Marked as `unknown` and excluded from analysis | Sensor reliability could not be verified                      |

---

## 2. Pipeline Approach

### Steps Performed

1. Read both CSV files using PySpark
2. Converted fake null strings into actual nulls
3. Removed duplicate rows
4. Converted date columns into proper date format
5. Removed rows with missing critical keys
6. Filtered invalid NDVI values
7. Standardized sensor status categories
8. Handled missing values
9. Joined parcel readings with metadata
10. Generated final cleaned dataset
11. Performed crop-level NDVI analysis

---

## 3. Analysis

The analysis calculates:

* Mean NDVI in the 30 days before sowing
* Mean NDVI in the 30 days after sowing
* Only records with good sensor status were included

### Output Columns

| Column           | Description                        |
| ---------------- | ---------------------------------- |
| crop_type        | Crop category                      |
| mean_ndvi_before | Average NDVI 30 days before sowing |
| mean_ndvi_after  | Average NDVI 30 days after sowing  |
| n_parcels        | Number of unique parcels           |

### Interpretation

Across most crop types, NDVI values generally increased after sowing, indicating improved vegetation health following planting activity.

Rows with unreliable or unknown sensor statuses were excluded to improve analytical reliability.

---

## 4. Production Readiness Reflection

### If the dataset becomes 100× larger

1. Replace CSV storage with Parquet or Delta format
2. Partition datasets by date for improved read performance
3. Introduce orchestration and monitoring using Apache Airflow

### Production Monitoring

I would monitor:

* Pipeline execution failures
* Null value spikes
* Duplicate row growth
* Invalid NDVI percentages
* Join mismatches
* Daily row count anomalies

### Most Likely Silent Failure

A likely silent failure would be unexpected upstream changes to sensor status values, such as new categories replacing existing values, which could bypass filtering logic while the pipeline still executes successfully.

---

## 5. Running the Pipeline

Run the pipeline using:

python src/pipeline.py

Output files will be generated inside the `output/` folder.

---

## 6. AI Usage Disclosure

ChatGPT was used to:

* Brainstorm data quality checks
* Review PySpark transformations
* Improve README wording and structure

Final implementation logic and validation were manually reviewed.
