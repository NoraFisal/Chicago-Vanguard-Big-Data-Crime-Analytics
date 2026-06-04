# Chicago-Vanguard-Big-Data-Crime-Analytics 🚔

## Overview

Chicago Vanguard is a Big Data analytics platform developed to process and analyze over 8.5 million crime records from the City of Chicago.

The project leverages Apache Spark and distributed computing techniques to identify crime patterns, geographic hotspots, temporal trends, and hidden relationships within large-scale public safety data.

---

## Problem Statement

Traditional data processing techniques struggle with large-scale crime datasets due to:

- Massive data volume
- Slow processing performance
- Difficulty identifying crime trends
- Challenges in extracting actionable insights

---

## Solution

Chicago Vanguard uses Apache Spark's distributed computing ecosystem to efficiently process millions of records and generate meaningful crime intelligence through:

- Data preprocessing
- Feature engineering
- SQL analytics
- RDD transformations
- Machine learning clustering

---

## Dataset

### Raw Dataset Access

The original Chicago Crime dataset is extremely large and was not included in this repository.

Dataset Source:

https://data.cityofchicago.org/Public-Safety/City-of-Chicago-Crime-Data/v9q9-3dm2

Dataset Provider:
City of Chicago – Public Safety Department

---

## Project Highlights

- 8.5M+ Crime Records
- Apache Spark Ecosystem
- Distributed Analytics
- Feature Engineering
- Crime Hotspot Detection
- Machine Learning Clustering

---

## Feature Engineering

The following engineered features were created:

- Location_Crime_Count
- PrimaryType_ArrestRate
- Is_Night
- Is_Weekend
- Time_Of_Day

These features improved crime pattern discovery and clustering performance.

---

## Technologies Used

### Big Data

- Apache Spark
- Spark SQL
- RDDs
- MLlib

### Programming

- Scala

### Data Processing

- Feature Engineering
- Data Cleaning
- Data Transformation

### Machine Learning

- K-Means Clustering
- Silhouette Evaluation

---

## Machine Learning Pipeline

Raw Dataset

↓

Feature Selection

↓

Missing Value Handling

↓

Data Transformation

↓

VectorAssembler

↓

StandardScaler

↓

Train/Test Split

↓

K-Means Clustering

↓

Silhouette Evaluation

---

## Key Findings

### Crime Distribution

- Theft was the most common crime category.
- Battery ranked second.
- Criminal Damage ranked third.

### Geographic Analysis

- District 8 recorded the highest crime concentration.
- Districts 11 and 6 also showed elevated crime activity.

### Time-Based Analysis

Highest crime activity occurred around:

- Hour 12
- Hour 0
- Hour 20

---

## Clustering Results

Best clustering configuration:

- K = 5
- Silhouette Score = 0.3467

The clustering model successfully identified meaningful spatial and temporal crime patterns.

---

## Repository Structure

code/
├── DataPreprocessing
├── RDDOperations
├── SQLOperations
└── MachineLearning

data/
├── Dataset Samples
└── Processed Data

results/
├── SQL Results
├── RDD Results
└── ML Metrics

---

## Learning Outcomes

- Big Data Processing
- Distributed Computing
- Apache Spark Ecosystem
- Spark SQL Analytics
- Feature Engineering
- Machine Learning Clustering
- Scalable Data Pipelines

---

## Author

Nora Albyahi

AI & Information Technology Student

King Saud University
