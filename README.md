# Multinational-Retail-Data-Centralisation
## Overview
<p>This project was developed as part of the scenario-based assignments provided by Ai Core, contributing to the Data Engineer Career Accelerator program.


<b>Scenario:</b>
 You work for a multinational company that sells various goods across the globe. Currently, the company's sales data is spread across many different data sources, making it not easily accessible or analysable by the current members of the team. In an effort to become more data-driven, this project aims to integrate and centralize the sales data, providing a unified and easily accessible data source for analysis and decision-making.</p>

## Project Goals
<br>
-Centralize Sales Data: Integrate sales data from various sources into a single, unified database.
</br>

-Improve Data Accessibility: Ensure that the centralized data is easily accessible to all team members.
<br>
-Enhance Data Analysis: Provide a clean and structured data set that can be used for advanced analytics and reporting.
</br>
-Support Data-Driven Decisions: Enable the company to make informed decisions based on comprehensive and accurate data.

## Features
<br>
-Data Extraction: Collect sales data from multiple sources, including S3 buckets, databases, and API endpoints.
</br>
-Data Cleaning: Standardize and clean the data to ensure consistency and accuracy.
<br>
-Data Transformation: Convert the data into a format that is suitable for analysis, including handling various units of measurement.
</br>
-Data Loading: Store the cleaned and transformed data in a centralized database.
<br>
-Data Analysis: Provide tools and scripts for analyzing the centralized data.
</b>

## Components:
### Data Extraction
<br>
-S3 Integration: Download and extract data from S3 buckets using the boto3 library.
</br>
-Database Connections: Connect to various databases to extract sales data.
<br>
-API Integration: Retrieve data from external APIs.
</br>

### Data Cleaning
<br>
-Standardize Units: Convert all weight measurements to a standard unit (e.g., kilograms).
</br>
-Handle Missing Values: Identify and appropriately handle missing or invalid data.
<br>
-Data Validation: Ensure the data meets predefined quality standards.
</br>

### Data Transformation
<br>
-Data Conversion: Convert data into a structured format suitable for loading into the database.
</br>
-Data Normalization: Normalize data to eliminate redundancies and improve integrity.

### Data Loading
<br>
-Database Storage: Load the cleaned and transformed data into a centralized database.
</br>
-Data Backup: Implement backup strategies to ensure data safety and integrity.

### Getting Started
### Prerequisites
<br>
-Python 3.8 or higher
</br>
-boto3 library for AWS S3 integration
<br>
-pandas library for data manipulation
</br>
-Database connector libraries (e.g., psycopg2 for PostgreSQL)

