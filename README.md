\# Virginia Energy Data Engineering Pipeline



An end-to-end data engineering and analytics pipeline for Virginia electricity generation data using the U.S. Energy Information Administration (EIA) API, Python, AWS S3, Databricks, PySpark, Delta Lake, SQL, and Power BI.



The pipeline ingests raw electricity generation data from an API, stores the raw data in cloud object storage, transforms it through a Bronze/Silver/Gold architecture, performs data-quality validation and analytical modeling, and delivers the final Gold dataset to Power BI.



\---



\## Project Overview



This project analyzes Virginia electricity generation from \*\*January 2020 through December 2025\*\*.



The pipeline processes \*\*22,610 records across 72 months\*\* and produces an analytical Gold layer focused on:



\- Total electricity generation

\- Renewable generation and renewable share

\- Natural gas generation

\- Nuclear generation

\- Wind generation

\- Solar generation

\- Hydroelectric generation

\- Biomass generation

\- Coal generation

\- Year-over-year generation changes

\- Rolling 12-month generation



The goal was to demonstrate an end-to-end data engineering workflow rather than build a standalone visualization.



\---



\## Architecture



```text

&#x20;                   EIA API

&#x20;                      │

&#x20;                      ▼

&#x20;               Python Ingestion

&#x20;                      │

&#x20;                API Pagination

&#x20;                      │

&#x20;                      ▼

&#x20;                   AWS S3

&#x20;               Raw JSON Storage

&#x20;                      │

&#x20;                      ▼

&#x20;             Databricks / PySpark

&#x20;                      │

&#x20;                      ▼

&#x20;                   BRONZE

&#x20;              Raw Delta Table

&#x20;                      │

&#x20;                      ▼

&#x20;                   SILVER

&#x20;         Cleaned / Typed / Validated

&#x20;                      │

&#x20;                      ▼

&#x20;                    GOLD

&#x20;            Analytical Data Model

&#x20;                      │

&#x20;                      ▼

&#x20;              Databricks SQL

&#x20;                      │

&#x20;                      ▼

&#x20;                 Power BI

&#x20;            Business Analytics

```



\---



\## Technology Stack



| Layer | Technology |

|---|---|

| Data Source | U.S. Energy Information Administration API |

| Ingestion | Python, Requests |

| Cloud Storage | AWS S3 |

| Processing | Databricks, PySpark |

| Storage Format | Delta Lake |

| Data Modeling | PySpark / SQL |

| Visualization | Power BI |

| Architecture | Bronze / Silver / Gold |

| Cloud Platforms | AWS / Databricks |



\---



\## Data Source



The project uses the \*\*U.S. Energy Information Administration (EIA) Electric Power Operations API\*\*.



The source provides monthly electricity generation records including:



\- Reporting period

\- Location

\- State

\- Sector

\- Fuel type

\- Electricity generation

\- Generation units



\### Coverage



```text

Start: January 2020

End:   December 2025

Records: 22,610

Months: 72

```



\---



\# Pipeline



\## 1. API Ingestion



The ingestion layer was developed in Python using the EIA REST API.



The API returned a maximum of 5,000 records per request, while the complete dataset contained 22,610 records.



The ingestion process therefore implemented offset-based pagination:



```text

Offset 0

Offset 5,000

Offset 10,000

Offset 15,000

Offset 20,000

```



The complete dataset was retrieved and written to AWS S3:



```text

s3://virginia-energy-data-2026/raw/va\_generation\_2020\_2025.json

```



The raw dataset is intentionally \*\*not committed to this GitHub repository\*\*. AWS S3 serves as the cloud raw-data landing layer for the pipeline.



\---



\## 2. Bronze Layer



The Bronze layer loads the raw JSON from S3 into Databricks.



The nested API response is flattened using PySpark:



```python

bronze\_df = (

&#x20;   df\_raw

&#x20;   .selectExpr("explode(response.data) as record")

&#x20;   .select("record.\*")

&#x20;   .withColumn("\_ingested\_at", current\_timestamp())

)

```



The Bronze layer preserves the source-level fields while adding an ingestion timestamp.



\### Bronze Validation



| Check | Result |

|---|---:|

| Total records | 22,610 |

| Distinct months | 72 |

| Earliest period | 2020-01 |

| Latest period | 2025-12 |

| Distinct fuel types | 36 |



Delta table:



```text

virginia\_energy\_bronze

```



\---



\## 3. Silver Layer



The Silver layer transforms the Bronze dataset into a structured, typed dataset.



Transformations include:



\- Converting reporting periods into date values

\- Converting generation values from strings to numeric values

\- Standardizing column names

\- Trimming fuel descriptions

\- Creating year and month fields

\- Preserving ingestion metadata



Example:



```python

silver\_df = (

&#x20;   spark.table("virginia\_energy\_bronze")

&#x20;   .select(

&#x20;       to\_date("period", "yyyy-MM").alias("period\_date"),

&#x20;       "location",

&#x20;       col("stateDescription").alias("state"),

&#x20;       col("sectorid").alias("sector\_id"),

&#x20;       col("sectorDescription").alias("sector"),

&#x20;       col("fueltypeid").alias("fuel\_type\_id"),

&#x20;       trim(col("fuelTypeDescription")).alias("fuel\_type"),

&#x20;       col("generation").alias("generation\_raw"),

&#x20;       col("generation").cast("double").alias("generation\_mwh"),

&#x20;       col("generation-units").alias("generation\_units"),

&#x20;       "\_ingested\_at"

&#x20;   )

)

```



\### Data Quality Checks



The Silver layer validates:



\- Null reporting dates

\- Invalid numeric generation values

\- Non-Virginia records

\- Missing fuel IDs

\- Missing generation units

\- Duplicate business-key combinations



All validation checks passed.



The duplicate check used:



```text

period\_date

location

sector\_id

fuel\_type\_id

```



Result:



```text

0 duplicate key groups

```



Delta table:



```text

virginia\_energy\_silver

```



\---



\## 4. Gold Layer



The Gold layer creates a monthly analytical dataset for downstream reporting.



A major modeling consideration was avoiding double-counting.



The EIA dataset contains overlapping fuel categories and multiple sector rollups. Simply summing every available record would produce incorrect generation totals.



For statewide generation analysis, the model uses:



```text

sector\_id = 99

```



representing \*\*All Sectors\*\*.



The model also uses selected fuel categories rather than summing overlapping aggregate and component categories.



For renewable generation, the model uses EIA's:



```text

AOR = All Renewables

```



rather than independently summing renewable components.



\### Gold Metrics



The Gold dataset contains:



\- Total generation

\- Renewable generation

\- Natural gas generation

\- Nuclear generation

\- Wind generation

\- Solar generation

\- Hydroelectric generation

\- Biomass generation

\- Coal generation

\- Renewable share

\- Natural gas share

\- Nuclear share

\- Coal share

\- Prior-year generation

\- YoY generation change

\- Rolling 12-month generation



Final Gold dataset:



```text

72 monthly records

2020-01 → 2025-12

```



Delta table:



```text

virginia\_energy\_gold\_generation\_mix

```



\---



\# Data Quality \& Validation



The pipeline validates the data at multiple stages rather than assuming the API output is analysis-ready.



\### Validation Summary



| Validation | Result |

|---|---:|

| Bronze records | 22,610 |

| Silver records | 22,610 |

| Gold records | 72 |

| Distinct months | 72 |

| Invalid generation values | 0 |

| Non-Virginia records | 0 |

| Null fuel IDs | 0 |

| Null generation units | 0 |

| Duplicate key groups | 0 |



\---



\# Analytical Results



The Gold dataset produced several notable trends.



\### Renewable Generation



Renewable generation represented between:



\*\*3.81% and 16.23%\*\*



of monthly electricity generation during the analyzed period.



The maximum observed renewable share was \*\*16.23% in April 2025\*\*.



\### Natural Gas



Natural gas represented between:



\*\*39.41% and 65.38%\*\*



of monthly electricity generation during the analyzed period.



\### December 2025



The final reporting month contained:



| Metric | Value |

|---|---:|

| Total Generation | 9,421.16 MWh |

| Renewable Share | 7.86% |

| Natural Gas Share | 63.63% |

| Nuclear Share | 22.50% |



\---



\# Power BI



The Gold Delta table is connected to Power BI through Databricks.



Power BI serves as the \*\*business-facing consumption layer\*\* rather than the primary data-processing layer.



\## Dashboard 1 — Virginia Energy Overview



The first page analyzes:



\- Total generation

\- Renewable generation

\- Renewable share

\- Natural gas share

\- Monthly generation trends

\- Generation mix over time



\## Dashboard 2 — Energy Transition Analysis



The second page analyzes:



\- Renewable share trends

\- Solar generation

\- Natural gas vs. nuclear generation

\- Year-over-year generation changes

\- Rolling 12-month generation

\- Fuel-level detail



\---



\# Engineering Challenges \& Decisions



\## API Pagination



The EIA API returned a maximum of 5,000 records per request.



Pagination was implemented to retrieve the complete 22,610-record dataset.



\## Nested API Response



The EIA response contains the actual records inside a nested array.



PySpark `explode()` was used to flatten the response into the Bronze layer.



\## Schema Transformation



Generation values were provided as strings and explicitly converted to numeric values in the Silver layer.



Reporting periods were converted from `yyyy-MM` strings into Spark date values.



\## Data Quality



Validation was performed for:



\- Record counts

\- Date ranges

\- Null values

\- Numeric conversion failures

\- Location consistency

\- Missing identifiers

\- Duplicate business keys



\## Avoiding Double Counting



The EIA dataset contains overlapping aggregate and granular fuel categories as well as multiple sector rollups.



The Gold model therefore uses the \*\*All Sectors\*\* rollup and carefully selected fuel categories rather than blindly summing every available record.



This ensures that the resulting analytical metrics represent meaningful electricity-generation totals.



\---



\# Repository Structure



```text

virginia-energy-pipeline/

│

├── notebooks/

│   ├── 01\_bronze\_ingestion

│   ├── 02\_silver\_transformation

│   ├── 03\_gold\_analytics

│   └── 04\_power\_bi

│

├── src/

│   └── ingestion/

│       └── eia\_ingestion.py

│

├── screenshots/

│   ├── virginia-energy-overview.png

│   └── energy-transition-analysis.png

│

├── README.md

└── requirements.txt

```



> \*\*Note:\*\* Raw electricity-generation data is stored in AWS S3 rather than committed to the repository. This keeps GitHub focused on the pipeline code, notebooks, documentation, and analytical outputs.



\---



\# Project Outcomes



This project demonstrates an end-to-end data engineering workflow covering:



\- REST API ingestion

\- API pagination

\- Python data extraction

\- AWS S3

\- Databricks

\- PySpark

\- Delta Lake

\- Medallion architecture

\- Schema transformation

\- Data-quality validation

\- Analytical data modeling

\- SQL

\- Power BI

\- Time-series analysis



The key design principle was to treat Power BI as the \*\*final consumption layer of an engineered data pipeline\*\*, rather than building a dashboard first and working backward.



\---



\# Skills Demonstrated



\### Data Engineering



`Python` `PySpark` `Databricks` `Delta Lake` `AWS S3` `REST APIs` `ETL` `Medallion Architecture` `Data Quality`



\### Data Analytics



`SQL` `Power BI` `DAX` `Time-Series Analysis` `Data Modeling` `KPI Development`



\### Cloud



`AWS S3` `Databricks`



\---



\# Future Improvements



Potential future iterations include:



\- Automated pipeline orchestration

\- Incremental API ingestion

\- Pipeline scheduling

\- Automated data-quality monitoring

\- Automated testing

\- Partitioning strategies for larger datasets

\- Databricks Lakeflow orchestration

\- Additional EIA datasets for demand, capacity, or emissions analysis



\---



\# Author



\*\*Isaiah\*\*



B.S. Data Analytics — Western Governors University



Microsoft Certified: Power BI Data Analyst Associate (PL-300)  

Microsoft Certified: Azure Database Administrator Associate (DP-300)



Interested in \*\*Data Engineering and Data Analytics\*\* roles involving Python, SQL, cloud platforms, data pipelines, and analytical systems.

