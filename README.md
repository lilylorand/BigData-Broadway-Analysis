# Broadway Big Data Project

## Project Overview
This project analyzes Broadway data using Hadoop MapReduce and Spark on NYU Dataproc.

The project includes:
- Data ingestion into HDFS
- Profiling
- ETL / cleaning
- Analytics with MapReduce
- Spark comparison tables

The main analysis compares:
- Summer vs winter by year
- Summer vs winter by year (Excluding blockbusters that earned over 1 billion dollars such as "Wicked" and "The Lion King")
- Dataset from 1990 up to 2016 vs Dataset from 2016 up to 2026

## Data Processing

The project implements a custom ETL pipeline to transform raw Broadway data into structured analytical datasets.

The transformations include:
- Mapping `Date.Month` to seasonal categories based on tourism (summer: 6–8, winter: 11–1) 
- Cleaning numeric fields by removing commas and converting to numeric types
- Creating two cleaned datasets:
  - Full dataset with show names (used for filtering major shows, to avoid bias)
  - Recent dataset without show names (due to schema differences)
- Standardizing output format:
  year, season, attendance, capacity, gross

These transformations allow consistent analytics across datasets with different schemas.

## Code Improvements

Several improvements were made during development:

- Updated CSV parsing to use Apache Commons CSV instead of manual string splitting to correctly handle quoted fields.
- Fixed numeric parsing issues in the 2016–2026 dataset by removing commas before converting values to numeric types.
- Adjusted MapReduce mappers to handle differences in schema between the full dataset and recent dataset.
- Improved job reliability by ensuring HDFS output directories are removed before each run.
- Created reusable scripts (`build.sh` and `run_all.sh`) to automate compilation and execution.

---

## Directory Structure

### `/ana_code`
Contains the analytics and Spark code, compiled class files, jar files, and supporting library.

Main files:
- `FirstCode.java`
- `FirstCodeMapper.java`
- `FirstCodeReducer.java`
- `StatsAnalysis.java`
- `StatsAnalysisMapper.java`
- `StatsAnalysisReducer.java`
- `SeasonStats.java`
- `SeasonStatsMapper.java`
- `SeasonStatsReducer.java`
- `make_comparison_tables.py`
- `broadway_analytics.jar`
- `commons-csv-1.10.0.jar`

Additional ETL drivers used by analytics are also included here for build/run convenience:
- `CleanWithShow.java`
- `CleanWithShowMapper.java`
- `CleanWithShowReducer.java`
- `CleanRecent.java`
- `CleanRecentMapper.java`
- `CleanRecentReducer.java`

### `/data_ingest`
Contains the input files and ingestion commands.

Files:
- `broadway.csv`
- `Broadway2016_26.csv`
- `ingest_commands.txt`

### `/etl_code/lily`
Contains ETL / cleaning code.

Files:
- `Clean.java`
- `CleanMapper.java`
- `CleanReducer.java`
- `CleanWithShow.java`
- `CleanWithShowMapper.java`
- `CleanWithShowReducer.java`
- `CleanRecent.java`
- `CleanRecentMapper.java`
- `CleanRecentReducer.java`

### `/profiling_code/lily`
Contains profiling code.

Files:
- `UniqueRecs.java`
- `UniqueRecsMapper.java`
- `UniqueRecsReducer.java`

### `/screenshots`
Contains screenshots showing:
- input data in HDFS
- ETL jobs running
- ETL outputs
- analytics jobs running
- analytics outputs
- Spark table outputs

### `/test_code`
Optional folder for extra or unused test files.

---

## Input Data Used

Two input datasets were used:

1. `broadway.csv` from [Corgis data project](https://corgis-edu.github.io/corgis/csv/broadway/)
2. `Broadway2016_26.csv` [ibdb](https://www.ibdb.com/statistics/)

They were uploaded to HDFS here:

```bash
/user/$USER/input/broadway.csv
/user/$USER/input/Broadway2016_26.csv
```

To verify access to the input data:

```bash
hdfs dfs -ls /user/$USER/input
```

## How to Build the Code

Navigate to the analytics directory:

```bash
cd ~/BigData_Final_Submission/ana_code
```

Build using provided script:
```bash
./build.sh
```

To run the full pipline of code (after building):
```bash
./run_all.sh
```

## Manual Execution:

The pipeline can also be run manually without using scripts:

### Cleaning full dataset
```bash
hdfs dfs -rm -r /user/$USER/clean_with_show_output
hadoop jar broadway_analytics.jar CleanWithShow \
/user/$USER/input/broadway.csv \
/user/$USER/clean_with_show_output
```

### Cleaning recent dataset
```bash
hdfs dfs -rm -r /user/$USER/clean_recent_output
hadoop jar broadway_analytics.jar CleanRecent \
/user/$USER/input/Broadway2016_26.csv \
/user/$USER/clean_recent_output
```

### Seasonal analytics (full dataset)
```bash
hdfs dfs -rm -r /user/$USER/season_output_full
hadoop jar broadway_analytics.jar SeasonStats \
/user/$USER/clean_with_show_output \
/user/$USER/season_output_full
```

### Seasonal analytics (recent dataset)
```bash
hdfs dfs -rm -r /user/$USER/season_output_recent
hadoop jar broadway_analytics.jar SeasonStats \
/user/$USER/clean_recent_output \
/user/$USER/season_output_recent
```

### Spark comparison tables
```bash
spark-submit make_comparison_tables.py
```

## Where to Find Results

All outputs from the pipeline are stored in HDFS under `/user/$USER/`.

- Profiling Output  
  `/user/$USER/unique_output`

- Cleaned Data (Full Dataset with Show Names)  
  `/user/$USER/clean_with_show_output`

- Cleaned Data (Recent Dataset)  
  `/user/$USER/clean_recent_output`

- Seasonal Analytics (Full Dataset)  
  `/user/$USER/season_output_full`

- Seasonal Analytics (Recent Dataset)  
  `/user/$USER/season_output_recent`

- Spark Comparison Table (All Shows)  
  `/user/$USER/comparison_table_all`

- Spark Comparison Table (No Big Shows)  
  `/user/$USER/comparison_table_no_big`

To view any output:

```bash
hdfs dfs -cat <HDFS_PATH>/part-* | head
```
