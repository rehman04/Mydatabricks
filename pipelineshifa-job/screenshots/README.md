# Screenshots

This folder contains DAG and data screenshots for the `pipelineshifa` pipeline and `shifaJOb` job.

## How to capture screenshots

### Pipeline DAG
1. Open the pipeline in Databricks: **Pipelines → pipelineshifa**
2. Click on a completed update to open the DAG view
3. Screenshot the graph and save as `pipeline_dag.png`

### Job DAG
1. Open the job in Databricks: **Jobs → shifaJOb**
2. Click on a completed run to open the run graph
3. Screenshot the task graph and save as `job_dag.png`

### Data Preview
1. Open a SQL editor and run:
   ```sql
   SELECT * FROM shifa.mywork.sales_stats LIMIT 20;
   ```
2. Screenshot the results table and save as `sales_stats_data.png`

## Expected files

| File | Description |
|------|-------------|
| `pipeline_dag.png` | Lakeflow Spark Declarative Pipeline DAG (source → streaming table → materialized view) |
| `job_dag.png` | Job run DAG (salespipeline → downstream) |
| `sales_stats_data.png` | Sample output from `shifa.mywork.sales_stats` |
