# sdp-rtm-rate-source record

This folder contains a record of the `sdp-rtm-rate-source` Declarative Automation Bundle, the plain-Python pipeline code, a notebook-equivalent source file without `# MAGIC`, and a runtime summary.

## Included files

* `databricks.yml` — bundle configuration
* `sdp-rtm-rate-source/transformations/temperature_rtm.py` — deployed pipeline library
* `notebooks/temperature_rtm_table_pipeline.py` — notebook-equivalent plain Python source
* `artifacts/runtime_summary.md` — output table and driver log summary

## Published output

* Output table: `shifa.myschema.hot_temperatures`
* Dataset type: streaming table
* Description: windowed temperature aggregates generated from the Spark `rate` source

## Driver logs

The pipeline emits simple `print()` messages to driver `stdout`:

* `Loading pipeline library: hot_temperatures`
* `Building streaming table hot_temperatures`

Python exceptions and tracebacks appear in `stderr`.

## Notes

* No notebook `# MAGIC` directives are included.
* The pipeline runs continuously with serverless compute and real-time mode enabled.
