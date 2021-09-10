#!/bin/bash

#'/home/pmeadm/clv-cpa-retention-curves/weigthed_curves/weighted_curves_'
input_file='/data/weighted_curves_'
table_name='pme.clv_weighted_curves_'
pmaxCosts='/data/pMaxCosts.csv'

for s in week month
do
  historize_query="INSERT INTO pme.clv_weighted_curves_${s}_history SELECT * FROM pme.clv_weighted_curves_${s}"
  truncate_table="TRUNCATE TABLE pme.clv_weighted_curves_${s}"
  deduplicate="OPTIMIZE TABLE pme.clv_weighted_curves_${s}_history FINAL DEDUPLICATE"

  clickhouse-client --format_csv_delimiter=";" --query="$historize_query"
  clickhouse-client --format_csv_delimiter=";" --query="$truncate_table"
  clickhouse-client --format_csv_delimiter=";" --query="$deduplicate"

  echo [Loading ${s}ly curves on Clickhouse ${table_name}${s} ...]

  cat ${input_file}${s}.csv | clickhouse-client --format_csv_delimiter=";" --query="INSERT INTO ${table_name}${s} FORMAT CSVWithNames"
  clickhouse-client --format_csv_delimiter=";" --query="$historize_query"
  clickhouse-client --format_csv_delimiter=";" --query="$deduplicate"
done


cat ${pmaxCosts} | clickhouse-client --format_csv_delimiter=";" --query="INSERT INTO pme.clv_pmax_costs FORMAT CSVWithNames"
deduplicate_costs="OPTIMIZE TABLE pme.clv_pmax_costs FINAL DEDUPLICATE"
clickhouse-client --format_csv_delimiter=";" --query="$deduplicate_costs"
