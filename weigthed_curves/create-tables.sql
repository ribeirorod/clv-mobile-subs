CREATE TABLE  pme.clv_weighted_curves_week
(
	joined_week UInt64,
	region String,
	country String,
	network_operator String,
	advertiser String,
	periodicity String,
	period String,
	rates Float64,
	lifetime Float64
)
ENGINE = ReplicatedReplacingMergeTree('/clickhouse/tables/{shard}/pme/clv_weighted_curves_week',
 '{replica}')
PARTITION BY joined_week
ORDER BY (joined_week,	region ,	country ,	network_operator ,	advertiser ,	periodicity ,	period)
SETTINGS index_granularity = 8192;

CREATE TABLE  pme.clv_weighted_curves_month
(
	joined_month UInt64,
	region String,
	country String,
	network_operator String,
	advertiser String,
	periodicity String,
	period String,
	rates Float64,
	lifetime Float64
)
ENGINE = ReplicatedReplacingMergeTree('/clickhouse/tables/{shard}/pme/clv_weighted_curves_month',
 '{replica}')
PARTITION BY joined_month
ORDER BY (joined_month,	region ,	country ,	network_operator ,	advertiser ,	periodicity ,	period)
SETTINGS index_granularity = 8192;