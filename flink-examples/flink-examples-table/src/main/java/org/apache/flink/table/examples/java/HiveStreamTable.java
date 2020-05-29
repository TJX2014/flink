package org.apache.flink.table.examples.java;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableResult;

public class HiveStreamTable {

	public static void main(String[] args) {
		EnvironmentSettings settings = EnvironmentSettings.newInstance().build();
		settings.toPlannerProperties().put("table.sql-dialect", "hive");
		TableEnvironment tEnv = TableEnvironment.create(settings);
//		tEnv.executeSql("set table.sql-dialect=hive");
		tEnv.executeSql("" +
			"CREATE TABLE hive_table (\n" +
			"  user_id STRING,\n" +
			"  order_amount DOUBLE\n" +
			") PARTITIONED BY (\n" +
			"  dt STRING,\n" +
			"  hour STRING\n" +
			") STORED AS PARQUET TBLPROPERTIES (\n" +
			"  'sink.partition-commit.trigger'='partition-time',\n" +
			"  'partition.time-extractor.timestamp-pattern'=’$dt $hour:00:00’,\n" +
			"  'sink.partition-commit.delay'='1 h',\n" +
			"  'sink.partition-commit.policy.kind’='metastore,success-file'\n" +
			")");
//		SET table.sql-dialect=default;
		tEnv.executeSql("\n" +
			"CREATE TABLE kafka_table (\n" +
			"  user_id STRING,\n" +
			"  order_amount DOUBLE,\n" +
			"  log_ts TIMESTAMP(3),\n" +
			"  WATERMARK FOR log_ts AS log_ts - INTERVAL '5' SECOND\n" +
			")");
		TableResult result = tEnv.executeSql("INSERT INTO TABLE hive_table SELECT user_id, order_amount, DATE_FORMAT(log_ts, 'yyyy-MM-dd'), DATE_FORMAT(log_ts, 'HH') FROM kafka_table;");
		result.print();
	}
}
