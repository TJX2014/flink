package org.apache.flink.table.examples.java;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class MysqlStreamTableJDBC {


	/**
	 * debug
	 * */
	public static void main(String[] args) throws Exception {
		EnvironmentSettings settings = EnvironmentSettings.newInstance().build();
		TableEnvironment tEnv = TableEnvironment.create(settings);

		tEnv.executeSql("CREATE TABLE currency (\n" +
			"  currency_id BIGINT,\n" +
			"  currency_name STRING,\n" +
			"  rate DOUBLE,\n" +
			"  currency_timestamp  TIMESTAMP,\n" +
			"  country STRING,\n" +
			"  precise_timestamp TIMESTAMP(6),\n" +
			"  precise_time TIME(6),\n" +
			"  gdp DECIMAL(10, 6)\n" +
			") WITH (\n" +
			"   'connector' = 'jdbc',\n" +
			"   'url' = 'jdbc:mysql://localhost:3306/flink',\n" +
			"   'username' = 'root',\n" +
			"   'password' = '123456',\n" +
			"   'table-name' = 'currency',\n" +
			"   'driver' = 'com.mysql.jdbc.Driver',\n" +
			"   'lookup.cache.max-rows' = '500',\n" +
			"   'lookup.cache.ttl' = '10s',\n" +
			"   'lookup.max-retries' = '3')");
		Table t = tEnv.sqlQuery("select * from currency");
		t.execute();
//		env.execute();

	}
}
