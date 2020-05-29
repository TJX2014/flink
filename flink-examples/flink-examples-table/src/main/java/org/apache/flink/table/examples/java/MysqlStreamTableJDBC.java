package org.apache.flink.table.examples.java;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableResult;

public class MysqlStreamTableJDBC {

	/**
	 *
	 *
 docker run -d  -e MYSQL_USER=root -e MYSQL_ROOT_PASSWORD=123456 -e MYSQL_DATABASE=flink -p 3307:3306 mariadb:latest
	create database flink;
	use flink;

	 CREATE TABLE currency (
	 currency_id BIGINT,
	 currency_name varchar(255),
	 rate DOUBLE,
	 currency_timestamp  TIMESTAMP,
	 country varchar(255),
	 precise_timestamp TIMESTAMP(6),
	 precise_time TIME(6),
	 gdp DECIMAL(10, 6)
	 );

	 insert into flink.currency(
	 currency_id,currency_name,rate,currency_timestamp,country,precise_timestamp,precise_time,gdp) values
	 (1,'dollar',102,'2020-05-06 13:07:29','China','2020-05-06 13:07:29.166132','13:07:29.166132',-132.576);

	 * */


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
			"   'url' = 'jdbc:mysql://0.0.0.0:3307/flink',\n" +
			"   'username' = 'root',\n" +
			"   'password' = '123456',\n" +
			"   'table-name' = 'currency',\n" +
			"   'driver' = 'com.mysql.jdbc.Driver',\n" +
			"   'lookup.cache.max-rows' = '500',\n" +
			"   'lookup.cache.ttl' = '10s',\n" +
			"   'lookup.max-retries' = '3')");

//		org.mariadb.jdbc.Driver

		TableResult result = tEnv.executeSql("select * from currency");
		result.print();

	}
}
