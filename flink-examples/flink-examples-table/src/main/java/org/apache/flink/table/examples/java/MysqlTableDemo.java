package org.apache.flink.table.examples.java;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class MysqlTableDemo {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment bsEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettings bsSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamTableEnvironment bsTableEnv = StreamTableEnvironment.create(bsEnv, bsSettings);
        bsEnv.setParallelism(1);

        //        mysql ddl:
//        create table bbb(area_code varchar(255), stat_date date, `index` bigint, is_record varchar(8));
//        create table aaa(area_code varchar(255), stat_date date, `index` bigint);
//         insert into bbb (is_record, area_code) values('是', 'SH')

        bsTableEnv.executeSql("CREATE TABLE bbb (\n" +
                "    `area_code`	VARCHAR,\n" +
                "    `stat_date`	        DATE,\n" +
                "    `index`		BIGINT,\n" +
                "     `is_record` varchar," +
                "    PRIMARY KEY (area_code, stat_date) NOT ENFORCED" +
                ") WITH (\n" +
                "  'connector'  = 'jdbc',\n" +
                "  'url'        = 'jdbc:mysql://localhost:3306/test',\n" +
                "  'table-name' = 'bbb',\n" +
                "  'driver'     = 'com.mysql.cj.jdbc.Driver',\n" +
                "  'username'   = 'xiaoxing',\n" +
                "  'password'   = '123456'" +
                ")");


        bsTableEnv.executeSql("CREATE TABLE aaa(\n" +
                "    `area_code`	VARCHAR,\n" +
                "    `stat_date`	        DATE,\n" +
                "    `index`		BIGINT,\n" +
                "    PRIMARY KEY (area_code, stat_date) NOT ENFORCED" +
                ") WITH (\n" +
                "  'connector'  = 'jdbc',\n" +
                "  'url'        = 'jdbc:mysql://localhost:3306/test',\n" +
                "  'table-name' = 'aaa',\n" +
                "  'driver'     = 'com.mysql.cj.jdbc.Driver',\n" +
                "  'username'   = 'xiaoxing',\n" +
                "  'password'   = '123456'\n" +
                ")");


        bsTableEnv.executeSql("INSERT INTO aaa SELECT area_code, CURRENT_DATE AS stat_date, count(*) AS index FROM bbb WHERE is_record = '是' GROUP BY area_code");

        bsTableEnv.execute("action result");
//		Table table = bsTableEnv.sqlQuery("SELECT area_code, CURRENT_DATE AS stat_date, count(*) AS index FROM bbb WHERE is_record = '是' GROUP BY area_code");
//		DataStream<Tuple2<Boolean, Row>> retractStream =
//				bsTableEnv.toRetractStream(table, Row.class);
//		retractStream.print();
//		bsEnv.execute();
    }
}
