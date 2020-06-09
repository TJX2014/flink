package org.apache.flink.streaming.examples.tables;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;

public class FlinkToHBase {

    private static final List<Row> testData2 = new ArrayList<>();
    private static final RowTypeInfo testTypeInfo2 = new RowTypeInfo(
            new TypeInformation[]{Types.INT, Types.INT, Types.STRING, Types.LONG, Types.DOUBLE,
                    Types.BOOLEAN, Types.STRING, Types.SQL_TIMESTAMP, Types.SQL_DATE, Types.SQL_TIME},
            new String[]{"rowkey", "f1c1", "f2c1", "f2c2", "f3c1", "f3c2", "f3c3", "f4c1", "f4c2", "f4c3"});

    public static void main(String[] args) throws ExecutionException, InterruptedException {

        testData2.add(Row.of(1, 10, "Hello-1", 100L, 1.01, false, "Welt-1",
                Timestamp.valueOf("2019-08-18 19:00:00"), Date.valueOf("2019-08-18"), Time.valueOf("19:00:00")));

        EnvironmentSettings streamSettings = EnvironmentSettings.newInstance()
                .inStreamingMode().useBlinkPlanner().build();
        StreamExecutionEnvironment execEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(execEnv, streamSettings);

        // prepare a source table
        String srcTableName = "src";

        DataStream<Row> ds = execEnv.fromCollection(testData2).returns(testTypeInfo2);
        tEnv.createTemporaryView(srcTableName, ds);

        tEnv.executeSql(
                "CREATE TABLE hbase (" +
                        " family1 ROW<col1 INT>," +
                        " family2 ROW<col1 STRING, col2 BIGINT>," +
                        " rk INT," +
                        " family3 ROW<col1 DOUBLE, col2 BOOLEAN, col3 STRING>" +
                        ") WITH (" +
                        " 'connector' = 'hbase-1.4'," +
                        " 'table-name' = 'testTable1'," +
                        " 'zookeeper.quorum' = 'localhost:2181'," +
                        " 'zookeeper.znode.parent' = '/hbase'" +
                        ")");


        String query = "INSERT INTO hbase SELECT ROW(f1c1), ROW(f2c1, f2c2), rowkey, ROW(f3c1, f3c2, f3c3) FROM src";

        TableResult tableResult = tEnv.executeSql(query);
        // wait to finish
        tableResult.getJobClient().get()
                .getJobExecutionResult(Thread.currentThread().getContextClassLoader()).get();

        tableResult.print();

    }
}
