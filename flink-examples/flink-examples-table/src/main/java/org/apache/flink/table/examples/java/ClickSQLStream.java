package org.apache.flink.table.examples.java;


import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

import static org.apache.flink.table.api.Expressions.$;


public class ClickSQLStream {

    public static class ClickItem {
        public String name;
        public String url;

        public ClickItem() {
        }

        public ClickItem(String name, String url) {
            this.name = name;
            this.url = url;
        }
    }

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<String> inputStream = env.socketTextStream("localhost", 11111, "\n");
//        DataStream srcTable = inputStream.map(new MapFunction<String, ClickItem>() {
//            @Override
//            public ClickItem map(String value) throws Exception {
//                String[] parts = value.split(",");
//                return new ClickItem(parts[0], parts[1]);
//            }
//        });

        DataStream srcTable = inputStream.flatMap(new FlatMapFunction<String, ClickItem>() {
            @Override
            public void flatMap(String value, Collector<ClickItem> out) throws Exception {
                String[] parts = value.split(",");
                out.collect(new ClickItem(parts[0], parts[1]));
            }
        });

        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        tEnv.createTemporaryView("clicks", srcTable, $("name"), $("url"));

//        tEnv.createTemporaryView("target", srcTable.print());;
        Table result = tEnv.sqlQuery("select name,count(url) as cnt from clicks group by name");
        tEnv.toRetractStream(result, Row.class).print();

        env.execute("start clicks");
//        tEnv.execute("start clicks");
    }
}
