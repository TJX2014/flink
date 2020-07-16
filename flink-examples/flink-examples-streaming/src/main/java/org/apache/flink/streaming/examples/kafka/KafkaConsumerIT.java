package org.apache.flink.streaming.examples.kafka;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.formats.csv.CsvRowDeserializationSchema;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.connectors.kafka.KafkaTableSource;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;

import java.util.Properties;

public class KafkaConsumerIT {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        Properties properties = new Properties();
        final TypeInformation<Row> rowInfo = Types.ROW(Types.STRING, Types.STRING);
        CsvRowDeserializationSchema schema = new CsvRowDeserializationSchema.Builder(rowInfo).build();
        KafkaTableSource source = new KafkaTableSource(TableSchema.builder().build(), "test", properties, schema);
        DataStream ds = source.getDataStream(env).filter(new FilterFunction<Row>() {
            @Override
            public boolean filter(Row value) throws Exception {
                return true;
            }
        }).map(new MapFunction<Row, Object>() {
            @Override
            public Object map(Row value) throws Exception {
                return null;
            }
        });
        final String INSERT_TEMPLATE = "insert into %s (id, title, author, price, qty) values (?,?,?,?,?)";
        final String INPUT_TABLE = "books";
        ds.addSink(JdbcSink.sink(
                String.format(INSERT_TEMPLATE, INPUT_TABLE),
                (ps, t) -> {
                    ps.setInt(1, 111);
                    ps.setString(2, "hello");
                },
                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                        .withUrl("")
                        .withDriverName("")
                        .build()));
        ds.print();
        env.execute();
    }
}
