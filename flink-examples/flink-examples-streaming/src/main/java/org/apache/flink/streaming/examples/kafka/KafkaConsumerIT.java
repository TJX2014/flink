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
import org.apache.flink.streaming.connectors.kafka.KafkaTableSource;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;

import java.util.Properties;

/**
 * nohup bin/kafka-server-start.sh config/server.properties > /tmp/logs &
 * <p>
 * bin/kafka-topics.sh --create --bootstrap-server localhost:9091 --topic test1 --partitions 1 --replication-factor 1
 * <p>
 * bin/kafka-topics.sh --create --zookeeper localhost:2181 --topic test1 --partitions 1 --replication-factor 1
 * <p>
 * bin/kafka-console-producer.sh --topic test1 --broker-list localhost:9092
 * bin/kafka-console-consumer.sh --topic test1 --bootstrap-server localhost:9092
 */

public class KafkaConsumerIT {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        Properties properties = new Properties();
        properties.put("bootstrap.servers", "localhost:9092");
        final TypeInformation<Row> rowInfo = Types.ROW(Types.STRING, Types.STRING);
        CsvRowDeserializationSchema schema = new CsvRowDeserializationSchema.Builder(rowInfo).build();
        KafkaTableSource source = new KafkaTableSource(TableSchema.builder().build(), "test1", properties, schema);

        DataStream<Row> ds = source.getDataStream(env).filter(new FilterFunction<Row>() {
            @Override
            public boolean filter(Row value) throws Exception {
                return true;
            }
        }).map(new MapFunction<Row, Row>() {
            @Override
            public Row map(Row value) throws Exception {
                return value;
            }
        });
        ds.addSink(JdbcSink.sink(
                "insert into books (id, title) values (?,?)",
                (ps, t) -> {
                    ps.setInt(1, 111);
                    ps.setString(2, "hello");
                },
                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                        .withUsername("xiaoxing")
                        .withPassword("123456")
                        .withUrl("mysql:jdbc://localhost:3306")
                        .withDriverName("com.mysql.jdbc.Driver")
                        .build()));

//        Configuration conf = new Configuration();
//        HBaseTableSchema hTableSchema = new HBaseTableSchema();
//        new LegacyMutationConverter(hTableSchema);
//        HBaseSinkFunction hSink = new HBaseSinkFunction("h111", conf,
//                new LegacyMutationConverter(hTableSchema), 1, 1, 1);
//        ds.addSink(hSink);
        ds.print();
        env.execute();
    }
}
