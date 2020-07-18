package org.apache.flink.streaming.examples;

import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoFlatMapFunction;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

public class ControlStreamDemo {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        List<Tuple1<String>> controls = new ArrayList<>();
        controls.add(new Tuple1<>("YELLOW"));
        controls.add(new Tuple1<>("BLUE"));

        DataStreamSource<Tuple1<String>> cStream = env.fromCollection(controls);

        List<Tuple1<String>> data = new ArrayList<>();

        for (int i = 0; i< 10000; i++) {
            data.add(new Tuple1<>("BLUE"));
            data.add(new Tuple1<>("READ"));
            data.add(new Tuple1<>("BLACK"));
        }
        KeyedStream<Tuple1<String>, Tuple> dStream =
                env.fromCollection(data).keyBy(0);

        DataStream resultStream = cStream.broadcast().connect(dStream).flatMap(
                new CoFlatMapFunction<Tuple1<String>, Tuple1<String>, Object>() {

                    HashSet blackList = new HashSet();

                    @Override
                    public void flatMap1(Tuple1<String> controlValue, Collector<Object> out) throws Exception {
                        blackList.add(controlValue);
                    }

                    @Override
                    public void flatMap2(Tuple1<String> dataValue, Collector<Object> out) throws Exception {
                        if (blackList.contains(dataValue)) {
                            out.collect("invalid color:" + dataValue);
                        } else {
                            out.collect("valid color:" + dataValue);
                        }
                    }
                }
        ).forward();
        resultStream.print();

        env.execute();
    }
}
