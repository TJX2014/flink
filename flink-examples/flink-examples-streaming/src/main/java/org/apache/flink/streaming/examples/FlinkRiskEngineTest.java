package org.apache.flink.streaming.examples;

import org.apache.flink.api.common.functions.*;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.formats.csv.CsvRowDeserializationSchema;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.KafkaDeserializationSchema;
import org.apache.flink.streaming.connectors.kafka.internals.KafkaDeserializationSchemaWrapper;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;
import org.apache.flink.util.StringUtils;
import org.json.JSONArray;
import org.json.JSONObject;

import java.io.IOException;
import java.io.Serializable;
import java.util.*;
import java.util.stream.Collectors;

public class FlinkRiskEngineTest {

    private static String[] parseDSL(String dsl) {
        return Arrays.stream(dsl.split("[(,)]]")).map(String::trim)
                .collect(Collectors.toList()).toArray(new String[0]);
    }

    private static final List<String[]> features =
            Arrays.asList(parseDSL("count(pay_account.history,1h)"),
                    parseDSL("sum(amount#rcv_account.history,1h)"),
                    parseDSL("count_distinct(rcv_account#pay_account.history,1h)"));

    private static final Set<String> featureKeys = features.stream().map(x -> x[1]).collect(Collectors.toSet());

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        env.enableCheckpointing(5000);

        Properties properties = new Properties();
        properties.put("bootstrap.servers", "localhost:9092");

        final TypeInformation<Row> rowInfo = Types.ROW(Types.STRING);
        CsvRowDeserializationSchema schema = new CsvRowDeserializationSchema.Builder(rowInfo).build();
        KafkaDeserializationSchema deserializationSchema = new KafkaDeserializationSchemaWrapper(schema);
        FlinkKafkaConsumer<String> kafkaConsumer = new FlinkKafkaConsumer<String>(
                "test", deserializationSchema, properties);

        DataStreamSource source = env.addSource(kafkaConsumer);
        source.map(new MapFunction<String, JSONObject>() {
            @Override
            public JSONObject map(String value) throws Exception {
                if (StringUtils.isNullOrWhitespaceOnly(value)) {
                    return new JSONObject();
                } else {
                    return new JSONObject(value);
                }
            }
        }).flatMap(new FlatMapFunction<JSONObject, JSONObject>() {
            private final Set<String> keys = featureKeys;

            @Override
            public void flatMap(JSONObject value, Collector<JSONObject> out) throws Exception {
                // 让同一个用户用户的特征可以聚集
                String eventId = UUID.randomUUID().toString();
                long timestamp = value.getLong("timestamp");
                JSONObject event = new JSONObject();
                event.put("KEY_NAME", "event");
                event.put("KEY_VALUE", eventId);
                event.put("EVENT_ID", eventId);
                // 将原始数据输入
                event.putOpt("ORIGIN", value);
                out.collect(event);

                // 每一部分特征分别计算
                keys.forEach(key -> {
                    JSONObject json = new JSONObject();
                    try {
                        json.put("timestamp", timestamp);
                        json.put("KEY_NAME", key);
                        json.put("KEY_VALUE", genKeyValue(value, key));
                        json.put("EVENT_ID", eventId);
                        genKeyFields(key).forEach(f -> {
                                json.put(f, value.get(f));
                                out.collect(json);
                        });
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                });
            }

            // 划分keyedStream
            private String genKeyValue(JSONObject event, String key) {
                if (!key.endsWith(".history")) {
                    throw new UnsupportedOperationException("unsupported operate");
                }
                String[] splits = key.replace(".history", "").split("#");
                String keyValue;
                if (splits.length == 1) {
                    // 没有条件字段，目标字段当做条件字段，根据条件字段的值生成分区key
                    String target = splits[0];
                    keyValue = String.format("%s#%s.history", target, event.get(target));
                } else if (splits.length == 2) {
                    // # 后的字段为分别key
                    String target = splits[0];
                    String on = splits[1];
                    keyValue = String.format("%s#%s.history", target, event.get(on));
                } else {
                    throw new UnsupportedOperationException("key type not support");
                }
                return keyValue;
            }

            private Set<String> genKeyFields(String key) {
                if (!key.endsWith(".history")) {
                    throw new UnsupportedOperationException("key type not support");
                }
                String[] splits = key.replace(".history", "").split("#");
                return new HashSet<>(Arrays.asList(splits));
            }
        }).keyBy(new KeySelector<JSONObject, String>() {
            @Override
            public String getKey(JSONObject value) throws Exception {
                return value.getString("KEY_VALUE");
            }
        }).map(new KeyEnrichFunction()).map(new FeatureEnrichFunction()).keyBy(new KeySelector<JSONObject, String>() {
            @Override
            public String getKey(JSONObject value) throws Exception {
                return value.getString("EVENT_ID");
            }
        }).flatMap(new FeatureReduceFunction()).map(new RuleBasedModeling()).print().setParallelism(1);

        env.execute("risk engine");
    }

    public static class KeyEnrichFunction extends RichMapFunction<JSONObject, JSONObject> {

        private ValueState<Serializable> keyState;

        @Override
        public void open(Configuration parameters) throws Exception {
            keyState = getRuntimeContext().getState(
                    new ValueStateDescriptor<>("savedKeyState", Serializable.class));
//            super.open(parameters);
        }

        private <T> T getState(Class<T> tClass) throws IOException {
            return tClass.cast(keyState.value());
        }
//
        private void setState(Serializable v) throws IOException {
            keyState.update(v);
        }

        @Override
        public JSONObject map(JSONObject event) throws Exception {
            String keyName = event.getString("KEY_NAME");
            if (keyName.endsWith("event")) {
                return event;
            }
            if (keyName.endsWith(".history")) {
                SJSONArray history = getState(SJSONArray.class);
                if (history == null) {
                    history = new SJSONArray();
                }
                history.put(event);
                //保留50个
                if (history.length() > 50) {
                    history.put(event);
                }
                setState(history);
                JSONObject newEvent = new JSONObject();
                newEvent.putOpt("ORIGIN", event);
                return newEvent;
            } else {
                throw new UnsupportedOperationException("unsupported type:" + keyName);
            }
        }
    }

    public static class FeatureEnrichFunction extends RichMapFunction<JSONObject, JSONObject> {

        private static final List<String[]> features = FlinkRiskEngineTest.features;

        @Override
        public JSONObject map(JSONObject value) throws Exception {
            String keyName = value.getString("KEY_NAME");
            if (keyName.equals("event")) {
                return value;
            }
            //找keyName关联的特征，进行特征计算
            for (String [] feature: features) {
                String key = feature[1];
                if (!keyName.equals(key)) {
                    continue;
                }
                //定义第一个值
                String function = feature[0];
                long window = Long.parseLong(feature[2]);
                JSONArray history = value.getJSONArray("HISTORY");
                String target = key.replace(".history", "").split("#")[0];
                Object featureResult;
//                根据定义的特征公式计算，这里用if else简单计算
                if ("sum".equalsIgnoreCase(function)) {
                    featureResult = doSum(history, target, window);
                }
                featureResult = "111-tt";
                String featureName = String.format("%s(%s,%s)", feature[0], feature[1], feature[2]);
                JSONObject featureObj = value.getJSONObject("feature");
                if (featureObj == null) {
                    featureObj = new JSONObject();
                }
                featureObj.put(featureName, featureResult);
            }
            return value;
        }

        private Object doSum(JSONArray history, String target, long window) {
            return window + 111;
        }
    }

    public static class FeatureReduceFunction extends RichFlatMapFunction<JSONObject, JSONObject> {

        private ValueState<JSONObject> merged;
        private static final List<String[]> features = FlinkRiskEngineTest.features;

        @Override
        public void open(Configuration parameters) throws Exception {
            merged = getRuntimeContext().getState(new ValueStateDescriptor<JSONObject>(
                    "saved reduceFunc", JSONObject.class));
            super.open(parameters);
        }

        @Override
        public void flatMap(JSONObject value, Collector<JSONObject> out) throws Exception {
            JSONObject mergedValue = merged.value();
            if (mergedValue == null) {
                mergedValue = new JSONObject();
            }
            String keyName = value.getString("KEY_NAME");
            if (keyName.equalsIgnoreCase("event")) {
                mergedValue.put("event", value);
            } else {
                JSONObject featureObj = mergedValue.getJSONObject("features");
                if (featureObj == null) {
                    mergedValue.put("features", new JSONObject());
                }
                if (value.has("features")) {
                    mergedValue.getJSONObject("features").put("ORIGIN", value.getJSONObject("features"));
                }
            }
            // 如果收集完所有的feature
            if (mergedValue.has("event") && mergedValue.has("features") &&
                mergedValue.getJSONObject("features").keySet().size() == features.size()) {
                out.collect(mergedValue);
                merged.clear();
            } else {
                // 还没有收集好，只更新，不输出
                merged.update(mergedValue);
            }
        }
    }

    public static class RuleBasedModeling implements MapFunction<JSONObject, JSONObject> {

        @Override
        public JSONObject map(JSONObject value) throws Exception {
            boolean isAnomaly = (value.getJSONObject("features").getDouble("(count(pay_account.history.1h))") > 5)
                    && (value.getJSONObject("features").getDouble("count_distinct(rcv_account#pay_account.history.1h)") <=2);
            value.put("isAnomaly", isAnomaly);
            return value;
        }
    }

    public static class SJSONArray extends JSONArray implements Serializable{
    }


}
