import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.kafka.common.TopicPartition;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.api.common.functions.MapFunction;


import java.util.Arrays;
import java.util.HashSet;

public class Main {
    static final String BROKERS = "kafka:9092";

    public static void main(String[] args) throws Exception {
        Thread.sleep(60000); // service sleeps for first 60 secs

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        KafkaSource<Order> source = KafkaSource.<Order>builder()
                                        .setBootstrapServers(BROKERS)
                                        .setProperty("partition.discovery.interval.ms", "1000")
                                        .setTopics("order")
                                        .setGroupId("groupdId-919292")
                                        .setStartingOffsets(OffsetsInitializer.earliest())
                                        .setValueOnlyDeserializer(new OrderDeserializationSchema())
                                        .build();

        DataStreamSource<Order> kafka = env.fromSource(source, WatermarkStrategy.noWatermarks(), "kafka");

        DataStream<Tuple2<String, Double>> sumCostAggregatorStream = kafka.keyBy(myEvent -> myEvent.category)
                                                                        .window(TumblingProcessingTimeWindows.of(Time.seconds(60)))
                                                                        .aggregate(new CostAggregator());

        sumCostAggregatorStream.print();

        env.execute("Kafka-flink-postres");
    }

    // taking 3 params: order, previous calculated sum of category
    public static class CostAggregator implements AggregateFunction<Order, Tuple2<String, Double>, Tuple2<String, Double>> {
        @Override
        public Tuple2<String, Double> createAccumulator() {
            return new Tuple2<>("", 0.0);
        }

        // accumulator is the previous calculated sum in the window
        @Override
        public Tuple2<String, Double> add(Order event, Tuple2<String, Double> accumulator) {
            return new Tuple2<>(event.category, accumulator.f1 + event.cost); // f0: category, f1: cost
        }

        @Override
        public Tuple2<String, Double> getResult(Tuple2<String, Double> accumulator) {
            return accumulator;
        }

        // merge: merge the results from different nodes
        @Override
        public Tuple2<String, Double> merge(Tuple2<String, Double> a, Tuple2<String, Double> b) {
            return new Tuple2<>(a.f0, a.f1 + b.f1);
        }
    }
}