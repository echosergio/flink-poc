package flink.poc;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.api.windowing.windows.Window;
import org.apache.flink.util.Collector;

public class Spamazon {

    public static void main(String[] args) throws Exception {

        // set up the execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<Tuple4<Integer, String, Double, String>> items = env.fromElements(
                new Tuple4<>(1516635369, "1", 1.2, "Madrid"),
                new Tuple4<>(1516635370, "2", 3.2, "Barcelona"),
                new Tuple4<>(1516635371, "3", 4.6, "Pontevedra"),
                new Tuple4<>(1516635372, "4", 1.7, "Barcelona"),
                new Tuple4<>(1516635374, "1", 1.2, "Madrid")
        ).setParallelism(1);

        SingleOutputStreamOperator<Tuple3<Integer, String, Integer>> output = items
                .filter(new FilterFunction<Tuple4<Integer, String, Double, String>>() {
                    @Override
                    public boolean filter(Tuple4<Integer, String, Double, String> value) throws Exception {
                        return value.f3.equals("Madrid");
                    }
                })
                .keyBy(1)
                .timeWindow(Time.seconds(9600), Time.seconds(600))
                .apply(new WindowFunction<Tuple4<Integer, String, Double, String>, Tuple3<Integer, String, Integer>, Tuple, TimeWindow>() {
                    public void apply (Tuple tuple,
                                       TimeWindow window,
                                       Iterable<Tuple4<Integer, String, Double, String>> values,
                                       Collector<Tuple3<Integer, String, Integer>> out) throws Exception {

                        int sales = 0;
                        int time = 0;
                        String id = "";

                        for (Tuple4<Integer, String, Double, String> value : values) {
                            sales++;
                            time = value.f0;
                            id = value.f1;
                        }

                        out.collect(new Tuple3<>(time, id, sales));
                    }
                });

        output.print();
        env.execute();

    }
}
