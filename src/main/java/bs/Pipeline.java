package bs;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

@Slf4j
public class Pipeline {
    public static void main(String[] args) throws Exception {
        log.info("Starting...");

        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();

        env.setParallelism(4);

        DataStream<String> stream = env.addSource(new EventSource());

        DataStream<String> broadcastStream = env.addSource(new BroadcastSource());

        MyProcessFunction myProcessFunction = new MyProcessFunction();

        stream
                .connect(broadcastStream.broadcast(myProcessFunction.getBroadcastStateDescriptor()))
                .process(myProcessFunction)
                .addSink(new StdOutSink());

        env.execute("broadcast-state-job");
    }
}
