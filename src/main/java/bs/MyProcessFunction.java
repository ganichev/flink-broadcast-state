package bs;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;

@Slf4j
public class MyProcessFunction extends BroadcastProcessFunction<String, String, String> {

    public static final String STATE_KEY = "StateKey";
    private MapStateDescriptor<String, String> broadcastStateDescriptor = new MapStateDescriptor<>(
            "RulesBroadcastState",
            BasicTypeInfo.STRING_TYPE_INFO,
            BasicTypeInfo.STRING_TYPE_INFO
    );

    @Override
    public void processElement(String s, BroadcastProcessFunction<String, String, String>.ReadOnlyContext readOnlyContext, Collector<String> collector) throws Exception {
        String rules = readOnlyContext.getBroadcastState(broadcastStateDescriptor).get(STATE_KEY);
        log.info("Processing event {} for rules {}", s, rules);
        collector.collect(s);
    }

    @Override
    public void processBroadcastElement(String s, BroadcastProcessFunction<String, String, String>.Context context, Collector<String> collector) throws Exception {
        log.info("Processing broadcast event {}", s);
        context.getBroadcastState(broadcastStateDescriptor).put(STATE_KEY, s);
    }

    public MapStateDescriptor<String, String> getBroadcastStateDescriptor() {
        return broadcastStateDescriptor;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
    }
}
