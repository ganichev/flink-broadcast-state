package bs;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

@Slf4j
public class StdOutSink implements SinkFunction<String> {
    @Override
    public void invoke(String value, Context context) throws Exception {
        log.info("Sinking {}", value);
    }
}
