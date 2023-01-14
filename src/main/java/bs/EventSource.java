package bs;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import static java.util.UUID.randomUUID;

@Slf4j
public class EventSource implements SourceFunction<String> {
    private volatile boolean isRunning = true;

    @Override
    public void run(SourceContext<String> sourceContext) throws Exception {
        log.info("Running EventSource");
        while(isRunning) {
            String event = "Event " + randomUUID();
            log.info("Issuing {}", event);
            sourceContext.collect(event);
            Thread.sleep(500);
        }
    }

    @Override
    public void cancel() {
        log.info("Cancelling EventSource");
        isRunning = false;
    }
}
