package bs;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import static java.util.UUID.randomUUID;

@Slf4j
public class BroadcastSource implements SourceFunction<String> {
    private volatile boolean isRunning = true;

    @Override
    public void run(SourceContext<String> sourceContext) throws Exception {
        log.info("Running BroadcastSource");
        while (isRunning) {
            String event = "BroadcastEvent " + randomUUID();
            sourceContext.collect(event);
            log.info("Issuing broadcast event {}", event);
            Thread.sleep(1000);
        }
    }

    @Override
    public void cancel() {
        log.info("Cancelling BroadcastSource");
        isRunning = false;
    }
}