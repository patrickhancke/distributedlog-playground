package be.patrickhancke.distributedlog;

import io.vavr.control.Try;
import org.apache.distributedlog.DLSN;
import org.apache.distributedlog.DistributedLogConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.time.ZonedDateTime;
import java.util.concurrent.TimeUnit;

public class SingleWriter {
    private static final Logger log;
    private static final int MAX_NUMBER_ITERATIONS = 20;

    static {
        System.setProperty("logback.configurationFile", "logback-writer.xml");
        log = LoggerFactory.getLogger(SingleWriter.class);
    }

    public static void main(String[] args) {
        DistributedLogConfiguration dlogConfiguration = new DistributedLogConfiguration()
                .setLockTimeout(-1)
                .setImmediateFlushEnabled(false)
                .setPeriodicFlushFrequencyMilliSeconds(2)
                .setOutputBufferSize(256 * 1024)
                .setReadAheadMaxRecords(10000)
                .setReadAheadBatchSize(10)
                .setAlertWhenPositioningOnTruncated(true)
                .setCreateStreamIfNotExists(true)
                .setLogSegmentRollingIntervalMinutes(1);
        dlogConfiguration.setThrowExceptionOnMissing(true);
        log.info("created {}", dlogConfiguration);

        Try<DLogManager> dLogManagerTry = DLogManager.create(URI.create(Settings.DLog.URI), dlogConfiguration, 20);
        dLogManagerTry
                .onSuccess(dLogManager -> {
                    try {
                        int currentIteration = 0;
                        while (currentIteration <= MAX_NUMBER_ITERATIONS) {
                            Try<DLSN> dlsnTry = dLogManager.writeRecord(Settings.DLog.logName(), payload(currentIteration + " : patrick-test-" + ZonedDateTime.now()));
                            dlsnTry.getOrElseThrow(throwable -> throwable);
                            TimeUnit.SECONDS.sleep(5);
                            currentIteration++;
                        }
                    } catch (Throwable t) {
                        log.error("error", t);
                    } finally {
                        dLogManager.close();
                    }
                })
                .onFailure(throwable -> log.error("error", throwable));
    }

    private static byte[] payload(String message) {
        return message.getBytes(StandardCharsets.UTF_8);
    }
}
