package be.patrickhancke.distributedlog;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import io.vavr.control.Try;
import org.apache.distributedlog.DLSN;
import org.apache.distributedlog.DistributedLogConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.time.ZonedDateTime;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

public class SingleWriter {
    private static final Logger log;
    private static final int MAX_NUMBER_ITERATIONS = 200;
    private static final int SECONDS_TO_SLEEP_BETWEEN_WRITES = 5;
    private static final int BUFFER = 60;

    static {
        System.setProperty("logback.configurationFile", "logback-writer.xml");
        log = LoggerFactory.getLogger(SingleWriter.class);
    }

    public static void main(String[] args) throws InterruptedException {
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

        int numberOfWriters = Settings.DLog.NUMBER_OF_LOGS;
        ExecutorService executorService = Executors.newFixedThreadPool(numberOfWriters, threadFactory());

        Try<DLogManager> dLogManagerTry = DLogManager.create(URI.create(Settings.DLog.URI), dlogConfiguration, 20);
        dLogManagerTry
                .onSuccess(dLogManager -> {
                    for (int i = 0; i < numberOfWriters; i++) {
                        int logSequenceNumber = i;
                        executorService.submit(() -> {
                            try {
                                int currentIteration = 0;
                                while (currentIteration <= MAX_NUMBER_ITERATIONS) {
                                    Try<DLSN> dlsnTry = dLogManager.writeRecord(Settings.DLog.logName(logSequenceNumber), payload(currentIteration + " : patrick-test-" + ZonedDateTime.now()));
                                    dlsnTry.getOrElseThrow(throwable -> throwable);
                                    TimeUnit.SECONDS.sleep(SECONDS_TO_SLEEP_BETWEEN_WRITES);
                                    currentIteration++;
                                }
                            } catch (Throwable t) {
                                log.error("error", t);
                            } finally {
                                dLogManager.close();
                            }
                        });
                    }
                })
                .onFailure(throwable -> log.error("error", throwable));
        executorService.shutdown();
        log.info("all threads launched, waiting for termination...");
        boolean terminated = executorService.awaitTermination(secondsToWaitForTermination(), TimeUnit.SECONDS);
        log.info("termination return state: {}", terminated);
    }

    private static int secondsToWaitForTermination() {
        return BUFFER + MAX_NUMBER_ITERATIONS * SECONDS_TO_SLEEP_BETWEEN_WRITES;
    }

    private static ThreadFactory threadFactory() {
        return new ThreadFactoryBuilder()
                .setNameFormat("dlog-writer-%d")
                .setUncaughtExceptionHandler((thread, throwable) -> log.error("uncaught exception on thread {}", thread, throwable))
                .build();
    }

    private static byte[] payload(String message) {
        return message.getBytes(StandardCharsets.UTF_8);
    }
}
