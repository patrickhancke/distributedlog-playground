package be.patrickhancke.distributedlog;

import io.vavr.control.Try;
import org.apache.distributedlog.DistributedLogConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

public class TailingReader {
    private static final Logger log;

    static {
        System.setProperty("logback.configurationFile", "logback-reader.xml");
        log = LoggerFactory.getLogger(TailingReader.class);
    }

    public static void main(String[] args) {
        DistributedLogConfiguration dlogConfiguration = new DistributedLogConfiguration()
                .setLockTimeout(-1)
                //.setImmediateFlushEnabled(false)
                //.setPeriodicFlushFrequencyMilliSeconds(2)
                .setOutputBufferSize(256 * 1024)
                .setReadAheadMaxRecords(10000)
                .setReadAheadBatchSize(10)
                .setAlertWhenPositioningOnTruncated(true)
                .setCreateStreamIfNotExists(true)
                //.setLogSegmentRollingConcurrency(10)
                //.setLogSegmentRollingIntervalMinutes(5)
                ;
        //dlogConfiguration.setThrowExceptionOnMissing(true);
        log.info("created {}", dlogConfiguration);

        Try<DLogManager> dLogManagerTry = DLogManager.create(URI.create(Settings.DLog.URI), dlogConfiguration, 20);
        dLogManagerTry
                .onSuccess(dLogManager -> {
                    Future<?> tailLogCompletion = dLogManager.tailLog(Settings.DLog.logName(), 0L,
                            (transactionId, payload) -> log.info("handling txid {} with payload {}", transactionId, byteArrayToString(payload)),
                            transactionId -> log.info("marking log record with txid {} as processed", transactionId));
                    try {
                        log.info("blocking until {} is completed", tailLogCompletion);
                        tailLogCompletion.get();
                        log.info("{} has completed", tailLogCompletion);
                    } catch (InterruptedException | ExecutionException e) {
                        log.error("error completing {}", tailLogCompletion);
                    }
                });
    }

    private static String byteArrayToString(byte[] payload) {
        return new String(payload, StandardCharsets.UTF_8);
    }
}
