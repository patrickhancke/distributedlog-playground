package be.patrickhancke.distributedlog;

import be.patrickhancke.distributedlog.mgr.DLogManager;
import io.vavr.control.Try;
import org.apache.distributedlog.DistributedLogConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.Future;

public class TailingReader {
    private static final int NUMBER_OF_READERS_PER_LOG = 3;
    private static final Logger log;

    static {
        System.setProperty("logback.configurationFile", "logback-reader.xml");
        log = LoggerFactory.getLogger(TailingReader.class);
    }

    public static void main(String[] args) {
        DistributedLogConfiguration dlogConfiguration = new DistributedLogConfiguration()
                .setLockTimeout(-1)
                .setOutputBufferSize(256 * 1024)
                .setReadAheadMaxRecords(10000)
                .setReadAheadBatchSize(10)
                .setAlertWhenPositioningOnTruncated(true)
                .setCreateStreamIfNotExists(true);
        log.info("created {}", dlogConfiguration);

        int maxNumberOfReaders = Settings.App.NUMBER_OF_LOGS * NUMBER_OF_READERS_PER_LOG;
        Try<DLogManager> dLogManagerTry = DLogManager.create(URI.create(Settings.DLog.URI), dlogConfiguration, maxNumberOfReaders);
        dLogManagerTry
                .onSuccess(dLogManager -> {
                    for (int j = 0; j < Settings.App.NUMBER_OF_LOGS; j++) {
                        for (int i = 0; i < NUMBER_OF_READERS_PER_LOG; i++) {
                            String logName = Settings.DLog.logName(j);
                            log.info("start reading from log {}", logName);
                            Future<?> tailLogCompletion = dLogManager.tailLog(logName, 0L,
                                    (transactionId, payload) -> log.info("handled txid {} from log {} with payload {}", transactionId, logName, byteArrayToString(payload)),
                                    (transactionId, readerStatistics) -> log.info("processed txid {} from log {}: {}", transactionId, logName, readerStatistics));
                            log.info("created future {}", futureToString(tailLogCompletion));
                        }
                    }
                });
    }

    private static String futureToString(Future<?> future) {
        return String.format("%s: cancelled=%s, done=%s", future, future.isCancelled(), future.isDone());
    }

    private static String byteArrayToString(byte[] payload) {
        return new String(payload, StandardCharsets.UTF_8);
    }
}
