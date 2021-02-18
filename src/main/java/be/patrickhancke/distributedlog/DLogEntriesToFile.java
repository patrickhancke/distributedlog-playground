package be.patrickhancke.distributedlog;

import be.patrickhancke.distributedlog.mgr.DLogManager;
import io.vavr.control.Try;
import org.apache.commons.io.IOUtils;
import org.apache.distributedlog.DistributedLogConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

public class DLogEntriesToFile {
    private static final Logger log;
    private static final String NAME_SPACE = "subcen-tsw";
    private static final String LOG_NAME = "toyota-tsw-sample-ss1";
    private static final FileOutputStream FILE_OUTPUT_STREAM;

    static {
        System.setProperty("logback.configurationFile", "logback-reader.xml");
        log = LoggerFactory.getLogger(TailingReader.class);
        File outputFile = new File("c:/users/ph/Downloads", fileName(NAME_SPACE, LOG_NAME));
        try {
            outputFile.createNewFile();
            log.info("created file {}", outputFile);
            FILE_OUTPUT_STREAM = new FileOutputStream(outputFile);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static void main(String[] args) {
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            log.info("shutting down");
            try {
                FILE_OUTPUT_STREAM.flush();
                FILE_OUTPUT_STREAM.close();
            } catch (IOException e) {
                log.error("error closing file", e);
            }
            log.info("shut down");
        }));
        DistributedLogConfiguration dlogConfiguration = new DistributedLogConfiguration()
                .setLockTimeout(-1)
                .setOutputBufferSize(256 * 1024)
                .setReadAheadMaxRecords(10000)
                .setReadAheadBatchSize(10)
                .setAlertWhenPositioningOnTruncated(true)
                .setCreateStreamIfNotExists(true);
        log.info("created {}", dlogConfiguration);
        Try<DLogManager> dLogManagerTry = DLogManager.create(URI.create(String.format("distributedlog://localhost:2181/%s", NAME_SPACE)), dlogConfiguration, 1);
        dLogManagerTry
                .onSuccess(dLogManager -> {
                    log.info("start reading from log {}", LOG_NAME);
                    Future<?> tailLogCompletion = dLogManager.tailLog(LOG_NAME, 0L,
                            (transactionId, payload) -> {
                                String entry = byteArrayToString(payload);
                                Try.run(() -> IOUtils.write(entry + "\n", FILE_OUTPUT_STREAM)).onFailure(throwable -> log.error("failed to write to file", throwable));
                                log.info("handled txid {} from log {} with payload {}", transactionId, LOG_NAME, entry);
                            },
                            (transactionId, readerStatistics) -> log.info("processed txid {} from log {}: {}", transactionId, LOG_NAME, readerStatistics));
                    log.info("created future {}", futureToString(tailLogCompletion));
                    try {
                        tailLogCompletion.get();
                    } catch (InterruptedException | ExecutionException e) {
                        log.error("interrupted while waiting for completion", e);
                    }
                });
    }

    private static String fileName(String nameSpace, String logName) {
        return String.format("dlog-%s-%s-%s", nameSpace, logName, LocalDateTime.now().format(DateTimeFormatter.ofPattern("YYYYMMdd-HHmmss")));
    }

    private static String futureToString(Future<?> future) {
        return String.format("%s: cancelled=%s, done=%s", future, future.isCancelled(), future.isDone());
    }

    private static String byteArrayToString(byte[] payload) {
        return new String(payload, StandardCharsets.UTF_8);
    }
}
