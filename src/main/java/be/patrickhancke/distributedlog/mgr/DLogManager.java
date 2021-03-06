package be.patrickhancke.distributedlog.mgr;

import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import io.vavr.control.Try;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.stats.NullStatsLogger;
import org.apache.distributedlog.DLSN;
import org.apache.distributedlog.DistributedLogConfiguration;
import org.apache.distributedlog.LogRecord;
import org.apache.distributedlog.LogRecordWithDLSN;
import org.apache.distributedlog.api.AsyncLogReader;
import org.apache.distributedlog.api.AsyncLogWriter;
import org.apache.distributedlog.api.DistributedLogManager;
import org.apache.distributedlog.api.namespace.Namespace;
import org.apache.distributedlog.api.namespace.NamespaceBuilder;
import org.apache.distributedlog.exceptions.LogNotFoundException;

import java.io.Closeable;
import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.*;

@Slf4j
public class DLogManager implements Closeable {
    private static final long INITIAL_TXID = 0L;
    private final Namespace namespace;
    private final Map<String, DistributedLogManager> logManagersPerLogname;
    private final Map<String, AsyncLogWriterWithTxid> logWritersPerLogname;
    private final ExecutorService readers;

    private DLogManager(Namespace namespace, int maxNumberOfReaders) {
        this.namespace = namespace;
        this.logManagersPerLogname = new HashMap<>();
        this.logWritersPerLogname = new HashMap<>();
        if (maxNumberOfReaders > 0) {
            this.readers = Executors.newFixedThreadPool(maxNumberOfReaders, readerThreadFactory());
        } else {
            log.info("reader thread pool disabled");
            this.readers = null;
        }
    }

    private static ThreadFactory readerThreadFactory() {
        return new ThreadFactoryBuilder()
                .setNameFormat("dlog-reader-%d")
                .setDaemon(true)
                .setUncaughtExceptionHandler((thread, throwable) -> log.error("error while running thread {}", thread, throwable))
                .build();
    }

    public static Try<DLogManager> create(URI uri, DistributedLogConfiguration configuration, int maxNumberOfReaders) {
        return Try.of(() -> {
            log.info("constructing namespace for URI {} with configuration {}", uri, configuration.getPropsAsString());
            return new DLogManager(NamespaceBuilder.newBuilder()
                    .uri(uri)
                    .conf(configuration)
                    .statsLogger(NullStatsLogger.INSTANCE)
                    .clientId("subcen")
                    .build(), maxNumberOfReaders);
        });
    }

    public static Try<DLogManager> create(URI uri, DistributedLogConfiguration dlogConfiguration) {
        return create(uri, dlogConfiguration, -1);
    }

    private static void safeClose(AsyncLogWriter logWriter) {
        try {
            log.info("closing {}", logWriter);
            logWriter.asyncClose().get();
            log.info("closed {}", logWriter);
        } catch (Exception e) {
            log.error("error closing {}", logWriter, e);
        }
    }

    private static void safeClose(AutoCloseable closeable) {
        try {
            log.info("closing {}", closeable);
            closeable.close();
            log.info("closed {}", closeable);
        } catch (Exception e) {
            log.error("error closing {}", closeable, e);
        }
    }


    public Try<DLSN> writeRecord(String logName, byte[] payload) {
        log.info("writing record to log {}", logName);
        return Try.of(() -> {
            Try<AsyncLogWriterWithTxid> writerTry = getWriterForLogname(logName);
            if (writerTry.isSuccess()) {
                DLSN dlsn = writerTry.get().write(payload).get();
                log.info("written record to log {}, DLSN={}", logName, dlsn);
                return dlsn;
            } else {
                log.error("failed to get a writer for log name {}", logName, writerTry.getCause());
                throw writerTry.getCause();
            }
        });
    }

    private Try<AsyncLogReader> openReader(String logName, long fromTxid) {
        return Try.of(() -> {
            Try<DistributedLogManager> logManagerTry = getLogManagerForLogname(logName);
            if (logManagerTry.isSuccess()) {
                log.info("opening log reader for log {} starting from txid {}", logName, fromTxid);
                return logManagerTry.get().openAsyncLogReader(fromTxid).get();
            } else {
                log.error("error getting DLM for log name {}", logName);
                throw logManagerTry.getCause();
            }
        });
    }

    public Future<?> tailLog(String logName, long fromTxid, LogRecordCallback logRecordCallback, TransactionIdCallback transactionIdCallback) {
        Preconditions.checkNotNull(this.readers, String.format("%s has not been configured with a reader thread pool", this));
        Try<AsyncLogReader> readerTry = openReader(logName, fromTxid);
        if (readerTry.isSuccess()) {
            AsyncLogReader logReader = readerTry.get();
            return this.readers.submit(() -> {
                boolean running = true;
                ReaderStatistics readerStatistics = new ReaderStatistics(logName, fromTxid);
                log.info("start reading log {} from txid {}", logName, fromTxid);
                while (running) {
                    try {
                        log.debug("trying to read next entry");
                        LogRecordWithDLSN logRecordWithDLSN = logReader.readNext().get();
                        long transactionId = logRecordWithDLSN.getTransactionId();
                        log.debug("read {} with txid {}, calling {}", logRecordWithDLSN, transactionId, logRecordCallback);
                        logRecordCallback.handle(transactionId, logRecordWithDLSN.getPayload());
                        log.debug("called {}, now calling {}", logRecordCallback, transactionIdCallback);
                        readerStatistics.markRead(transactionId);
                        transactionIdCallback.markProcessed(transactionId, readerStatistics);
                        log.debug("called {}", transactionIdCallback);
                    } catch (InterruptedException | ExecutionException | IOException e) {
                        log.error("error reading next entry", e);
                        running = false;
                    }
                }
                log.info("finished reading");
                log.info("closing reader {}", logReader);
                try {
                    logReader.asyncClose().get();
                } catch (InterruptedException | ExecutionException e) {
                    log.error("error closing reader {}", logReader, e);
                }
                log.info("closed reader {}", logReader);
            });
        } else {
            throw new RuntimeException(readerTry.getCause());
        }
    }

    private Try<AsyncLogWriterWithTxid> getWriterForLogname(String logName) {
        log.debug("getting writer for log name {}", logName);
        if (this.logWritersPerLogname.containsKey(logName)) {
            return Try.of(() -> {
                AsyncLogWriterWithTxid logWriter = logWritersPerLogname.get(logName);
                log.debug("returning cached log writer {} for log name {}", logWriter, logName);
                return logWriter;
            });
        } else {
            return Try.of(() -> {
                Try<DistributedLogManager> logManagerTry = getLogManagerForLogname(logName);
                if (logManagerTry.isSuccess()) {
                    DistributedLogManager logManager = logManagerTry.get();
                    Try<AsyncLogWriter> logWriterTry = Try.of(() -> logManager.openAsyncLogWriter().get())
                            .recoverWith(LogNotFoundException.class, Try.of(() -> {
                                log.info("log name {} doesn't exist yet, creating it", logName);
                                namespace.createLog(logName);
                                log.info("created log name {}", logName);
                                return logManager.openAsyncLogWriter().get();
                            }));
                    if (logWriterTry.isSuccess()) {
                        long lastTxId = Try.of(logManager::getLastTxId).getOrElse(INITIAL_TXID);
                        AsyncLogWriterWithTxid writerWithTxid = new AsyncLogWriterWithTxid(logWriterTry.get(), lastTxId);
                        log.info("returning {} for log name {}", writerWithTxid, logName);
                        logWritersPerLogname.put(logName, writerWithTxid);
                        return writerWithTxid;
                    } else {
                        log.error("failed to get a log writer for log name {}", logName, logWriterTry.getCause());
                        throw logWriterTry.getCause();
                    }
                } else {
                    log.error("failed to get a DLM for log name {}", logName, logManagerTry.getCause());
                    throw logManagerTry.getCause();
                }
            });
        }
    }

    private Try<DistributedLogManager> getLogManagerForLogname(String logName) {
        if (!logManagersPerLogname.containsKey(logName)) {
            log.info("no DLM yet for log name {}, creating it", logName);
            Try<DistributedLogManager> logManagerTry = Try.of(() -> namespace.openLog(logName));
            logManagerTry
                    .onSuccess(distributedLogManager -> logManagersPerLogname.put(logName, distributedLogManager))
                    .onFailure(throwable -> log.error("failed to create DLM for log name {}", logName, throwable));
            return logManagerTry;
        }
        return Try.of(() -> {
            DistributedLogManager logManager = logManagersPerLogname.get(logName);
            Preconditions.checkArgument(logManager != null, "missing log manager for log name " + logName);
            log.debug("returning DLM {}", logManager);
            return logManager;
        });
    }

    @Override
    public void close() {
        for (DistributedLogManager logManager : logManagersPerLogname.values()) {
            safeClose(logManager);
        }
        for (AsyncLogWriterWithTxid logWriter : logWritersPerLogname.values()) {
            safeClose(logWriter);
        }
        safeClose(namespace);
        if (readers != null) {
            close(readers);
        }
    }

    private void close(ExecutorService executor) {
        log.info("shutting down {}", executor);
        try {
            executor.shutdown();
            try {
                if (!executor.awaitTermination(10, TimeUnit.SECONDS)) {
                    log.warn("{} failed to terminate properly", executor);
                    executor.shutdownNow();
                    if (!executor.awaitTermination(10, TimeUnit.SECONDS)) {
                        log.error("{} did not terminate", executor);
                    }
                }
            } catch (InterruptedException ie) {
                log.warn("interrupted while waiting for termination", ie);
                executor.shutdownNow();
                Thread.currentThread().interrupt();
            }
            log.info("shut down {}", executor);
        } catch (RuntimeException e) {
            log.error("failed to shutdown {}", executor);
        }
    }

    @ToString
    private class AsyncLogWriterWithTxid extends AbstractLogWriterWithTxid {
        private final AsyncLogWriter logWriter;

        private AsyncLogWriterWithTxid(AsyncLogWriter logWriter, long lastTxid) {
            super(lastTxid);
            this.logWriter = logWriter;
        }

        @Override
        public void close() {
            safeClose(this.logWriter);
        }

        @Override
        CompletableFuture<DLSN> write(long txid, byte[] payload) {
            return logWriter.write(new LogRecord(txid, payload));
        }
    }
}

