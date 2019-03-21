package be.patrickhancke.distributedlog.mgr;

@FunctionalInterface
public interface TransactionIdCallback {
    void markProcessed(long transactionId, ReaderStatistics readerStatistics);
}
