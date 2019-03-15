package be.patrickhancke.distributedlog;

@FunctionalInterface
public interface TransactionIdCallback {
    void markProcessed(long transactionId);
}
