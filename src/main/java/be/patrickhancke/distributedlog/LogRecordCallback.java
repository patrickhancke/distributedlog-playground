package be.patrickhancke.distributedlog;

@FunctionalInterface
public interface LogRecordCallback {
    void handle(long transactionId, byte[] payload);
}
