package be.patrickhancke.distributedlog.mgr;

@FunctionalInterface
public interface LogRecordCallback {
    void handle(long transactionId, byte[] payload);
}
