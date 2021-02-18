package be.patrickhancke.distributedlog.mgr;

import java.io.IOException;

@FunctionalInterface
public interface LogRecordCallback {
    void handle(long transactionId, byte[] payload) throws IOException;
}
