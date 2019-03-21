package be.patrickhancke.distributedlog.mgr;

import lombok.ToString;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@ToString
public class ReaderStatistics {
    private final String logName;
    private final long firstTxid;
    private long lastReadTxid;
    private int totalNumberRead;

    ReaderStatistics(String logName, long firstTxid) {
        this.logName = logName;
        this.firstTxid = firstTxid;
        this.lastReadTxid = -1;
        this.totalNumberRead = 0;
    }

    void markRead(long transactionId) {
        this.totalNumberRead++;
        log.info("read txid {}, previous txid {}, total {}", transactionId, lastReadTxid, totalNumberRead);
        this.lastReadTxid = transactionId;
    }
}
