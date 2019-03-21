package be.patrickhancke.distributedlog.mgr;

import lombok.ToString;
import lombok.extern.slf4j.Slf4j;
import org.apache.distributedlog.DLSN;

import java.io.Closeable;
import java.util.concurrent.CompletableFuture;

@ToString
@Slf4j
abstract class AbstractLogWriterWithTxid implements Closeable {
    private long txid;

    protected AbstractLogWriterWithTxid(long txid) {
        this.txid = txid;
    }

    abstract CompletableFuture<DLSN> write(long txid, byte[] payload);

    public CompletableFuture<DLSN> write(byte[] payload) {
        log.debug("writing payload with txid {}", txid);
        return write(txid++, payload);
        //return logWriter.write(new LogRecord(txid++, payload));
    }
}
