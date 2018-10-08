package HBaseIA.TwitBase.coprocessors;

import org.apache.hadoop.hbase.ipc.CoprocessorProtocol;

import java.io.IOException;

public interface RelationCountProtocol extends CoprocessorProtocol {
    public long followedByCount(String userId) throws IOException;
}
