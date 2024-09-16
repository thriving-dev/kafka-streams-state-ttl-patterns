package dev.thriving.poc;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.state.RocksDBConfigSetter;
import org.apache.kafka.streams.state.internals.BlockBasedTableConfigWithAccessibleCache;
import org.rocksdb.Options;

import java.util.Map;

@Slf4j
public class RocksConfigSetter implements RocksDBConfigSetter {

    @Override
    public void setConfig(final String storeName, final Options options, final Map<String, Object> configs) {
        BlockBasedTableConfigWithAccessibleCache tableConfig =
                (BlockBasedTableConfigWithAccessibleCache) options.tableFormatConfig();
        options.setTableFormatConfig(tableConfig);

        if (storeName.equals(KStreamsTopologyFactory.STATE_STORE)) {
            // TTL in seconds
            options.setTtl(300000); // 5m
        }
    }

    @Override
    public void close(final String storeName, final Options options) {}
}
