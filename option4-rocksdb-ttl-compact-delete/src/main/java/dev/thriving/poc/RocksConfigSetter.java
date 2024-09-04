package dev.thriving.poc;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.state.RocksDBConfigSetter;
import org.apache.kafka.streams.state.internals.BlockBasedTableConfigWithAccessibleCache;
import org.rocksdb.CompactionStyle;
import org.rocksdb.Options;

import java.util.Map;

@Slf4j
public class RocksConfigSetter implements RocksDBConfigSetter {

    // From RocksDB docs: default is 4kb, but Facebook tends to use 16-32kb in production.
    // With longer keys (we have a lot of long keys), higher block size is recommended. We
    // increase the block size to 32kb here.
    //
    // The danger here is that we don't have a "hard limit" on memory allocated by these
    // blocks. To do that, we would need to investigate creating an LRUCache with a
    // special index filter block ratio, and then do some math. For more context:
    //
    // NOTE: Increasing the Block Size makes the un-managed memory footprint SMALLER because
    // there are fewer blocks to index. However, I think it makes reads a bit slower.
    //
    // Confluent: https://docs.confluent.io/platform/current/streams/developer-guide/memory-mgmt.html
    // Rocksdb: https://github.com/facebook/rocksdb/wiki/Memory-usage-in-RocksDB#indexes-and-filter-blocks
    private static final long BLOCK_SIZE = 1024 * 32;

    // From RocksDB docs: this allows better performance when using jemalloc
    // https://github.com/facebook/rocksdb/wiki/Memory-usage-in-RocksDB#indexes-and-filter-blocks
    private static final boolean OPTIMIZE_FILTERS_FOR_MEMORY = true;

    // Most of the time, when we do a get() we end up finding an object. This config makes RocksDB
    // NOT put a Bloom Filter on the last level of SST files. This reduces memory usage of the
    // Bloom Filter by 90%, but it costs one IO operation on every get() for a missing key.
    // In my opinion, it's a good trade-off.
    private static final boolean OPTIMIZE_FILTERS_FOR_HITS = true;

    @Override
    public void setConfig(final String storeName, final Options options, final Map<String, Object> configs) {
        log.trace("Overriding rocksdb settings for store {}", storeName);

        BlockBasedTableConfigWithAccessibleCache tableConfig =
                (BlockBasedTableConfigWithAccessibleCache) options.tableFormatConfig();

        tableConfig.setOptimizeFiltersForMemory(OPTIMIZE_FILTERS_FOR_MEMORY);
        tableConfig.setBlockSize(BLOCK_SIZE);
        options.setTableFormatConfig(tableConfig);

        options.setOptimizeFiltersForHits(OPTIMIZE_FILTERS_FOR_HITS);
        options.setCompactionStyle(CompactionStyle.LEVEL);

        // Streams default
        options.setMaxWriteBufferNumber(3);

        // TTL
        options.setTtl(60); // 60s
    }

    @Override
    public void close(final String storeName, final Options options) {}

}
