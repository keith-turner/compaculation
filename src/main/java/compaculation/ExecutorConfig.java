package compaculation;

import org.apache.accumulo.core.spi.compaction.CompactionExecutorId;

public class ExecutorConfig {

  public final CompactionExecutorId id;
  public final int numThreads;

  public ExecutorConfig(CompactionExecutorId id, int numThreads) {
    this.id = id;
    this.numThreads = numThreads;
  }
}
