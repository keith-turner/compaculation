package compaculation.mgmt;

import java.util.Collection;
import java.util.List;

import org.apache.accumulo.core.client.admin.compaction.CompactableFile;
import org.apache.accumulo.core.spi.compaction.CompactionJob;
import org.apache.accumulo.core.spi.compaction.CompactionPlan;

import compaculation.ExecutorConfig;

public interface CompaculationPlanner {
  CompactionPlan makePlan(Collection<CompactableFile> allFiles,
      Collection<CompactableFile> candidates, List<CompactionJob> running);

  Collection<ExecutorConfig> getExecutorConfig();
}
