package compaculation.mgmt;

import java.util.Collection;
import java.util.List;

import org.apache.accumulo.core.client.admin.compaction.CompactableFile;
import org.apache.accumulo.core.spi.compaction.CompactionJob;
import org.apache.accumulo.core.spi.compaction.CompactionPlan;

/**
 * The represents the interface I would eventually like to have in Accumulo for making tablet
 * compaction decision.
 */
public interface CompaculationPlanner {
  CompactionPlan makePlan(Collection<CompactableFile> allFiles,
      Collection<CompactableFile> candidates, List<CompactionJob> running);
}
