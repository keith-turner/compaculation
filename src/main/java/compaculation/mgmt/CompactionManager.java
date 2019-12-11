package compaculation.mgmt;

import java.util.List;
import java.util.Map;

/**
 * The represents the interface I would eventually like to have in Accumulo for making tablet
 * compaction decision.
 */
public interface CompactionManager {
  CompactionPlan makePlan(Map<String,Long> files, List<SubmittedJob> submitted);
}
