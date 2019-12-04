package compaculation;

import java.util.List;
import java.util.Map;

public interface CompactionManager {
  CompactionPlan makePlan(Map<String,Long> files, List<SubmittedJob> submitted);
}
