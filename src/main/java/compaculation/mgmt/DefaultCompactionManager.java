package compaculation.mgmt;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

import compaculation.mgmt.SubmittedJob.Status;
import compaculation.ratio.DefaultCompactionStrategy;

// Attempt to simulate accumulo's current default compaction behavior
public class DefaultCompactionManager implements CompaculationPlanner {

  private double ratio;

  public DefaultCompactionManager(double compactionRatio) {
    this.ratio = compactionRatio;
  }

  public static Set<String> findMapFilesToCompact(Map<String,Long> files, double ratio) {
    var group = DefaultCompactionStrategy.findMapFilesToCompact(files, ratio, 10, 15);
    if (group == null)
      return Collections.emptySet();

    return group;
  }

  @Override
  public CompactionPlan makePlan(Map<String,Long> files, List<SubmittedJob> submitted) {
    if (submitted.size() > 1)
      throw new IllegalArgumentException();

    if (submitted.stream().anyMatch(sj -> sj.getStatus() == Status.RUNNING)) {
      return new CompactionPlan();
    }

    Set<String> group = findMapFilesToCompact(files, ratio);

    if (group.isEmpty()) {
      return new CompactionPlan();
    }

    List<Long> cancellations = List.of();

    if (!submitted.isEmpty()) {
      if (!submitted.get(0).getFiles().equals(group)) {
        cancellations = List.of(submitted.get(0).getId());
      } else {
        return new CompactionPlan();
      }
    }

    return new CompactionPlan(List.of(new Job(files.size(), group, "huge")), cancellations);
  }

}
