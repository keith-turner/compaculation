package compaculation.mgmt;

import static java.util.stream.Collectors.toSet;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.OptionalLong;
import java.util.Set;

import compaculation.mgmt.SubmittedJob.Status;
import compaculation.ratio.DefaultCompactionStrategy.SizeWindow;

public class TieredCompactionManager implements CompactionManager {

  private double cRatio;

  public TieredCompactionManager(double compactionRatio) {
    this.cRatio = compactionRatio;
  }

  public static Set<String> findMapFilesToCompact(Map<String,Long> files, double ratio) {

    if (files.size() <= 1)
      return Collections.emptySet();

    SizeWindow window = new SizeWindow(files).tail(2);

    SizeWindow goodWindow = null;

    do {
      if (window.topSize() * ratio <= window.sum()) {
        goodWindow = window.clone();
      } else if (goodWindow != null) {
        // a smaller window matched the CR, so use that
        break;
      }
    } while (window.slideTop());

    if (goodWindow == null) {
      return Collections.emptySet();
    }

    return goodWindow.getFiles();
  }

  String getExecutor(long size) {
    long meg = 1000000;

    if (size < 10 * meg) {
      return "small";
    } else if (size < 100 * meg) {
      return "medium";
    } else if (size < 500 * meg) {
      return "large";
    } else {
      return "huge";
    }
  }

  @Override
  public CompactionPlan makePlan(Map<String,Long> files, List<SubmittedJob> submitted) {

    // TODO only create if needed in an elegant way.
    Map<String,Long> filesCopy = new HashMap<>(files);

    // find minimum file size for running compactions
    OptionalLong maxCompacting = submitted.stream().filter(sj -> sj.getStatus() == Status.RUNNING)
        .flatMap(sj -> sj.getFiles().stream()).mapToLong(files::get).max();

    if (maxCompacting.isPresent()) {
      filesCopy.values().removeIf(size -> size >= maxCompacting.getAsLong());
    }

    submitted.stream().filter(sj -> sj.getStatus() == Status.RUNNING)
        .flatMap(sj -> sj.getFiles().stream()).forEach(filesCopy::remove);

    Set<String> group = findMapFilesToCompact(filesCopy, cRatio);

    List<Long> cancellations = new ArrayList<>();

    if (group.size() > 0) {
      Set<String> queued = submitted.stream().filter(sj -> sj.getStatus() == Status.QUEUED)
          .flatMap(sj -> sj.getFiles().stream()).collect(toSet());

      if (!queued.isEmpty()) {
        if (queued.equals(group)) {
          // currently queued compactions are fine
          return new CompactionPlan();
        } else {
          submitted.stream().map(sj -> sj.getId()).forEach(cancellations::add);
        }
      }

      String executor = getExecutor(group.stream().mapToLong(files::get).sum());

      Job job = new Job(files.size(), group, executor);

      return new CompactionPlan(List.of(job), cancellations);

    }

    return new CompactionPlan();

  }
}
