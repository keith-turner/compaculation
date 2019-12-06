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

  // see https://gist.github.com/keith-turner/16125790c6ff0d86c67795a08d2c057f
  public static Set<String> findMapFilesToCompact(Map<String,Long> files, double ratio) {

    if (files.size() <= 1)
      return Collections.emptySet();

    // create a window with the smallest two files
    SizeWindow window = new SizeWindow(files).tail(2);

    SizeWindow goodWindow = null;

    do {
      if (window.topSize() * ratio <= window.sum()) {
        // this window matched the compaction ratio, so keep it
        goodWindow = window.clone();
      } else if (goodWindow != null) {
        // for this case a previous set of files matched the CR, however we just saw a much larger
        // file that caused us to no longer match the CR.. so use the previous set.
        break;
      }
      // the call slideTopUp() will move the top of the window up adding the next smallest file to
      // the window.
    } while (window.slideTopUp());

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

    // find maximum file size for running compactions
    OptionalLong maxCompacting = submitted.stream().filter(sj -> sj.getStatus() == Status.RUNNING)
        .flatMap(sj -> sj.getFiles().stream()).mapToLong(files::get).max();

    if (maxCompacting.isPresent()) {
      // remove any files from consideration that are larger than the max file size compacting....
      // TODO explain why
      filesCopy.values().removeIf(size -> size >= maxCompacting.getAsLong());
    }

    // remove any files from consideration that are in use by a running compaction
    submitted.stream().filter(sj -> sj.getStatus() == Status.RUNNING)
        .flatMap(sj -> sj.getFiles().stream()).forEach(filesCopy::remove);

    Set<String> group = findMapFilesToCompact(filesCopy, cRatio);

    List<Long> cancellations = new ArrayList<>();

    if (group.size() > 0) {
      // find all files related to queued jobs
      Set<String> queued = submitted.stream().filter(sj -> sj.getStatus() == Status.QUEUED)
          .flatMap(sj -> sj.getFiles().stream()).collect(toSet());

      if (!queued.isEmpty()) {
        if (queued.equals(group)) {
          // currently queued jobs is the same set of files we want to compact, so just use that
          return new CompactionPlan();
        } else {
          // currently queued jobs are different than what we want to compact, so cancel them
          submitted.stream().map(sj -> sj.getId()).forEach(cancellations::add);
        }
      }

      // TODO do we want to queue a job to an executor if we already have something running there??
      // determine which executor to use based on the size of the files
      String executor = getExecutor(group.stream().mapToLong(files::get).sum());

      Job job = new Job(files.size(), group, executor);

      return new CompactionPlan(List.of(job), cancellations);

    }

    return new CompactionPlan();

  }
}
