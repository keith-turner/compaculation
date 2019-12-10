package compaculation.mgmt;

import static java.util.stream.Collectors.toSet;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.OptionalLong;
import java.util.Set;

import compaculation.mgmt.SubmittedJob.Status;

public class TieredCompactionManager implements CompactionManager {

  private double cRatio;

  public TieredCompactionManager(double compactionRatio) {
    this.cRatio = compactionRatio;
  }

  // see https://gist.github.com/keith-turner/16125790c6ff0d86c67795a08d2c057f
  public static Set<String> findMapFilesToCompact(Map<String,Long> files, double ratio) {
    if (files.size() <= 1)
      return Collections.emptySet();

    List<Entry<String,Long>> sortedFiles = new ArrayList<>(files.entrySet());

    // sort from smallest file to largest
    Collections.sort(sortedFiles,
        Comparator.comparingLong(Entry<String,Long>::getValue).thenComparing(Entry::getKey));

    // index into sortedFiles, everything at and below this index is a good set of files to compact
    int goodIndex = -1;

    long sum = sortedFiles.get(0).getValue();

    for (int c = 1; c < sortedFiles.size(); c++) {
      long currSize = sortedFiles.get(c).getValue();
      sum += currSize;

      if (currSize * ratio < sum) {
        goodIndex = c;
      } else if (goodIndex > 0 && sum < 2 * currSize) {
        break;
      }
    }

    if (goodIndex == -1)
      return Collections.emptySet();

    return sortedFiles.subList(0, goodIndex + 1).stream().map(Entry<String,Long>::getKey)
        .collect(toSet());
  }

  //TODO test
  public static Map<String,Long> getSmallestFilesWithSumLessThan(Map<String,Long> files,
      long cutoff) {
    List<Entry<String,Long>> sortedFiles = new ArrayList<>(files.entrySet());

    // sort from smallest file to largest
    Collections.sort(sortedFiles,
        Comparator.comparingLong(Entry<String,Long>::getValue).thenComparing(Entry::getKey));

    HashMap<String,Long> ret = new HashMap<>();

    long sum = 0;

    for (int index = 0; index < sortedFiles.size(); index++) {
      var e = sortedFiles.get(index);
      sum += e.getValue();

      if (sum < cutoff) {
        ret.put(e.getKey(), e.getValue());
      } else {
        break;
      }
    }

    return ret;
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
    OptionalLong minCompacting = submitted.stream().filter(sj -> sj.getStatus() == Status.RUNNING)
        .flatMap(sj -> sj.getFiles().stream()).mapToLong(files::get).min();

    if (minCompacting.isPresent()) {
      // TODO explain why
      filesCopy = getSmallestFilesWithSumLessThan(filesCopy, minCompacting.getAsLong());
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
