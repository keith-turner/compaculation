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

  @Override
  public CompactionPlan makePlan(Map<String,Long> files, List<SubmittedJob> submitted) {

    // TODO only create if needed in an elegant way.
    Map<String,Long> filesCopy = new HashMap<>(files);

    // find minimum file size for running compactions
    OptionalLong minCompacting = submitted.stream().filter(sj -> sj.getStatus() == Status.RUNNING)
        .flatMap(sj -> sj.getFiles().stream()).mapToLong(files::get).min();

    if (minCompacting.isPresent()) {
      // This is done to ensure that compactions over time result in the mimimum number of files.
      // See the gist for more info.
      filesCopy = getSmallestFilesWithSumLessThan(filesCopy, minCompacting.getAsLong());
    }

    // remove any files from consideration that are in use by a running compaction
    submitted.stream().filter(sj -> sj.getStatus() == Status.RUNNING)
        .flatMap(sj -> sj.getFiles().stream()).forEach(filesCopy::remove);

    Set<String> group = findMapFilesToCompact(filesCopy, cRatio);

    if (group.size() > 0) {
      List<Long> cancellations = new ArrayList<>();

      // find all files related to queued jobs
      Set<String> queued = submitted.stream().filter(sj -> sj.getStatus() == Status.QUEUED)
          .flatMap(sj -> sj.getFiles().stream()).collect(toSet());

      if (!queued.isEmpty()) {
        if (queued.equals(group)) {
          // currently queued jobs is the same set of files we want to compact, so just use that
          return new CompactionPlan();
        } else {
          // currently queued jobs are different than what we want to compact, so cancel them
          submitted.stream().filter(sj -> sj.getStatus() == Status.QUEUED).map(sj -> sj.getId())
              .forEach(cancellations::add);
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

  /**
   * Find the largest set of small files to compact.
   * 
   * <p>
   * See https://gist.github.com/keith-turner/16125790c6ff0d86c67795a08d2c057f
   */
  public static Set<String> findMapFilesToCompact(Map<String,Long> files, double ratio) {
    if (files.size() <= 1)
      return Collections.emptySet();

    // sort files from smallest to largest. So position 0 has the smallest file.
    List<Entry<String,Long>> sortedFiles = sortByFileSize(files);

    // index into sortedFiles, everything at and below this index is a good set of files to compact
    int goodIndex = -1;

    long sum = sortedFiles.get(0).getValue();

    for (int c = 1; c < sortedFiles.size(); c++) {
      long currSize = sortedFiles.get(c).getValue();
      sum += currSize;

      if (currSize * ratio < sum) {
        goodIndex = c;

        // look ahead to the next file to see if this a good stopping point
        if (c + 1 < sortedFiles.size()) {
          long nextSize = sortedFiles.get(c + 1).getValue();
          boolean nextMeetsCR = nextSize * ratio < nextSize + sum;

          if (!nextMeetsCR && sum < nextSize) {
            // These two conditions indicate the largest set of small files to compact was found, so
            // stop looking.
            break;
          }
        }
      }
    }

    if (goodIndex == -1)
      return Collections.emptySet();

    return sortedFiles.subList(0, goodIndex + 1).stream().map(Entry<String,Long>::getKey)
        .collect(toSet());
  }

  public static Map<String,Long> getSmallestFilesWithSumLessThan(Map<String,Long> files,
      long cutoff) {
    List<Entry<String,Long>> sortedFiles = sortByFileSize(files);

    HashMap<String,Long> ret = new HashMap<>();

    long sum = 0;

    for (int index = 0; index < sortedFiles.size(); index++) {
      var e = sortedFiles.get(index);
      sum += e.getValue();

      if (sum < cutoff)
        ret.put(e.getKey(), e.getValue());
      else
        break;
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

  public static List<Entry<String,Long>> sortByFileSize(Map<String,Long> files) {
    List<Entry<String,Long>> sortedFiles = new ArrayList<>(files.entrySet());

    // sort from smallest file to largest
    Collections.sort(sortedFiles,
        Comparator.comparingLong(Entry<String,Long>::getValue).thenComparing(Entry::getKey));

    return sortedFiles;
  }
}
