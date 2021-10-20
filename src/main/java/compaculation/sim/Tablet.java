package compaculation.sim;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.function.LongSupplier;
import java.util.stream.Collectors;

import org.apache.accumulo.core.client.admin.compaction.CompactableFile;
import org.apache.accumulo.core.spi.compaction.CompactionJob;
import org.apache.accumulo.core.spi.compaction.CompactionPlan;

import compaculation.Parameters;
import compaculation.mgmt.Job;

public class Tablet {

  enum Status {
    QUEUED, RUNNING
  }

  private Set<CompactableFile> files = new HashSet<>();
  private Map<CompactionJob,Status> jobs = new HashMap<>();

  // TODO maybe have a server context
  private LongSupplier idSupplier;

  private int tabletId;

  private long rewritten = 0;
  private Function<Long,Long> compactionTicker;

  Tablet(Parameters params, int tabletId, LongSupplier idSupplier) {
    this.tabletId = tabletId;
    this.idSupplier = idSupplier;
    this.compactionTicker = params.compactionTicker;
  }

  private CompactableFile newCFile(String prefix, long size) {
    try {
      String path = String.format("hdfs://fake/accumulo/tables/1/t-%08d/%s%08d.rf", tabletId,
          prefix, idSupplier.getAsLong());

      return CompactableFile.create(new URI(path), size, 0);
    } catch (URISyntaxException e) {
      throw new RuntimeException(e);
    }
  }

  public void addFile(long size) {
    var cFile = newCFile("F", size);
    synchronized (this) {
      files.add(cFile);
    }
  }

  private void compact(CompactionJob job) {

    synchronized (this) {
      if (jobs.put(job, Status.RUNNING) != Status.QUEUED) {
        jobs.remove(job);
        return;
      }
    }

    // bytes per millis
    // long bpm = 5000000;

    long size = job.getFiles().stream().mapToLong(CompactableFile::getEstimatedSize).sum();

    long sleepTime = Math.max(1, compactionTicker.apply(size));
    try {
      Thread.sleep(sleepTime);
    } catch (InterruptedException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }

    var cFile = newCFile("C", size);

    synchronized (this) {
      jobs.remove(job);
      files.removeAll(job.getFiles());
      files.add(cFile);
      rewritten += size;
    }
  }

  public synchronized boolean cancelCompactions(CompactionPlan plan,
      Collection<CompactionJob> prevRunning) {
    if (plan.getJobs().isEmpty() || jobs.keySet().containsAll(plan.getJobs())) {
      return false;
    }

    if (jobs.isEmpty()) {
      return true;
    }

    Set<CompactionJob> running = new HashSet<>();
    Set<CompactableFile> runningFiles = new HashSet<>();

    jobs.forEach((job, status) -> {
      if (status == Status.RUNNING) {
        running.add(job);
        runningFiles.addAll(job.getFiles());
      }
    });

    if (!running.equals(prevRunning)) {
      return false;
    }

    if (!runningFiles.isEmpty()) {
      var planFiles = plan.getJobs().stream().flatMap(job -> job.getFiles().stream())
          .collect(Collectors.toSet());

      if (!Collections.disjoint(runningFiles, planFiles)) {
        return false;
      }
    }

    jobs.values().removeIf(status -> status == Status.QUEUED);

    return true;
  }

  public synchronized Runnable newCompactor(CompactionJob job) {

    jobs.keySet().forEach(sj -> {
      if (!Collections.disjoint(sj.getFiles(), job.getFiles())) {
        throw new IllegalArgumentException();
      }
    });

    if (!files.containsAll(job.getFiles())) {
      throw new IllegalArgumentException();
    }

    jobs.put(job, Status.QUEUED);

    return () -> {
      compact(job);
    };
  }

  class Snapshot {
    public final List<CompactionJob> running;
    public final List<CompactionJob> queued;

    public final Set<CompactableFile> files;
    public final long rewritten;

    Snapshot(Set<CompactableFile> files, List<CompactionJob> running, List<CompactionJob> queued,
        long rewritten) {
      this.files = Set.copyOf(files);
      this.running = List.copyOf(running);
      this.queued = List.copyOf(queued);
      this.rewritten = rewritten;
    }
  }

  public int getTableId() {
    return tabletId;
  }

  public synchronized Snapshot getSnapshot() {
    List<CompactionJob> running = new ArrayList<>();
    List<CompactionJob> queued = new ArrayList<>();

    jobs.forEach((job, status) -> {
      if (status == Status.RUNNING) {
        running.add(job);
      } else {
        queued.add(job);
      }
    });

    return new Snapshot(files, running, queued, rewritten);
  }

  public synchronized boolean hasCompactions() {
    return jobs.size() > 0;
  }
}
