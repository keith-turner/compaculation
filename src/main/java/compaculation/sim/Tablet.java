package compaculation.sim;

import java.net.URI;
import java.net.URISyntaxException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Function;
import java.util.function.LongSupplier;
import java.util.stream.Collectors;

import org.apache.accumulo.core.client.admin.compaction.CompactableFile;
import org.apache.accumulo.core.spi.compaction.CompactionJob;
import org.apache.accumulo.core.spi.compaction.CompactionPlan;

import com.google.common.collect.Sets;

import compaculation.Parameters;

public class Tablet {

  private Set<CompactableFile> files = new HashSet<>();
  private Set<CompactionJob> queuedJobs = new HashSet<>();
  private Set<CompactionJob> runningJobs = new HashSet<>();

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
      if (queuedJobs.remove(job)) {
        runningJobs.add(job);
      } else {
        return;
      }

      // System.out.println("Compacting "+tabletId+" files:"+files+" job:"+job.getFiles());
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
      runningJobs.remove(job);
      files.removeAll(job.getFiles());
      files.add(cFile);
      rewritten += size;
    }
  }

  public synchronized boolean cancelCompactions(CompactionPlan plan,
      Collection<CompactionJob> prevRunning) {

    if (plan.getJobs().isEmpty()
        || Sets.union(runningJobs, queuedJobs).containsAll(plan.getJobs())) {
      return false;
    }

    if (runningJobs.isEmpty() && queuedJobs.isEmpty()) {
      return true;
    }

    Set<CompactableFile> runningFiles = new HashSet<>();

    runningJobs.forEach(job -> {
      runningFiles.addAll(job.getFiles());
    });

    if (!runningJobs.equals(prevRunning)) {
      return false;
    }

    if (!runningFiles.isEmpty()) {
      var planFiles = plan.getJobs().stream().flatMap(job -> job.getFiles().stream())
          .collect(Collectors.toSet());

      if (!Collections.disjoint(runningFiles, planFiles)) {
        return false;
      }
    }

    queuedJobs.clear();

    return true;
  }

  public synchronized Runnable newCompactor(CompactionJob job) {

    Sets.union(runningJobs, queuedJobs).forEach(sj -> {
      if (!Collections.disjoint(sj.getFiles(), job.getFiles())) {
        throw new IllegalArgumentException();
      }
    });

    if (!files.containsAll(job.getFiles())) {
      throw new IllegalArgumentException();
    }

    queuedJobs.add(job);

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
    List<CompactionJob> running = new ArrayList<>(runningJobs);
    List<CompactionJob> queued = new ArrayList<>(queuedJobs);
    return new Snapshot(files, running, queued, rewritten);
  }

  public synchronized boolean hasCompactions() {
    return Sets.union(runningJobs, queuedJobs).size() > 0;
  }
}
