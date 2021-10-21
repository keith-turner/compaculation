package compaculation.sim;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.LongSupplier;
import java.util.stream.Collectors;

import org.apache.accumulo.core.client.admin.compaction.CompactableFile;
import org.apache.accumulo.core.spi.compaction.CompactionExecutorId;
import org.apache.accumulo.core.spi.compaction.CompactionJob;
import org.apache.accumulo.core.spi.compaction.CompactionPlan;

import compaculation.Driver.Tablets;
import compaculation.Parameters;
import compaculation.mgmt.CompaculationPlanner;
import compaculation.sim.Tablet.Snapshot;

public class TabletServer implements Tablets {
  List<Tablet> tablets;
  CompaculationPlanner compactionManager;
  Map<CompactionExecutorId,ExecutorService> executors;

  private static short extractPriority(Runnable r) {
    return ((Compactor) r).getJob().getPriority();
  }

  private static ExecutorService newFixedThreadPool(int nThreads) {

    var comparator = Comparator.comparingInt(TabletServer::extractPriority).reversed();

    PriorityBlockingQueue<Runnable> queue = new PriorityBlockingQueue<Runnable>(100, comparator);

    // TODO periodically call removeIf on queue to remove canceled jobs

    return new ThreadPoolExecutor(nThreads, nThreads, 0L, TimeUnit.MILLISECONDS, queue);
  }

  public TabletServer(Parameters params) {

    AtomicLong idcounter = new AtomicLong();
    LongSupplier ids = idcounter::getAndIncrement;

    tablets = new ArrayList<Tablet>(params.numberOfTablets);
    for (int i = 0; i < params.numberOfTablets; i++) {
      tablets.add(new Tablet(params, i, ids));
    }

    executors = new HashMap<>();

    params.planner.getExecutorConfig()
        .forEach(ec -> executors.put(ec.id, newFixedThreadPool(ec.numThreads)));

    compactionManager = params.planner;
  }

  public void addFile(int tablet, long size) {
    tablets.get(tablet).addFile(size);
  }

  public boolean compactionsRunning() {
    for (Tablet tablet : tablets) {
      if (tablet.hasCompactions()) {
        return true;
      }
    }

    return false;
  }

  public void initiateCompactions() {
    for (Tablet tablet : tablets) {
      Snapshot snapshot = tablet.getSnapshot();

      Set<CompactableFile> candidates = new HashSet<>(snapshot.files);
      snapshot.running.forEach(job -> candidates.removeAll(job.getFiles()));

      CompactionPlan plan =
          compactionManager.makePlan(snapshot.files, candidates, snapshot.running);

      boolean allCancelled = tablet.cancelCompactions(plan, snapshot.running);

      if (allCancelled) {
        for (CompactionJob job : plan.getJobs()) {
          var ct = tablet.newCompactor(job);
          executors.get(job.getExecutor()).execute(new Compactor(job, ct));
        }
      }
    }
  }

  public void printSummary(int tick) {
    var snapshots = tablets.stream().map(Tablet::getSnapshot).collect(Collectors.toList());

    var fsum = snapshots.stream().mapToInt(snap -> snap.files.size()).summaryStatistics();
    var csum = snapshots.stream().mapToInt(snap -> snap.running.size() + snap.queued.size())
        .summaryStatistics();
    long totalRewritten = snapshots.stream().mapToLong(snap -> snap.rewritten).sum();
    long totalSize = snapshots.stream().flatMap(snap -> snap.files.stream())
        .mapToLong(cf -> cf.getEstimatedSize()).sum();

    System.out.printf("%d %d %d %f %d %d %f %,d %,d\n", tick, fsum.getMin(), fsum.getMax(),
        fsum.getAverage(), csum.getMin(), csum.getMax(), csum.getAverage(), totalRewritten,
        totalSize);
  }

  public void print() {
    for (Tablet tablet : tablets) {
      var snap = tablet.getSnapshot();
      System.out.println("Tablet : " + tablet.getTableId());

      Comparator<CompactableFile> comp = Comparator.comparing(CompactableFile::getEstimatedSize);
      snap.files.stream().sorted(comp.reversed())
          .forEach(e -> System.out.println("  " + e.getFileName() + " " + e.getEstimatedSize()));

      snap.queued.forEach(job -> System.out.println("QUEUED " + job));
      snap.running.forEach(job -> System.out.println("RUNNING " + job));

      System.out.println();
    }
  }

  @Override
  public int getNumTablets() {
    return tablets.size();
  }

  public void shutdown() {
    executors.values().forEach(ExecutorService::shutdown);
  }
}
