package compaculation.sim;

import static java.util.stream.Collectors.counting;
import static java.util.stream.Collectors.groupingBy;
import static java.util.stream.Collectors.toList;

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
import compaculation.mgmt.Job;
import compaculation.sim.Tablet.Snapshot;

public class TabletServer implements Tablets {
  List<Tablet> tablets;
  CompaculationPlanner compactionManager;
  Map<CompactionExecutorId,ThreadPoolExecutor> executors;
  List<CompactionExecutorId> orderedCEIs;
  long initiateCallCounter = 0;

  private static short extractPriority(Runnable r) {
    return ((Compactor) r).getJob().getPriority();
  }

  private static ThreadPoolExecutor newFixedThreadPool(int nThreads) {

    var comparator = Comparator.comparingInt(TabletServer::extractPriority).reversed();

    PriorityBlockingQueue<Runnable> queue = new PriorityBlockingQueue<Runnable>(100, comparator);

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

    orderedCEIs = executors.keySet().stream().sorted().collect(toList());

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

  private static boolean isCanceled(Runnable r) {
    return ((Job)(((Compactor) r).getJob())).isCanceled();
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
    
    if(initiateCallCounter++ % 60 == 0) {
      // periodically remove canceled jobs that are sitting in the queue
      executors.values().forEach(threadPool -> threadPool.getQueue().removeIf(TabletServer::isCanceled));
    }
  }

  public void printSummaryHeader() {

    String rcols = orderedCEIs.stream().map(cei -> "r-" + cei).collect(Collectors.joining(" "));
    String qcols = orderedCEIs.stream().map(cei -> "q-" + cei).collect(Collectors.joining(" "));

    System.out.println("tick fsumMin fsumMax fsumAvg " + rcols + " " + qcols + " rewritten size");
  }

  public void printSummary(int tick) {
    var snapshots = tablets.stream().map(Tablet::getSnapshot).collect(Collectors.toList());

    var fsum = snapshots.stream().mapToInt(snap -> snap.files.size()).summaryStatistics();

    var perExecRunningCounts =
        snapshots.stream().flatMap(snap -> snap.running.stream().map(job -> job.getExecutor()))
            .collect(groupingBy(e -> e, counting()));
    var perExecQueuedCounts =
        snapshots.stream().flatMap(snap -> snap.queued.stream().map(job -> job.getExecutor()))
            .collect(groupingBy(e -> e, counting()));

    var queuedCounts = new ArrayList<String>();
    var runningCounts = new ArrayList<String>();

    for (CompactionExecutorId cei : orderedCEIs) {
      queuedCounts.add(perExecQueuedCounts.getOrDefault(cei, 0L) + "");
      runningCounts.add(perExecRunningCounts.getOrDefault(cei, 0L) + "");
    }

    String qCounts = queuedCounts.stream().collect(Collectors.joining(" "));
    String rCounts = runningCounts.stream().collect(Collectors.joining(" "));

    long totalRewritten = snapshots.stream().mapToLong(snap -> snap.rewritten).sum();
    long totalSize = snapshots.stream().flatMap(snap -> snap.files.stream())
        .mapToLong(cf -> cf.getEstimatedSize()).sum();

    System.out.printf("%d %d %d %f %s %s %,d %,d\n", tick, fsum.getMin(), fsum.getMax(),
        fsum.getAverage(), rCounts, qCounts, totalRewritten, totalSize);
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
