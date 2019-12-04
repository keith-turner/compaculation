package compaculation;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.LongSupplier;
import java.util.stream.Collectors;

import compaculation.Tablet.Snapshot;

public class TabletServer {
  List<Tablet> tablets;
  CompactionManager compactionManager;
  Map<String,Executor> executors;

  private static long extractTotalFiles(Runnable r) {
    return ((Compactor) r).getJob().getTotalFiles();
  }

  private static long extractJobFiles(Runnable r) {
    return ((Compactor) r).getJob().getFiles().size();
  }

  private static ExecutorService newFixedThreadPool(int nThreads) {

    var comparator = Comparator.comparingLong(TabletServer::extractTotalFiles)
        .thenComparingLong(TabletServer::extractJobFiles).reversed();

    PriorityBlockingQueue<Runnable> queue = new PriorityBlockingQueue<Runnable>(100, comparator);

    // TODO periodically call removeIf on queue to remove canceled jobs

    return new ThreadPoolExecutor(nThreads, nThreads, 0L, TimeUnit.MILLISECONDS, queue);
  }

  public TabletServer(int numTablets, CompactionManager cm) {

    AtomicLong idcounter = new AtomicLong();
    LongSupplier ids = idcounter::getAndIncrement;

    tablets = new ArrayList<Tablet>(numTablets);
    for (int i = 0; i < numTablets; i++) {
      tablets.add(new Tablet(i, ids));
    }

    executors = new HashMap<>();

    executors.put("small", newFixedThreadPool(2));
    executors.put("medium", newFixedThreadPool(2));
    executors.put("large", newFixedThreadPool(2));
    executors.put("huge", newFixedThreadPool(2));

    compactionManager = cm;
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

      CompactionPlan plan = compactionManager.makePlan(snapshot.files, snapshot.jobs);

      boolean allCancelled = true;
      for (Long id : plan.cancellations) {
        allCancelled &= tablet.cancelCompaction(id);
      }

      if (allCancelled) {
        for (Job job : plan.jobs) {
          var ct = tablet.newCompactor(job);
          executors.get(job.getExecutor()).execute(new Compactor(job, ct));
        }
      }
    }
  }

  public void printSummary() {
    var snapshots = tablets.stream().map(Tablet::getSnapshot).collect(Collectors.toList());
    
    var fsum = snapshots.stream().mapToInt(snap -> snap.files.size()).summaryStatistics();
    var csum = snapshots.stream().mapToInt(snap -> snap.jobs.size()).summaryStatistics();
    long totalRewritten = snapshots.stream().mapToLong(snap -> snap.rewritten).sum();
    long totalSize = snapshots.stream().flatMap(snap -> snap.files.values().stream()).mapToLong(l -> l).sum();
    
    System.out.printf("%d %d %f %d %d %f %,d %,d\n",fsum.getMin(), fsum.getMax(), fsum.getAverage(), csum.getMin(), csum.getMax(), csum.getAverage(), totalRewritten, totalSize);
  }

  public void print() {
    for (Tablet tablet : tablets) {
      var snap = tablet.getSnapshot();
      System.out.println("Tablet : " + tablet.getTableId());

      Comparator<Map.Entry<String,Long>> comp = Comparator.comparing(Entry::getValue);
      snap.files.entrySet().stream().sorted(comp.reversed())
          .forEach(e -> System.out.println("  " + e.getKey() + " " + e.getValue()));

      snap.jobs.forEach(System.out::println);

      System.out.println();
    }
  }
}
