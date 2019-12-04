package compaculation;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.LongSupplier;

import compaculation.SubmittedJob.Status;

public class Tablet {
  private Map<String,Long> files = new HashMap<String,Long>();
  private Map<Long,Status> compactionStatus = new HashMap<>();
  private Map<Long,Job> jobs = new HashMap<>();

  // TODO maybe have a server context
  private LongSupplier idSupplier;

  private int tabletId;

  private long rewritten = 0;
  
  Tablet(int tabletId, LongSupplier idSupplier) {
    this.tabletId = tabletId;
    this.idSupplier = idSupplier;
  }

  public void addFile(long size) {
    String nfn = String.format("F%08d.fr", idSupplier.getAsLong());
    synchronized (this) {
      files.put(nfn, size);
    }
  }

  private void compact(long jobId, Job job) {

    long size = 0;

    synchronized (this) {
      if (compactionStatus.put(jobId, Status.RUNNING) != Status.QUEUED) {
        compactionStatus.remove(jobId);
        return;
      }

      for (String file : job.getFiles()) {
        size += files.get(file);
      }
    }

    // bytes per millis
    long bpm = 5000000;

    long sleepTime = Math.max(1, size / bpm);
    try {
      Thread.sleep(sleepTime);
    } catch (InterruptedException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }

    String nfn = String.format("C%08d.fr", idSupplier.getAsLong());
    int numFiles;
    synchronized (this) {
      compactionStatus.remove(jobId);
      jobs.remove(jobId);
      files.keySet().removeAll(job.getFiles());
      files.put(nfn, size);
      numFiles = files.size();
      rewritten += size;
    }

    if(tabletId == 0) {
     System.out.println("tablet : "+tabletId+" compacted ["+job+"] to "+nfn+" "+numFiles+" "+size+" "+sleepTime);
    }
  }

  public synchronized boolean cancelCompaction(long id) {
    if (compactionStatus.get(id) == Status.QUEUED) {
      compactionStatus.remove(id);
      jobs.remove(id);
      return true;
    }

    return false;
  }

  public synchronized Runnable newCompactor(Job job) {

    jobs.values().forEach(sj -> {
      if (!Collections.disjoint(sj.getFiles(), job.getFiles())) {
        throw new IllegalArgumentException();
      }
    });

    if (!files.keySet().containsAll(job.getFiles())) {
      throw new IllegalArgumentException();
    }

    long jobId = idSupplier.getAsLong();

    compactionStatus.put(jobId, Status.QUEUED);
    jobs.put(jobId, job);

    return () -> {
      compact(jobId, job);
    };
  }

  class Snapshot {
    public final List<SubmittedJob> jobs;
    public final Map<String,Long> files;
    public final long rewritten;

    Snapshot(Map<String,Long> files, List<SubmittedJob> jobs, long rewritten) {
      this.files = Map.copyOf(files);
      this.jobs = List.copyOf(jobs);
      this.rewritten = rewritten;
    }
  }

  public int getTableId() {
    return tabletId;
  }
  
  public synchronized Snapshot getSnapshot() {
    List<SubmittedJob> submitted = new ArrayList<>();

    jobs.forEach((id, job) -> submitted.add(new SubmittedJob(job, id, compactionStatus.get(id))));

    return new Snapshot(files, submitted, rewritten);
  }

  public synchronized boolean hasCompactions() {
    return compactionStatus.size() > 0;
  }
}
