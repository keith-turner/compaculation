package compaculation.sim;

import org.apache.accumulo.core.spi.compaction.CompactionJob;

public class Compactor implements Runnable {

  private CompactionJob job;
  private Runnable compactor;

  Compactor(CompactionJob job, Runnable compactor) {
    this.job = job;
    this.compactor = compactor;
  }

  public CompactionJob getJob() {
    return job;
  }

  @Override
  public void run() {
    compactor.run();
  }
}
