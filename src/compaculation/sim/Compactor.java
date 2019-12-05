package compaculation.sim;

import compaculation.mgmt.Job;

public class Compactor implements Runnable {

  private Job job;
  private Runnable compactor;

  Compactor(Job job, Runnable compactor) {
    this.job = job;
    this.compactor = compactor;
  }

  public Job getJob() {
    return job;
  }

  @Override
  public void run() {
    compactor.run();
  }
}
