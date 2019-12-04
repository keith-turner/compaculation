package compaculation;

import java.util.List;

public class CompactionPlan {
  public final List<Long> cancellations;
  public final List<Job> jobs;

  public CompactionPlan() {
    this.cancellations = List.of();
    this.jobs = List.of();
  }

  public CompactionPlan(List<Job> jobs, List<Long> cancellations) {
    this.jobs = List.copyOf(jobs);
    this.cancellations = List.copyOf(cancellations);
  }

  @Override
  public String toString() {
    return "jobs: " + jobs + " cancellations: " + cancellations;
  }
}
