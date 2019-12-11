package compaculation.mgmt;

public class SubmittedJob extends Job {

  // TODO encapsulate instead of extent Job ?

  public enum Status {
    RUNNING, QUEUED
  }

  private final Status status;
  private final long id; // in accumulo would want a CompactionJobId type

  public SubmittedJob(Job job, long id, Status status) {
    super(job.getTotalFiles(), job.getFiles(), job.getExecutor());
    this.id = id;
    this.status = status;
  }

  public long getId() {
    return id;
  }

  public Status getStatus() {
    return status;
  }

}
