package compaculation.mgmt;

import java.util.Collection;
import java.util.Set;

public class Job {
  private final Collection<String> files;
  private final String executor;
  private final int totalFiles;

  public Job(int totalFiles, Collection<String> files, String executor) {
    this.totalFiles = totalFiles;
    this.files = Set.copyOf(files);
    this.executor = executor;
  }

  // todo maybe have a tablet desc object?
  public int getTotalFiles() {
    return totalFiles;
  }

  public Collection<String> getFiles() {
    return files;
  }

  public String getExecutor() {
    return executor;
  }

  @Override
  public String toString() {
    return "executor: " + executor + " files: " + files;
  }
}
