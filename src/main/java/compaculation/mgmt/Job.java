package compaculation.mgmt;

import java.util.Collection;
import java.util.Objects;
import java.util.Set;

import org.apache.accumulo.core.client.admin.compaction.CompactableFile;
import org.apache.accumulo.core.spi.compaction.CompactionExecutorId;
import org.apache.accumulo.core.spi.compaction.CompactionJob;
import org.apache.accumulo.core.spi.compaction.CompactionKind;

public class Job implements CompactionJob {
  private final Set<CompactableFile> files;
  private final CompactionExecutorId executor;
  private final short prio;
  private final int hash;
  private volatile boolean canceled = false;

  public Job(short prio, Collection<CompactableFile> files, CompactionExecutorId executor) {
    this.prio = prio;
    this.files = Set.copyOf(files);
    this.executor = executor;
    this.hash = Objects.hash(this.files, this.executor, this.prio);
  }

  @Override
  public String toString() {
    return "executor: " + executor + " files: " + files;
  }

  @Override
  public short getPriority() {
    return prio;
  }

  @Override
  public CompactionExecutorId getExecutor() {
    return executor;
  }

  @Override
  public Set<CompactableFile> getFiles() {
    return files;
  }

  @Override
  public CompactionKind getKind() {
    return CompactionKind.SYSTEM;
  }

  public void markCanceled() {
    canceled = true;
  }
  
  public boolean isCanceled() {
    return canceled;
  }
  
  @Override
  public int hashCode() {
    return hash;
  }

  @Override
  public boolean equals(Object o) {
    if (o instanceof Job) {
      Job j = (Job) o;
      return hash == j.hash && files.equals(j.files) && executor.equals(j.executor)
          && prio == j.prio;
    }

    return false;
  }
}
