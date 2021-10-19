package compaculation.mgmt;

import java.util.Set;

import org.apache.accumulo.core.client.admin.compaction.CompactableFile;
import org.apache.accumulo.core.spi.compaction.CompactionExecutorId;
import org.apache.accumulo.core.spi.compaction.CompactionJob;
import org.apache.accumulo.core.spi.compaction.CompactionKind;

public class Job implements CompactionJob {
	private final Set<CompactableFile> files;
	private final CompactionExecutorId executor;
	private final short prio;

	public Job(short prio, Set<CompactableFile> files, CompactionExecutorId executor) {
		this.prio = prio;
		this.files = Set.copyOf(files);
		this.executor = executor;
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
}
