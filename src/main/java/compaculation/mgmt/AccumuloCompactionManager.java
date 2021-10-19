package compaculation.mgmt;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import org.apache.accumulo.core.client.admin.compaction.CompactableFile;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.spi.common.ServiceEnvironment;
import org.apache.accumulo.core.spi.compaction.CompactionExecutorId;
import org.apache.accumulo.core.spi.compaction.CompactionJob;
import org.apache.accumulo.core.spi.compaction.CompactionKind;
import org.apache.accumulo.core.spi.compaction.CompactionPlan.Builder;
import org.apache.accumulo.core.spi.compaction.CompactionPlanner;
import org.apache.accumulo.core.spi.compaction.CompactionPlanner.InitParameters;
import org.apache.accumulo.core.spi.compaction.CompactionPlanner.PlanningParameters;
import org.apache.accumulo.core.spi.compaction.ExecutorManager;

import compaculation.ExecutorConfig;

public class AccumuloCompactionManager implements CompaculationPlanner {

	
	private CompactionPlanner planner;
	private List<ExecutorConfig> execConfig = new ArrayList<>();

	AccumuloCompactionManager(String className, Map<String, String> options) {
		try {
		    planner = AccumuloCompactionManager.class.getClassLoader().loadClass(className).asSubclass(CompactionPlanner.class).newInstance();
			planner.init(new InitParameters() {

				@Override
				public ServiceEnvironment getServiceEnvironment() {
					throw new UnsupportedOperationException();
				}

				@Override
				public Map<String, String> getOptions() {
					return options;
				}

				@Override
				public String getFullyQualifiedOption(String key) {
					throw new UnsupportedOperationException();
				}

				@Override
				public ExecutorManager getExecutorManager() {
					// TODO Auto-generated method stub
					return new ExecutorManager() {

						@Override
						public CompactionExecutorId createExecutor(String name, int threads) {
							execConfig.add(new ExecutorConfig(name, threads));
							return new CompactionExecutorId(name) {
								
							};
						}

						@Override
						public CompactionExecutorId getExternalExecutor(String name) {
							throw new UnsupportedOperationException();
						}
					};
				}
			});
			
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}
	
	
	@Override
	public CompactionPlan makePlan(Map<String, Long> files, List<CompactionJob> running) {
		// TODO Auto-generated method stub
		var aplan = planner.makePlan(new PlanningParameters() {

			@Override
			public TableId getTableId() {
				// TODO Auto-generated method stub
				return null;
			}

			@Override
			public ServiceEnvironment getServiceEnvironment() {
				throw new UnsupportedOperationException();
			}

			@Override
			public CompactionKind getKind() {
				return CompactionKind.SYSTEM;
			}

			@Override
			public double getRatio() {
				// TODO Auto-generated method stub
				return 0;
			}

			@Override
			public Collection<CompactableFile> getAll() {
				// TODO Auto-generated method stub
				return null;
			}

			@Override
			public Collection<CompactableFile> getCandidates() {
				// TODO Auto-generated method stub
				return null;
			}

			@Override
			public Collection<CompactionJob> getRunningCompactions() {
				return running;
			}

			@Override
			public Map<String, String> getExecutionHints() {
				return Map.of();
			}

			@Override
			public Builder createPlanBuilder() {
				// TODO Auto-generated method stub
				return null;
			}
		});
		
		return null;
	}

}
