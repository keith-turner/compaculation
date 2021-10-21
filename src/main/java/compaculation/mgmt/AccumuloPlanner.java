package compaculation.mgmt;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.apache.accumulo.core.client.admin.compaction.CompactableFile;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.spi.common.ServiceEnvironment;
import org.apache.accumulo.core.spi.compaction.CompactionExecutorId;
import org.apache.accumulo.core.spi.compaction.CompactionJob;
import org.apache.accumulo.core.spi.compaction.CompactionKind;
import org.apache.accumulo.core.spi.compaction.CompactionPlan;
import org.apache.accumulo.core.spi.compaction.CompactionPlan.Builder;
import org.apache.accumulo.core.spi.compaction.CompactionPlanner;
import org.apache.accumulo.core.spi.compaction.CompactionPlanner.InitParameters;
import org.apache.accumulo.core.spi.compaction.CompactionPlanner.PlanningParameters;
import org.apache.accumulo.core.spi.compaction.ExecutorManager;

import compaculation.ExecutorConfig;

public class AccumuloPlanner implements CompaculationPlanner {

  private CompactionPlanner planner;
  private List<ExecutorConfig> execConfig = new ArrayList<>();
  private double ratio;

  public AccumuloPlanner(double ratio, String className, Map<String, String> options) {
    
    this.ratio = ratio;
    
    try {
      planner = AccumuloPlanner.class.getClassLoader().loadClass(className)
          .asSubclass(CompactionPlanner.class).newInstance();
      planner.init(new InitParameters() {

        @Override
        public ServiceEnvironment getServiceEnvironment() {
          throw new UnsupportedOperationException();
        }

        @Override
        public Map<String,String> getOptions() {
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
              var id = new CompactionExecutorId(name) {};
              execConfig.add(new ExecutorConfig(id, threads));
              return id;
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
  public CompactionPlan makePlan(Collection<CompactableFile> allFiles,
      Collection<CompactableFile> candidates, List<CompactionJob> running) {
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
        return ratio;
      }

      @Override
      public Collection<CompactableFile> getAll() {
        return allFiles;
      }

      @Override
      public Collection<CompactableFile> getCandidates() {
        return candidates;
      }

      @Override
      public Collection<CompactionJob> getRunningCompactions() {
        return running;
      }

      @Override
      public Map<String,String> getExecutionHints() {
        return Map.of();
      }

      @Override
      public Builder createPlanBuilder() {
        // TODO Auto-generated method stub
        return new Builder() {

          List<CompactionJob> jobs = new ArrayList<>();

          @Override
          public Builder addJob(short priority, CompactionExecutorId executor,
              Collection<CompactableFile> group) {
            jobs.add(new Job(priority, group, executor));
            return this;
          }

          @Override
          public org.apache.accumulo.core.spi.compaction.CompactionPlan build() {

            var immutableJobs = Set.copyOf(jobs);

            return new org.apache.accumulo.core.spi.compaction.CompactionPlan() {

              @Override
              public Collection<CompactionJob> getJobs() {
                return immutableJobs;
              }
            };
          }
        };
      }
    });

    return aplan;
  }

  @Override
  public Collection<ExecutorConfig> getExecutorConfig() {
    return execConfig;
  }

}
