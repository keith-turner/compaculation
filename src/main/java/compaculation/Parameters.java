package compaculation;

import java.util.function.Function;

import compaculation.mgmt.CompaculationPlanner;

/**
 * Parameters for running a compaction simulation.
 */
public class Parameters {

  // This function decides how many ticks a compaction should take for a given input data size
  public Function<Long,Long> compactionTicker;

  // number of tablets to create
  public int numberOfTablets;

  // compaction manager to use for the simulation
  public CompaculationPlanner planner;

  // test driver that creates files and decides when the simulation should stop
  public Driver driver;

}
