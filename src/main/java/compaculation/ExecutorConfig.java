package compaculation;

public class ExecutorConfig {

  public final String name;
  public final int numThreads;

  public ExecutorConfig(String name, int numThreads) {
    this.name = name;
    this.numThreads = numThreads;
  }
}
