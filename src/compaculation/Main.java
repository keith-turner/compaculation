package compaculation;

import java.util.List;
import java.util.Random;

import compaculation.mgmt.DefaultCompactionManager;
import compaculation.mgmt.TieredCompactionManager;

public class Main {
  public static void main(String[] args) {
    Parameters params = new Parameters();

    params.numberOfTablets = 100;
    params.compactionTicker = size ->  size / 5000000;
    //params.compactionManager = new DefaultCompactionManager(3);
    params.compactionManager = new TieredCompactionManager(3);
    params.executors = List.of(new ExecutorConfig("huge", 2), new ExecutorConfig("large", 2),
        new ExecutorConfig("medium", 2), new ExecutorConfig("small", 2));

    Random random = new Random();

    params.driver = (tick, tablets) -> {
      if (tick > 100000)
        return false;

      int nt = tablets.getNumTablets();

      tablets.addFile(random.nextInt(nt), 500000 + random.nextInt(1000));
      tablets.addFile(random.nextInt(nt), 500000 + random.nextInt(1000));
      tablets.addFile(random.nextInt(nt), 500000 + random.nextInt(1000));
      tablets.addFile(random.nextInt(nt), 500000 + random.nextInt(1000));

      return true;
    };

    new Compacultation(params).run();
  }
}
