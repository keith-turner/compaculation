package compaculation;

import java.util.List;
import java.util.Random;

import compaculation.mgmt.DefaultCompactionManager;
import compaculation.mgmt.TieredCompactionManager;

public class Main {
  public static void main(String[] args) {
    if (args.length != 2) {
      printUsage();
      return;
    }

    double ratio = Double.parseDouble(args[0]);

    Parameters params = new Parameters();

    params.numberOfTablets = 100;
    params.compactionTicker = size -> size / 5000000;
    switch (args[1]) {
      case "NEW":
        params.compactionManager = new TieredCompactionManager(ratio);
        break;
      case "OLD":
        params.compactionManager = new DefaultCompactionManager(ratio);
        break;
      default:
        printUsage();
        return;
    }

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

  private static void printUsage() {
    System.err.println("Usage : " + Main.class.getName() + " <ratio> OLD|NEW");
  }
}
