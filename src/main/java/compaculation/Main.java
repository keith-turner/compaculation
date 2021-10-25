package compaculation;

import java.util.Map;
import java.util.Random;

import org.apache.accumulo.core.spi.compaction.DefaultCompactionPlanner;

import compaculation.mgmt.AccumuloPlanner;

public class Main {
  public static void main(String[] args) {
    if (args.length != 1) {
      printUsage();
      return;
    }

    double ratio = Double.parseDouble(args[0]);

    Parameters params = new Parameters();

    params.numberOfTablets = 100;
    params.compactionTicker = size -> size / 5_000_000;
    var plannerOpts = Map.of("maxOpen", "10", "executors",
        "[{'name':'small','type':'internal','maxSize':'128M','numThreads':3},{'name':'large','type':'internal','numThreads':1}]");
    params.planner =
        new AccumuloPlanner(ratio, DefaultCompactionPlanner.class.getName(), plannerOpts);

    Random random = new Random();

    params.driver = (tick, tablets) -> {
      if (tick > 60*60*24*3)
        return false;

      int nt = tablets.getNumTablets();

      for(int i = 0; i<4; i++) {
        tablets.addFile(random.nextInt(nt), 490_000 + random.nextInt(2000));
      }
      
      return true;
    };

    new Compacultation(params).run();
  }

  private static void printUsage() {
    System.err.println("Usage : " + Main.class.getName() + " <ratio>");
  }
}
