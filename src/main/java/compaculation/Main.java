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
    params.compactionTicker = size -> size / 5000000;
    var plannerOpts = Map.of("maxOpen","10","executors","[{'name':'small','type':'internal','maxSize':'32M','numThreads':2},{'name':'medium','type':'internal','maxSize':'128M','numThreads':2},{'name':'large','type':'internal','numThreads':2}]");    
    params.planner = new AccumuloPlanner(ratio, DefaultCompactionPlanner.class.getName(), plannerOpts);


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
    System.err.println("Usage : " + Main.class.getName() + " <ratio>");
  }
}
