package compaculation;

import java.util.Random;

public class Test {
  public static void main(String[] args) throws InterruptedException {
    TabletServer tserver = new TabletServer(100, new DefaultCompactionManager());

    Random random = new Random();
    int tablet = 0;
    
    for (int i = 0; i < 100000; i++) {
      
      tserver.addFile(tablet, 500000+random.nextInt(1000));
      tablet = (tablet+1) % 100;
      tserver.addFile(tablet, 500000+random.nextInt(1000));
      tablet = (tablet+1) % 100;
      tserver.addFile(tablet, 500000+random.nextInt(1000));
      tablet = (tablet+1) % 100;
      tserver.addFile(tablet, 500000+random.nextInt(1000));
      tablet = (tablet+1) % 100;
      
      tserver.printSummary();
      tserver.initiateCompactions();
      Thread.sleep(1);

      if (i % 10 == 0) {
        //tserver.printSummary();
      }
    }

    tserver.initiateCompactions();
    while(tserver.compactionsRunning()) {
      Thread.sleep(1000);
      tserver.initiateCompactions();
      
    }
        
    tserver.print();
    tserver.printSummary();

  }
}
