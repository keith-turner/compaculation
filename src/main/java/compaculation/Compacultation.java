package compaculation;

import compaculation.sim.TabletServer;

public class Compacultation {
  private Parameters params;

  Compacultation(Parameters params) {
    this.params = params;
  }

  private void sleep(long millis) {
    try {
      Thread.sleep(millis);
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
  }

  public void run() {
    TabletServer tserver = new TabletServer(params);

    int tick = 0;

    tserver.printSummaryHeader();

    while (params.driver.drive(tick, tserver)) {
      tserver.printSummary(tick);
      tick++;
      tserver.initiateCompactions();
      sleep(1);
    }

    tserver.initiateCompactions();
    while (tserver.compactionsRunning()) {
      tick++;
      sleep(1);
      tserver.initiateCompactions();

    }

    // tserver.print();
    tserver.printSummary(tick);

    tserver.shutdown();
  }

}
