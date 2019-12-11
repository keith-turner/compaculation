package compaculation;

public interface Driver {

  // represents a collection of tablets
  public interface Tablets {
    public int getNumTablets();

    public void addFile(int tabletId, long size);
  }

  /**
   * Perform actions on the set of tablets for a given tick. Return false when the simulation should
   * stop.
   */
  public boolean drive(int tick, Tablets tablets);

}
