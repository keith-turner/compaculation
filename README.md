Compaculation : An Accumulo Compaction Simulator
================================================

This project was created to simulate allowing multiple concurrent compactions
per tablet as described in
[#564](https://github.com/apache/accumulo/issues/564).  The simulation includes
multiple tablets and multiple executors.  In the simulation multiple concurrent
compactions can be scheduled for each tablet.

The simulation has the following properties.

 * One tick per millisecond.  A tick simulates a second of real time.
 * When a simulation compaction executes in a thread pool is sleeps a number of ticks based on the input data size.  
 * Every tick, the simulation adds files to zero or more tablets.

The following commands can be used to run the simulation.  To change the simulation, edit the [Parameters](src/compaculation/Parameters.java) object in [Main](src/compaculation/Main.java).

```
$ mvn compile
$ java -cp target/classes compaculation.Main 3 NEW > results.txt
```

After running, the following command will plot the average number of files per
tablet over the simulation.

```
$ gnuplot
gnuplot> plot 'results.txt' using 1:4
```


