Compaculation : An Accumulo Compaction Simulator
================================================

This project simulates multiple concurrent compactions per tablet running on
multiple executors.  The simulation includes multiple tablets and multiple
executors.  The simulation uses an Accumulo compaction planner.  This allows
seeing how a planner behaves in different scenarios. 

The simulation has the following properties.

 * One tick per millisecond.  A tick simulates a second of real time.
 * When a simulation compaction executes in a thread pool is sleeps a number of ticks based on the input data size.  
 * Every tick, the simulation adds files to zero or more tablets.

The following commands can be used to run the simulation.  To change the simulation, edit the [Parameters](src/compaculation/Parameters.java) object in [Main](src/compaculation/Main.java).

```
$ mvn compile -q exec:java -Dexec.mainClass="compaculation.Main" -Dexec.args="3" > results.txt
```

After running, the following command will plot the average number of files per
tablet and the compactions queued per executor over the simulation.

```bash
# roll data for each 100 ticks of the simulation into a single line.  This
# command assumes two compaction executors, if different the will need change all
# col numbers in the commands below.
# 
$ cat results.txt | datamash -W -H -f bin:100 1 | datamash -H -g 11 mean 4 mean 5 mean 6 mean 7 mean 8 > results-summary.txt
$ gnuplot
gnuplot> set key top left autotitle columnhead
gnuplot> plot 'results-summary.txt' using 1:2 with lines , '' using 1:5 with lines, '' using 1:6 with lines
```


