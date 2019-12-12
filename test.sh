#!/bin/bash

mvn clean compile
java -cp target/classes compaculation.Main 3 NEW > results_3_NEW.txt
java -cp target/classes compaculation.Main 3 OLD > results_3_OLD.txt
java -cp target/classes compaculation.Main 2 NEW > results_2_NEW.txt
java -cp target/classes compaculation.Main 2 OLD > results_2_OLD.txt
java -cp target/classes compaculation.Main 1.5 NEW > results_1.5_NEW.txt
java -cp target/classes compaculation.Main 1.5 OLD > results_1.5_OLD.txt
