JARS=lib/hadoop-mapreduce-client-core-2.7.2.jar:lib/hadoop-common-2.7.2.jar

all:
	make prob DIR=src

prob:
	rm -rf bin average-ratings.dat ratings-summary.dat
	mkdir -p bin
	javac -classpath $(JARS) -d bin src/*.java
	jar -cvf exec.jar -C bin .
	hadoop jar exec.jar Main rating.dat average-ratings.dat ratings-summary.dat
