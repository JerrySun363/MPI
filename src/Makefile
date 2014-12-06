clean:
	rm DNA/*.class
	rm Point/*.class

all:
	javac Point/Point.java
	javac */Seq*.java
	javac DNA/GenerateDNAStrand.java
	mpijavac */MPI*.java

generate_point:
	python ./Point/generaterawdata.py -c $(K) -p $(Point) -o $(Output)

run_seq_point:
	java Point/SeqPointCluster $(Input) $(K) $(Output)

generate_DNA:
	java DNA/GenerateDNAStrand $(Num) $(Length) $(Output)

run_seq_DNA:
	java DNA/SeqDNACluster $(Input) $(K) $(Output)

run_MPI_Point:
	mpirun -np $(Procs) java Point/MPIPointCluster $(Input) $(K) $(Number) $(Output)

run_MPI_DNA:
	mpirun -np $(Procs) java DNA/MPIDNACluster $(Input) $(K) $(Length) $(Number) $(Output)
