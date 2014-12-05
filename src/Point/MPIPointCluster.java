package Point;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Random;

import mpi.*;

public class MPIPointCluster {

	private int rank;
	private int procs;

	// each number represents the cluster it belongs to.
	private int[] clusters;
	private int[] capacity;

	private double[] xPoint;
	private double[] yPoint;
	private double[] seedX;
	private double[] seedY;
	private int clusterNumber;

	public static void main(String args[]) throws MPIException{
		if (args.length != 2) {
			System.out
					.println("Usage: MPIPointCluster <DataFileName> <ClusterNumber>");
			System.exit(-1);
		}
		
		MPI.Init(args);
		MPIPointCluster cluster = new MPIPointCluster(Integer.parseInt(args[1]));
		cluster.readData(args[0]);
		cluster.initSeed();
		//start to calculate time data
		cluster.init();
		cluster.iteration();
		//time ends here.
		MPI.Finalize();
		cluster.printCluster();
	}

	public MPIPointCluster(int k) throws MPIException {
		this.rank = MPI.COMM_WORLD.Rank();
		this.procs = MPI.COMM_WORLD.Size();

	}

	private void readData(String filename) {
		ArrayList<Double> xs = new ArrayList<>();
		ArrayList<Double> ys = new ArrayList<>();
		try {
			BufferedReader br = new BufferedReader(new FileReader(filename));
			String line = null;
			while ((line = br.readLine()) != null) {
				String[] coordinate = line.split(",");
				double x = Double.parseDouble(coordinate[0]);
				double y = Double.parseDouble(coordinate[1]);
				xs.add(x);
				ys.add(y);
			}
			br.close();
		} catch (FileNotFoundException e) {
			System.out.println(filename + " does not exist!");
			System.exit(-1);
		} catch (IOException e) {
			System.out.println("I/O Exception while reading the data");
			System.exit(-1);
		}
		this.xPoint = new double[xs.size()];
		this.yPoint = new double[ys.size()];
		for (int i = 0; i < xPoint.length; i++) {
			this.xPoint[i] = xs.get(i);
			this.yPoint[i] = ys.get(i);
		}
	}

	private void initSeed() {
		this.seedX = new double[this.clusterNumber];
		this.seedY = new double[this.clusterNumber];
		Random rand = new Random();
		for (int i = 0; i < this.clusterNumber; i++) {
			int index = rand.nextInt(this.xPoint.length);
			this.seedX[i] = this.xPoint[index];
			this.seedY[i] = this.yPoint[index];
		}
	}

	/**
	 * Send data to each process
	 */
	public void init() throws MPIException{
		this.clusters = new int[xPoint.length];
		Arrays.fill(clusters, -1);

		this.capacity = new int[this.procs];
		for (int i = 1; i < this.procs; i++) {
			this.capacity[i] = xPoint.length / (this.procs - 1)
					+ (i <= xPoint.length % (this.procs - 1) ? 1 : 0);
		}

		if (rank == 0) {// master
			int offset = 0;
			for (int i = 1; i < this.procs; i++) {
				MPI.COMM_WORLD.Send(xPoint, offset, this.capacity[i],
						MPI.DOUBLE, i, i);
				MPI.COMM_WORLD.Send(yPoint, offset, this.capacity[i],
						MPI.DOUBLE, i, i);
				MPI.COMM_WORLD.Send(clusters, offset, this.capacity[i],
						MPI.INT, i, i);
				offset += this.capacity[i];
			}
		} else {
			MPI.COMM_WORLD.Recv(xPoint, 0, this.capacity[rank], MPI.DOUBLE, 0,
					rank);
			MPI.COMM_WORLD.Recv(yPoint, 0, this.capacity[rank], MPI.DOUBLE, 0,
					rank);
			MPI.COMM_WORLD.Recv(clusters, 0, this.capacity[rank], MPI.INT, 0,
					rank);
		}
	}

	public void iteration() throws MPIException{
		boolean[] changed = new boolean[1];
		changed[0] = true;
		int count = 0;
		while (changed[0]) {
			System.out.println("Iteration #"+count);
			count++;
			MPI.COMM_WORLD.Bcast(seedX, 0, this.clusterNumber, MPI.DOUBLE, 0);
			MPI.COMM_WORLD.Bcast(seedY, 0, this.clusterNumber, MPI.DOUBLE, 0);

			for (int i = 0; i < this.capacity[rank]; i++) {
				double dis = Double.MAX_VALUE;
				
				for (int j = 0; j < seedX.length; j++) {
					double mydis = distance(xPoint[i], yPoint[i], seedX[i],
							seedY[i]);
					if (mydis < dis) {
						dis = mydis; 
						this.clusters[i] = j;
					}
				}
			}
			// calculate distance
			if (this.rank != 0) { // wait for all the participants to send
				MPI.COMM_WORLD.Send(clusters, 0, this.capacity[rank], MPI.INT,
						0, 0);
			} else {
				int[] newCluster = new int[this.clusters.length];
				int offset = 0;
				for (int i = 1; i < this.procs; i++) {
					MPI.COMM_WORLD.Recv(newCluster, offset, this.capacity[i],
							MPI.INT, i, 0);
					offset += this.capacity[i];
				}
				// compare the two
				int i = 0;
				for (i = 0; i < newCluster.length; i++) {
					if (this.clusters[i] == newCluster[i]) {
						changed[0] = false;
						continue;
					} else {
						this.clusters = newCluster;
						this.recalculateSeed();
						changed[0] = true;
						break;
					}
				}

			}
			if (rank == 0) {
				for (int i = 1; i < procs; i++) {
					MPI.COMM_WORLD.Send(changed, 0, 1, MPI.BOOLEAN, i, i);
				}
			} else {
				MPI.COMM_WORLD.Recv(changed, 0, 1, MPI.BOOLEAN, 0, rank);
			}

		}
	}

	private void recalculateSeed() {
		double[] seedX = new double[this.clusterNumber];
		double[] seedY = new double[this.clusterNumber];
		int[] count = new int[this.clusterNumber];

		for (int i = 0; i < this.clusters.length; i++) {
			seedX[clusters[i]] += this.xPoint[i];
			seedY[clusters[i]] += this.yPoint[i];
			count[clusters[i]]++;
		}

		for (int i = 0; i < this.clusterNumber; i++) {
			seedX[i] /= count[i];
			seedY[i] /= count[i];
			this.seedX = seedX;
			this.seedY = seedY;
		}

	}

	public void printCluster() {
		try {
			BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(
					new FileOutputStream(new File("mpioutput.csv"))));
			for (int i = 0; i < xPoint.length; i++) {
				bw.write(xPoint[i] + "," + yPoint[i] + "," + clusters[i]+"\n");
			}
			bw.close();
		} catch (FileNotFoundException e) {
			e.printStackTrace();
			System.err.println("I/O Exception!");
		} catch (IOException e) {
			e.printStackTrace();
			System.err.println("I/O Exception!");
		}
	}

	private double distance(double x, double y, double xCenter, double yCenter) {
		return Math.sqrt((x - xCenter) * (x - xCenter) + (y - yCenter)
				* (y - yCenter));
	}

}
