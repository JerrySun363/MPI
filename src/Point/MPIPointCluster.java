package Point;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.net.InetAddress;
import java.net.UnknownHostException;
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
	private int number;

	public static void main(String args[]) throws MPIException {
		if (args.length != 3) {
			System.out
					.println("Usage: MPIPointCluster <Input> <K> <PointNumber>");
			System.exit(-1);
		}

		MPI.Init(args);
		MPIPointCluster cluster = new MPIPointCluster(
				Integer.parseInt(args[1]), Integer.parseInt(args[2]));
		if (cluster.rank == 0) {
			cluster.readData(args[0]);
			cluster.initSeed();
		}
		// start to calculate time data
		long start = System.currentTimeMillis();
		cluster.init();
		cluster.iteration();
		// time ends here.
		System.out.println("Rank " + cluster.rank + ": "
				+ (System.currentTimeMillis() - start));
		MPI.Finalize();
		cluster.printCluster();

	}

	public MPIPointCluster(int k, int number) throws MPIException {
		this.rank = MPI.COMM_WORLD.Rank();
		this.procs = MPI.COMM_WORLD.Size();
		this.clusterNumber = k;
		this.number = number;
		this.xPoint = new double[number];
		this.yPoint = new double[number];
		this.seedX = new double[this.clusterNumber];
		this.seedY = new double[this.clusterNumber];
	}

	private void readData(String filename) {
		try {
			BufferedReader br = new BufferedReader(new FileReader(filename));
			String line = null;
			int count = 0;
			while ((line = br.readLine()) != null && count < this.number) {
				String[] coordinate = line.split(",");
				double x = Double.parseDouble(coordinate[0]);
				double y = Double.parseDouble(coordinate[1]);
				xPoint[count] = x;
				yPoint[count] = y;
				count++;
			}
			br.close();
		} catch (FileNotFoundException e) {
			System.out.println(filename + " does not exist!");
			System.exit(-1);
		} catch (IOException e) {
			System.out.println("I/O Exception while reading the data");
			System.exit(-1);
		}

	}

	private void initSeed() {

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
	public void init() throws MPIException {
		this.capacity = new int[this.procs];
		for (int i = 1; i < this.procs; i++) {
			this.capacity[i] = xPoint.length / (this.procs - 1)
					+ (i <= xPoint.length % (this.procs - 1) ? 1 : 0);
		}
		try {
			System.out.println(InetAddress.getLocalHost().getHostName());
		} catch (UnknownHostException e) {
			e.printStackTrace();
		}
		this.clusters = new int[xPoint.length];
		Arrays.fill(clusters, -1);
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

	public void iteration() throws MPIException {
		boolean[] changed = new boolean[1];
		changed[0] = true;
		int count = 0;
		while (changed[0]) {
			System.out.println("Iteration #" + count + " rank #" + this.rank);
			count++;
			MPI.COMM_WORLD.Bcast(seedX, 0, this.clusterNumber, MPI.DOUBLE, 0);
			MPI.COMM_WORLD.Bcast(seedY, 0, this.clusterNumber, MPI.DOUBLE, 0);
			// System.out.println("SeedX length " + seedX.length +" for rank "+
			// this.rank);
			// System.out.println("SeedX: " + Arrays.toString(this.seedX));

			for (int i = 0; i < this.capacity[rank]; i++) {
				double dis = Double.MAX_VALUE;
				for (int j = 0; j < seedX.length; j++) {
					double mydis = distance(xPoint[i], yPoint[i], seedX[j],
							seedY[j]);
					if (mydis < dis) {
						dis = mydis;
						this.clusters[i] = j;
					}
				}
				// System.out.println("The cluster it belongs to"+
				// this.clusters[i]);
			}
			// calculate distance
			if (this.rank != 0) { // wait for all the participants to send
				// System.out.println("Send back cluster!");
				MPI.COMM_WORLD.Send(clusters, 0, this.capacity[rank], MPI.INT,
						0, 0);
			} else {
				int[] newCluster = new int[this.clusters.length];
				int offset = 0;
				for (int i = 1; i < this.procs; i++) {
					// System.out.println("Offset is " + offset);
					MPI.COMM_WORLD.Recv(newCluster, offset, this.capacity[i],
							MPI.INT, i, 0);
					offset += this.capacity[i];
				}
				// System.out.println(Arrays.toString(newCluster));
				// System.out.println(Arrays.toString(this.clusters));

				// compare the two
				int i = 0;
				for (i = 0; i < newCluster.length; i++) {
					if (this.clusters[i] == newCluster[i]) {
						changed[0] = false;
					} else {
						this.clusters = newCluster;
						this.recalculateSeed();
						changed[0] = true;
						break;
					}
				}
				// //System.out.println("now status: "+changed[0]);

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
		// System.out.println("Data To Calculate Now:"+
		// Arrays.toString(this.xPoint));

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
		}

		this.seedX = seedX;
		this.seedY = seedY;
	}

	public void printCluster() {
		if (this.rank == 0) {
			try {
				BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(
						new FileOutputStream(new File("mpioutput.csv"))));
				for (int i = 0; i < xPoint.length; i++) {
					bw.write(xPoint[i] + "," + yPoint[i] + "," + clusters[i]
							+ "\n");
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
	}

	private double distance(double x, double y, double xCenter, double yCenter) {
		return Math.sqrt((x - xCenter) * (x - xCenter) + (y - yCenter)
				* (y - yCenter));
	}

}
