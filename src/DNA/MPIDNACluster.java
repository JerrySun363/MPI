package DNA;

/**
 * 
 */

/**
 * @author Nicolas_Yu
 *
 */
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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Random;

import mpi.*;

public class MPIDNACluster {

	private int rank;
	private int procs;

	// each number represents the cluster it belongs to.
	private int[] clusters;
	private int[] capacity;
	private int DNALength;
	private int DNANumber;

	private char[][] DNAStrands;
	private char[][] seeds;
	private int clusterNumber;
	private String output = "MPIClusterOutput.csv";

	public static void main(String args[]) throws MPIException {
		if (args.length != 5) {
			System.out
			.println("Usage: MPIDNACluster <DataFileName> <ClusterNumber> <DNALength> <DNANumber> <Output>");
			System.exit(-1);
		}

		MPI.Init(args);
		MPIDNACluster cluster = new MPIDNACluster(Integer.parseInt(args[1]),
				Integer.parseInt(args[2]), Integer.parseInt(args[3]));
		cluster.output = args[4];

		if (cluster.rank == 0) {
			cluster.readData(args[0]);
			cluster.initSeed();
		}
		// start to calculate time data
		long start = System.currentTimeMillis();
		cluster.init();
		cluster.iteration();
		// time ends here.
		System.out.println("Rank " + cluster.rank + ": It uses "
				+ (System.currentTimeMillis() - start)
				+ " milliseconds to finish");
		MPI.Finalize();
		cluster.printCluster();
	}

	/**
	 * Constructor
	 * @param k
	 * @param len
	 * @param num
	 * @throws MPIException
	 */
	public MPIDNACluster(int k, int len, int num) throws MPIException {
		this.rank = MPI.COMM_WORLD.Rank();
		this.procs = MPI.COMM_WORLD.Size();
		this.clusterNumber = k;
		this.DNALength = len;
		this.DNANumber = num;

		this.DNAStrands = new char[this.DNANumber][this.DNALength];
		this.seeds = new char[this.clusterNumber][this.DNALength];
	}

	/**
	 * read data from file 
	 * @param filename
	 */
	private void readData(String filename) {
		ArrayList<String> dnaStrands = new ArrayList<String>();
		try {
			BufferedReader br = new BufferedReader(new FileReader(filename));
			String line = null;
			while ((line = br.readLine()) != null) {
				dnaStrands.add(line);
			}
			br.close();
		} catch (FileNotFoundException e) {
			System.out.println(filename + " does not exist!");
			System.exit(-1);
		} catch (IOException e) {
			System.out.println("I/O Exception while reading the data");
			System.exit(-1);
		}

		for (int i = 0; i < this.DNAStrands.length; i++) {
			this.DNAStrands[i] = dnaStrands.get(i).toCharArray();
		}
	}

	/**
	 * initialize seeds randomly 
	 */
	private void initSeed() {

		Random rand = new Random();
		for (int i = 0; i < this.clusterNumber; i++) {
			int index = rand.nextInt(this.DNAStrands.length);
			for (int j = 0; j < this.DNALength; j++) {
				seeds[i][j] = this.DNAStrands[index][j];
			}
		}
	}

	/**
	 * Send data to each process
	 */
	public void init() throws MPIException {
		this.clusters = new int[DNAStrands.length];
		Arrays.fill(clusters, -1);

		this.capacity = new int[this.procs];
		for (int i = 1; i < this.procs; i++) {
			this.capacity[i] = DNAStrands.length / (this.procs - 1)
					+ (i <= DNAStrands.length % (this.procs - 1) ? 1 : 0);
		}

		try {
			System.out.println("Host: " + InetAddress.getLocalHost().getHostName());
		} catch (UnknownHostException e) {
			e.printStackTrace();
		}

		if (rank == 0) {// master
			int offset = 0;
			for (int i = 1; i < this.procs; i++) {
				for (int j = 0; j < capacity[i]; j++)
					MPI.COMM_WORLD.Send(DNAStrands[offset + j], 0,
							this.DNALength, MPI.CHAR, i, i);

				MPI.COMM_WORLD.Send(clusters, offset, this.capacity[i],
						MPI.INT, i, i);

				offset += this.capacity[i];
			}
		} else {
			for (int i = 0; i < capacity[this.rank]; i++)
				MPI.COMM_WORLD.Recv(DNAStrands[i], 0, this.DNALength, MPI.CHAR,
						0, rank);

			MPI.COMM_WORLD.Recv(clusters, 0, this.capacity[rank], MPI.INT, 0,
					rank);
		}
	}

	/**
	 * iterations for K-means until converge
	 * @throws MPIException
	 */
	public void iteration() throws MPIException {
		boolean[] changed = new boolean[1];
		changed[0] = true;
		int count = 0;
		while (changed[0]) {
			System.out.println("Iteration #" + count + " rank #" + this.rank);
			count++;
			for (int i = 0; i < this.clusterNumber; i++) {
				MPI.COMM_WORLD.Bcast(seeds[i], 0, this.DNALength, MPI.CHAR, 0); // TODO
			}

			// reassign the class
			for (int i = 0; i < this.capacity[rank]; i++) {
				int dis = Integer.MAX_VALUE;
				for (int j = 0; j < seeds.length; j++) {

					int mydis = distance(DNAStrands[i], seeds[j]);
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
				// check whether the cluster is fixed
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

	/**
	 * update seeds after one iteration
	 */
	private void recalculateSeed() {

		ArrayList<ArrayList<String>> clusterStrings = new ArrayList<ArrayList<String>>();
		for (int i = 0; i < this.clusterNumber; i++) {
			clusterStrings.add(new ArrayList<String>());
		}

		for (int i = 0; i < this.clusters.length; i++) {
			clusterStrings.get(clusters[i]).add(new String(DNAStrands[i]));
		}

		for (int i = 0; i < this.clusterNumber; i++) {
			StringBuilder newSeed = new StringBuilder();
			for (int j = 0; j < this.DNALength; j++) {
				HashMap<Character, Integer> record = new HashMap<Character, Integer>();
				for (String tempDNA : clusterStrings.get(i)) {
					char temp = tempDNA.charAt(j);
					if (record.containsKey(temp)) {
						record.put(temp, record.get(temp) + 1);
					} else {
						record.put(temp, 1);
					}
				}
				char choiceBase = '\0';
				int max = 0;
				for (char temp : record.keySet()) {
					if (record.get(temp) > max) {
						choiceBase = temp;
						max = record.get(temp);
					}
				}
				newSeed.append(choiceBase);
			}
			seeds[i] = newSeed.toString().toCharArray();
		}
	}

	public void printCluster() {
		if (this.rank == 0) {
			try {
				BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(
						new FileOutputStream(new File(this.output))));
				for (int i = 0; i < DNAStrands.length; i++) {
					bw.write(new String(DNAStrands[i]) + " belongs to "
							+ " cluster " + clusters[i]  + "\n");
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

	/**
	 * calculate the edit distance between two DNA
	 * by counting the minimum number of operations(Insertion, Deletion, Substitution) 
	 * required to transform one string into the other. 
	 * @param DNAStrand1
	 * @param DNAStrand2
	 * @return
	 */
	private int distance(char[] DNAStrand1, char[] DNAStrand2) {
		int len = DNAStrand1.length;
		int record[][] = new int[len + 1][len + 1];
		// initial state
		record[0][0] = 0;
		for (int i = 1; i <= len; i++)
			// need to begin form index 0 which means word1 has no character
			record[i][0] = i;
		for (int i = 1; i <= len; i++)
			record[0][i] = i;
		for (int i = 1; i <= len; i++) {
			for (int j = 1; j <= len; j++) {
				int temp = Math.min(record[i - 1][j] + 1, record[i][j - 1] + 1);
				record[i][j] = Math.min(record[i - 1][j - 1]
						+ (DNAStrand1[i - 1] == DNAStrand2[j - 1] ? 0 : 1),
						temp);
			}
		}
		return record[len][len];
	}

}
