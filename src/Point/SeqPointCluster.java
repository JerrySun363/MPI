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
import java.util.HashSet;
import java.util.Random;

public class SeqPointCluster {
	private ArrayList<Point> points;
	private ArrayList<Point> seeds;
	private int clusterNumber;
	private ArrayList<HashSet<Point>> clusters;
	private String outputFile = "SeqPointCluster.csv";

	public void readData(String filename) {
		try {
			BufferedReader br = new BufferedReader(new FileReader(filename));
			String line = null;
			while ((line = br.readLine()) != null) {
				String[] coordinate = line.split(",");
				double x = Double.parseDouble(coordinate[0]);
				double y = Double.parseDouble(coordinate[1]);
				points.add(new Point(x, y));
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

	public void initSeed() {
		Random rand = new Random();
		for (int i = 0; i < this.clusterNumber; i++) {
			int index = rand.nextInt(this.points.size());
			this.seeds.add(points.get(index));
			this.clusters.add(new HashSet<Point>());
		}
	}

	private double distance(Point p1, Point p2) {
		return Math.sqrt((p1.x - p2.x) * (p1.x - p2.x) + (p1.y - p2.y)
				* (p1.y - p2.y));
	}

	public SeqPointCluster(int k) {
		this.points = new ArrayList<Point>();
		this.clusterNumber = k;
		this.seeds = new ArrayList<Point>(k);
		this.clusters = new ArrayList<HashSet<Point>>(k);
	}

	public static void main(String args[]) {
		if (args.length != 3) {
			System.out
					.println("Usage: Java SeqPointCluster <InputFileName> <ClusterNumber> <OutputFileName>");
		}

		SeqPointCluster spc = new SeqPointCluster(Integer.parseInt(args[1]));
		spc.readData(args[0]);
		spc.initSeed();
		spc.outputFile = args[2];
		long start = System.currentTimeMillis();
		spc.iteration();
		long time = System.currentTimeMillis() - start;
		System.out.println("Time passed: " + time);
		spc.printCluster();

	}

	private void recalculateSeed() {
		for (int i = 0; i < this.clusters.size(); i++) {
			double x = 0, y = 0;
			int size = this.clusters.get(i).size();
			for (Point p : this.clusters.get(i)) {
				x += p.x;
				y += p.y;
			}
			this.seeds.set(i, new Point(x / size, y / size));
		}
	}

	public void iteration() {
		boolean changed = true;
		int count = 0;

		while (changed) {
			count++;
			System.out.println("Iteration " + count);

			// before iterate, initialize
			ArrayList<HashSet<Point>> newClusters = new ArrayList<>();
			for (int i = 0; i < this.clusterNumber; i++) {
				newClusters.add(new HashSet<Point>());
			}

			// calculate each point and put them into the cluster
			for (Point point : this.points) {
				double distance = Double.MAX_VALUE;
				int index = -1;
				for (int i = 0; i < this.seeds.size(); i++) {
					Point seed = seeds.get(i);
					double dis = distance(point, seed);
					if (dis < distance) {
						distance = dis;
						index = i;
					}
				}
				newClusters.get(index).add(point);
			}

			// compare whether each cluster has changed or not

			for (int i = 0; i < this.clusters.size(); i++) {
				if (this.clusters.get(i).equals(newClusters.get(i))) {
					changed = false;
					continue;
				} else {
					this.clusters = newClusters;
					this.recalculateSeed();
					changed = true;
					break;
				}
			}

		}

	}

	public void printCluster() {
		try {
			BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(
					new FileOutputStream(new File(outputFile))));
			for (int i = 0; i < this.clusters.size(); i++) {
				HashSet<Point> points = clusters.get(i);
				for (Point p : points) {
					bw.write("Point: " + p.x + "," + p.y + " belongs to " 
							+ " cluster " + i + "\n");
				}
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
