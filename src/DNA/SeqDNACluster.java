/**
 * 
 */
package DNA;

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
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Random;

import Point.Point;

public class SeqDNACluster {
	
	class PopularBase implements Comparator<PopularBase>{
		char base;
		int times;
		public PopularBase(char base, int times) {
			this.base = base;
			this.times = times;
		}
		@Override
		public int compare(PopularBase o1, PopularBase o2) {
			// TODO Auto-generated method stub
			return o1.times-o2.times;
		}
	}
	
	
	protected ArrayList<String> DNAStrands;
	protected ArrayList<String> seeds;
	protected int clusterNumber;
	protected ArrayList<HashSet<String>> clusters;

	public void readData(String filename) {
		try {
			BufferedReader br = new BufferedReader(new FileReader(filename));
			String line = null;
			while ((line = br.readLine()) != null) {
				DNAStrands.add(line);
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
			int index = rand.nextInt(this.DNAStrands.size());
			this.seeds.add(DNAStrands.get(index));
			this.clusters.add(new HashSet<String>());
		}
	}

	protected int distance(String DNAStrand1, String DNAStrand2) {
		int len = DNAStrand1.length();
        int record[][] = new int[len+1][len+1];
        //initial state
        record[0][0] = 0;
        for (int i = 1; i <= len; i++) // need to begin form index 0 which means word1 has no character
            record[i][0] = i;
        for (int i = 1; i <= len; i++)
            record[0][i] = i;
        for (int i = 1; i <= len; i++) {
            for (int j = 1; j <= len; j++) {
                int temp = Math.min(record[i-1][j] + 1, record[i][j-1] + 1);
                record[i][j] = Math.min(record[i-1][j-1] + (DNAStrand1.charAt(i-1) == DNAStrand2.charAt(j-1) ? 0 : 1), temp);
            }
        }   
        return record[len][len];
	}

	public SeqDNACluster(int k) {
		this.DNAStrands = new ArrayList<String>();
		this.clusterNumber = k;
		this.seeds = new ArrayList<String>(k);
		this.clusters = new ArrayList<HashSet<String>>(k);
	}

	public static void main(String args[]) {
		if (args.length != 2) {
			System.out
					.println("Usage: Java SeqDNACluster <DataFileName> <ClusterNumber>");
		}

		SeqDNACluster spc = new SeqDNACluster(Integer.parseInt(args[1]));
		spc.readData(args[0]);
		spc.initSeed();
		long start = System.currentTimeMillis();
		spc.iteration();
		long time = System.currentTimeMillis() - start;
		System.out.println("Time passed: " + time);
		spc.printCluster();

	}

	private void recalculateSeed() {
		for (int i = 0; i < this.clusters.size(); i++) {
			StringBuilder newSeed = new StringBuilder();
			int size = this.clusters.get(i).size();
			int len = this.DNAStrands.get(0).length();
			for (int j = 0; j < len; j++) {
				HashMap<Character, Integer> record = new HashMap<Character, Integer>();
				for (String p : this.clusters.get(i)) {
					char temp = p.charAt(j);
					if (record.containsKey(temp)) {
						record.put(temp, record.get(temp)+1);
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
			
			this.seeds.set(i, newSeed.toString());
		}
	}

	public void iteration() {
		boolean changed = true;
		int count = 0;

		while (changed) {
			count++;
			System.out.println("Iteration " + count);

			// before iterate, initialize
			ArrayList<HashSet<String>> newClusters = new ArrayList<>();
			for (int i = 0; i < this.clusterNumber; i++) {
				newClusters.add(new HashSet<String>());
			}

			// calculate each point and put them into the cluster
			for (String dnaStrand : this.DNAStrands) {
				double distance = Integer.MAX_VALUE;
				int index = -1;
				for (int i = 0; i < this.seeds.size(); i++) {
					String seed = seeds.get(i);
					double dis = distance(dnaStrand, seed);
					if (dis < distance) {
						distance = dis;
						index = i;
					}
				}
				newClusters.get(index).add(dnaStrand);
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
					new FileOutputStream(new File("output.csv"))));
			for (int i = 0; i < this.clusters.size(); i++) {
				HashSet<String> dnaStrands = clusters.get(i);
				for (String p : dnaStrands) {
					bw.write(p + " ");
				}
				bw.write("\n");
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

