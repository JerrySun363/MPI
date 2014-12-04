package DNA;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Random;

public class GenerateDNAStrand {
	public static void main(String args[]) {
		if (args.length != 3) {
			System.out
					.println("Usage: GenerateDNAStrand <number> <DNALength> <FileName>");
			System.exit(-1);
		}
		int number = Integer.parseInt(args[0]);
		int length = Integer.parseInt(args[1]);
		char[] seg = { 'A', 'C', 'G', 'T' };
		try {
			BufferedWriter bf = new BufferedWriter(new FileWriter(new File(
					args[2])));
			char[] chars = new char[length];
			Random rand = new Random(System.currentTimeMillis());
			for (int i = 0; i < number; i++) {
				for (int j = 0; j < length; j++) {
					chars[j] = seg[rand.nextInt(4)];
				}
				bf.write(new String(chars) + "\n");
			}
			bf.close();
		} catch (IOException e) {
			e.printStackTrace();
		}

	}
}
