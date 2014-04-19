package dyc.tools;

import java.util.Random;

public class RowGenerator {
	public static void main(String args[]) {
		int lines = 1000;
		int maxValue = 100; 
		Random r = new Random();
		for(int i = 0; i < lines; i++) {
			System.out.format("%d %d\n", r.nextInt(maxValue), r.nextInt(maxValue));
		}
		
	}
}
