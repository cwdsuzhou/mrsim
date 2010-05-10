package addition2;

import java.util.HashSet;
import java.util.Random;
import java.util.Set;

public class TestRandom {

	Random rnd=new Random();
	int groups;
	
	int getSize(int groups, int sample){
		Set<Integer> set=new HashSet<Integer>(sample);
		for (int i = 0; i < sample; i++) {
			set.add(rnd.nextInt(groups));
		}
		return set.size();
		
	}
	
	double getSize(int groups, int sample, int times){
		double sum=0;
		for (int i = 0; i < times; i++) {
			sum+=getSize(groups, sample);
		}
		return sum/times;
	}
	public void runTest(int groups, int min, int max, int step, int times){
		
		for (int i = min; i < max; i+=step) {
			double size=getSize(groups, i, times);
			System.out.println(""+i+"\t"+size);
		}
	}
	
	public static void main(String[] args) {
		TestRandom t=new TestRandom();
		t.runTest(100000, 10000, 1000000, 10000, 5);
	}
}
