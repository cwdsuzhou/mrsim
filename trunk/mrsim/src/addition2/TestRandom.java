package addition2;

import static java.lang.Math.pow;
import static java.lang.Math.exp;
import java.util.HashSet;
import java.util.Random;
import java.util.Set;

public class TestRandom {

	static Random rnd=new Random();

	
	public static int getSize(int groups, int sample){
		Set<Integer> set=new HashSet<Integer>(sample);
		for (int i = 0; i < sample; i++) {
			set.add(rnd.nextInt(groups));
		}
		return set.size();
		
	}
	
	public static double getSizeEquation(int groups, int sample){
		return groups * (1.0-exp(-((double)sample)/groups));
	}
	public static double getSizePowerSum(int groups, int sample){
		double a=1.0-1.0/groups;
		double n=(double)sample;
		return (1-pow(a, n))/(1-a);
	}
	
	public static double getSize(int groups, int sample, int times){
		double sum=0;
		for (int i = 0; i < times; i++) {
			sum+=getSize(groups, sample);
		}
		return sum/times;
	}
	public void runTest(int groups, int min, int max, int step, int times){
		
		for (int i = min; i < max; i+=step) {
			double size=getSize(groups, i, times);
			double sizeEq=getSizeEquation(groups, i);
			double sizePower=getSizePowerSum(groups, i);
			System.out.println(""+i+"\t"+size+"\t"+sizeEq+"\t"+sizePower);
		}
	}
	
	public static void main(String[] args) {
		TestRandom t=new TestRandom();
		t.runTest(100000, 10000, 500000, 10000, 10);
	}
}
