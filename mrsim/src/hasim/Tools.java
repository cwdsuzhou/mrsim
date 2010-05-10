package hasim;

import org.apache.log4j.Logger;

import dfs.MapBuffer;
import eduni.simjava.distributions.Sim_normal_obj;
import eduni.simjava.distributions.Sim_random_obj;
import eduni.simjava.distributions.Sim_uniform_obj;


import hasim.json.JsonJob;

import java.io.File;
import java.text.DateFormat;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.Formatter;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.atomic.AtomicLong;

public class Tools {
	/**
	 * Logger for this class
	 */
	private static final Logger logger = Logger.getLogger(Tools.class);
	static private DecimalFormat format = new DecimalFormat(); 

	static{
		
		format.setDecimalSeparatorAlwaysShown(true);
		format.setGroupingSize(3);
		format.setMaximumFractionDigits(2);
		format.setMinimumFractionDigits(2);
		format.setMinimumIntegerDigits(10);
	}
	public static String format(double d){
		return format.format(d);
	}
	public static List<String> scatter(Map<String, Integer> map) {
		logger.debug(map);

		List<String> result=new ArrayList<String>();
		for (Map.Entry<String, Integer> e : map.entrySet()) {
			for (int i = 0; i < e.getValue(); i++) {
				result.add(e.getKey());
			}
		}

		//Collections.shuffle(result);
		return result;
	}
	
	private static AtomicLong seed=new AtomicLong();
	//int reducerInputGroups, int numReducers
	
	
	public static List<Double> getAllFractions(int numOfReducers,
			double mean, double variance){
		
		List<Double> result=new ArrayList<Double>();
		if(numOfReducers==1){
			result.add(mean);
			return result;
		}

		Sim_normal_obj norm=new Sim_normal_obj("normal",
				mean, variance, seed.incrementAndGet());
		
		double sum=0;
		for (int i = 0; i < numOfReducers-1; i++) {
			double sample=Math.abs(norm.sample());
			result.add(sample);
			sum+= sample;
		}
		while(sum> numOfReducers*mean){
			double first=result.remove(0);
			sum-=first;
			double sample=Math.abs(norm.sample());
			result.add(sample);
			sum+=sample;
		}
		
		double remain=numOfReducers*mean-sum;
		assert remain >0;
		result.add(rnd.nextInt(numOfReducers-1), remain);
		
		return result;
	}
	public static List<Double> getFractions(int numOfReducers){
		Sim_random_obj sro=new Sim_random_obj("test");
		
		
		int numSamples=100;
		double mean=10;
		double dev=1;
		Sim_normal_obj uni=new Sim_normal_obj("uni", mean, dev);
		
		Random rnd=new Random();
		List<Double> list=new ArrayList<Double>(numSamples);
		CircularList<Double> cl=new CircularList<Double>(numOfReducers);

		for (int i = 0; i < numSamples	; i++) {
			double d=uni.sample();
			cl.add(d);
			System.out.println(""+format(d));
		}
		for (int i = 0; i < numSamples; i++) {
			double sample=rnd.nextInt(numOfReducers);
			sample= rnd.nextGaussian();
			double sum=0;
			for (int j = 0; j < numOfReducers; j++) {
				
			}
			System.out.println("\t"+sample);
//			double sample=uni.sample();
			mean+=sample;
			list.add(sample);
		}
		mean = mean/numSamples;
		logger.info("mean = "+ mean);
		return null;
	}
	
	
	public static String getGeneratedFileName(String dirName){
        File gfile=new File(dirName);
        String[] filenames=gfile.list();
        Arrays.sort(filenames);        
        return filenames[filenames.length-1];
	}
	
	public static Random rnd=new Random();
	
	
	public static void main(String[] args) {
//		getFractions(6);
		for (int i = 0; i < 100; i++) {
			List<Double> fractions=getAllFractions(4, 1, 1);
			
			double sum=0;
			for (Double d : fractions) {
				sum+=d;
			}
			assert Math.abs(sum-4) < 1e-5;
			System.out.println(fractions);
		}
	}


}
