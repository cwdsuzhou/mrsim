package hasim;

import org.apache.log4j.Logger;

import hasim.core.Datum;
import hasim.json.JsonAlgorithm;
import hasim.json.JsonConfig;
import hasim.json.JsonJob;

import java.io.IOError;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

public class HJobStory {
	/**
	 * Logger for this class
	 */
	private static final Logger logger = Logger.getLogger(HJobStory.class);

	final JsonAlgorithm alg;
	final JsonJob job;
	
	List<Datum> spills=new ArrayList<Datum>();
	List<Datum> spillsJob=new ArrayList<Datum>();

	List<Datum> merges=new ArrayList<Datum>();

	List<Datum> mapperSpills=new ArrayList<Datum>();
	List<Datum> mapperOut=new ArrayList<Datum>();
	
	
	public HJobStory(String algFile, String jobFile) throws Exception {
		alg=JsonJob.read(algFile, JsonAlgorithm.class);
		job=JsonJob.read(jobFile, JsonJob.class);
		
		logger.info("read alg:"+ alg);
		logger.info("read job:"+job);
	}
	public HJobStory(JsonAlgorithm alg2, JsonJob job2) {
		this.alg=alg2;
		this.job=job2;
	}
	
	public static Datum generateCombineSpills(Datum in, JobInfo jobinfo){
		JsonJob jsnJob=jobinfo.getJob();
		JsonAlgorithm alg=jobinfo.getAlgorithm();
		
		double numOfGroups=alg.getCombineGroups();
		double itemsPerGroup=in.records/numOfGroups;
		
		double itemsOut = 0.0;//TODO other than 0.0
		double cost=alg.getCombineCost()*in.records;
		double size=(alg.getCombineOutAvRecordSize()+
				alg.getCombineOutAvRecordSize_add()* itemsPerGroup)*numOfGroups;
		return null;
	}
	public static List<Datum> generateSpills(Datum in, JobInfo jobinfo){
		JsonJob jsnJob=jobinfo.getJob();
		JsonAlgorithm alg=jobinfo.getAlgorithm();

		double sPercent=jsnJob.getIoSortSpillPercent();
		double rPercent=jsnJob.getIoSortRecordPercent();
		double av=alg.getMapOutAvRecordSize();
		
		double buffer_size=jsnJob.getIoSortMb() *1024 * 1024;

		double record_thrshld_r=buffer_size* rPercent* sPercent/16;
		
		double size_thrshld_s=buffer_size* sPercent* (1-rPercent);
		double record_thrshld_s=size_thrshld_s/av;
		
	
		double record_thrshld=record_thrshld_r>record_thrshld_s?
				record_thrshld_s:record_thrshld_r;
		double size_thrshld=record_thrshld * av;
		double numberOfSpills= in.records/record_thrshld;
		//logger.info("number of spills = "+numberOfSpills);
		
		int intSpills=(int)Math.floor(numberOfSpills);
		double remainSpill=numberOfSpills-intSpills;
		
		List<Datum> list=new ArrayList<Datum>();
		
		for (int i = 0; i < intSpills; i++) {
			Datum d=new Datum("spill_"+i,size_thrshld, record_thrshld);
			list.add(d);
		}
		{
			Datum last=new Datum("spill_"+list.size(),
					size_thrshld*remainSpill, record_thrshld);
			
			//list.add(0,last);
			list.add(last);
		}
		
		//logger.info("Map output bytes = "+ (records * av));
		
		return list;
	}
	
	private void mapStory(Datum in, JobInfo jobInfo) {
		
		
	}
	
	public static void main(String[] args) throws Exception{
		JsonAlgorithm alg=JsonJob.read("data/json/alg.json",
				JsonAlgorithm.class);
		JsonJob job=JsonJob.read("data/json/job.json", JsonJob.class);

		double rc=2499900.00;
		double sz=rc * 12;
		
		logger.info("mapoutsize:"+sz);
		
		Datum in=new Datum("in", sz ,  rc);
		
		HJobStory story=new HJobStory(alg,job);
		
	}
}

