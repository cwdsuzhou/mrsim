package hasim.gui;

import org.apache.log4j.Logger;

import java.io.File;
import java.io.ObjectInputStream.GetField;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import javax.swing.JOptionPane;

import eduni.simjava.Sim_entity;
import addition2.JobTest;
import hasim.CTag;
import hasim.HCounter;
import hasim.HJobTracker;
import hasim.HTAG;
import hasim.HcopierTest;
import hasim.JobInfo;
import hasim.RF;
import hasim.core.Datum;
import hasim.gui.HMonitor.DebugMode;
import hasim.json.JsonJob;
import hasim.json.JsonRealRack;

public class MRSimTest1 implements JobTestInterface {
	/**
	 * Logger for this class
	 */
	private static final Logger logger = Logger.getLogger(MRSimTest1.class);

	static HSimulator sim;
	List<JobInfo> jobs;
	List<JobInfo> resultJobs=new ArrayList<JobInfo>();


	List<String> fileNames(String dirName){
		List<String> result=new ArrayList<String>();
		File dir=new File(dirName);
		if( ! dir.exists() || dir.isFile()){
			logger.error(dirName + " does not exist or it is a file");
		}
		for (File file : dir.listFiles()) {
			result.add(file.getAbsolutePath());
//			break;//to be deleted later
		}
		Collections.sort(result);
		return result;
	}
	
	List<JobInfo> getJobs(List<String> fileNames, Sim_entity user, int returnTag){
		List<JobInfo> result=new ArrayList<JobInfo>();
		for (String fileName : fileNames) {
			JobInfo jobinfo=new JobInfo(fileName, user);
			jobinfo.setReturnTag(returnTag);
			result.add(jobinfo);
		}
		return result;
	}

	static void test_start()throws Exception{

		String rackDir="data/json/rack";
		rackDir = "/home/hadoop/django-projects/count/mrsim/rack";
		String rackFile=RF.firstFileInDir(rackDir);
		JsonRealRack jsonRack=JsonJob.read(rackFile, JsonRealRack.class);
		HSimulator.initSimulator(jsonRack.isFlowType());
		//copy rack file to the result dir
		RF.copy(rackFile, RF.get(HSimulator.resultDir, RF.jsonRack));
		sim = new HSimulator();
		sim.jobTracker=new HJobTracker("JobTracker", rackFile,null);
		HMonitor.setDebugMode(DebugMode.NONE);
		HMonitor.setSLEEP(600);

		sim.jobTracker.createEntities(HSimulator.resultDir);
		sim.start();//start simulation thread
		logger.info("sim started");
	}

	
	void bulkTest(){
		sim.jobTracker.getHUser().submitJobTest(this);

	}
	
	

	void testInit(Sim_entity user){

		logger.info("+++++++++++++++++++++++++++");
		String jobDir="data/json/jobs";
		jobDir="/home/hadoop/django-projects/count/mrsim/jobs";

//		System.out.println(Arrays.toString(fileNames(jobDir).toArray()));
		List<String> files = fileNames(jobDir);
		List<JobInfo> jobs=getJobs(files, 
				user, HTAG.jobinfo_complete.id);
		

		for (int i = 0; i < files.size(); i++) {
			String jobFile=files.get(i);
			JobInfo jobinfo = jobs.get(i);
		
			assert jobinfo.getUser() != null;
			try {
				logger.info("submit jobid =  " +jobinfo.getId()+ 
						", user ="+ jobinfo.getUser());

				sim.submitJob(jobFile, jobinfo);
			} catch (Exception e) {
				e.printStackTrace();
			}
			
			JobInfo jobinfo_return=(JobInfo)Datum.collectOne(
					user, HTAG.jobinfo_complete.id());
			assert jobinfo_return.getId() == jobinfo.getId();
			resultJobs.add(jobinfo_return);
//			
			logger.info("got job return");
 		}
		logger.info("jobtest created");
		
	}
	@Override
	public void submit(Sim_entity user) {

		testInit(user);
		printResult();
//		System.out.println(printAllJobInfoHeader());
//		System.out.println(printTimes());
//		sim.stopSimulator();
	}
	
	String printTimes(){
		
		StringBuffer sb=new StringBuffer("\nconf\trecords\ts-map\ts-reduce\ts-total");
		for (JobInfo jobinfo : resultJobs) {
			double records =jobinfo.getJob().getData().getRecords();

			double avM = jobinfo.getCounters().get(CTag.avMappersTime);
			double avR = jobinfo.getCounters().get(CTag.avReducersTime);
			double avJ = jobinfo.getCounters().get(CTag.JOB_TOTAL_TIME);
			
			String type="a";
			String jobName= jobinfo.getJob().getJobName();
			if(jobName.contains("-j"))
				type="j";
			else if(jobName.contains("-c"))
				type="c";
			sb.append("\n"+type+"\t"+records+"\t"+avM+"\t"+avR+"\t"+avJ);
 		}
		return sb.toString();
	}

	String printJobInfo(JobInfo jobinfo){
		StringBuffer sb=new StringBuffer();
		
		String type="a";
		String jobName= jobinfo.getJob().getJobName();
		if(jobName.contains("-j"))
			type="j";
		else if(jobName.contains("-c"))
			type="c";

		double records =jobinfo.getJob().getData().getRecords();

		double avM = jobinfo.getCounters().get(CTag.avMappersTime);
		double avR = jobinfo.getCounters().get(CTag.avReducersTime);
		double avJ = jobinfo.getCounters().get(CTag.JOB_TOTAL_TIME);
		
		double spilledM = jobinfo.getmCounter().get(CTag.SPILLED_RECORDS);
		double spilledR = jobinfo.getrCounter().get(CTag.SPILLED_RECORDS);
		double spilledJ = jobinfo.getCounters().get(CTag.SPILLED_RECORDS);
	
		sb.append("\n"+type+"\t"+records+"\t"+
				avM+"\t"+avR+"\t"+avJ+"\t"+
				spilledM+"\t"+spilledR+"\t"+spilledJ
				);
		return sb.toString();
	}

	String printAllJobInfoHeader() {
		String result = "\nConf\tRecords" +
				"\tS-M-FBR\tS-M-FBW\tS-M-Spilled\tS-M-Time" +
				"\tS-R-FBR\tS-R-FBW\tS-R-Spilled\tS-R-Time" +
				"\tS-J-FBR\tS-J-FBW\tS-J-Spilled\tS-J-Time" +
				"\tS-shuffle\tS-C-input\tS-C-output\tS-R-input";
		return result;
	}
	String printAllJobInfo(JobInfo jobinfo){
		StringBuffer sb=new StringBuffer();
		HCounter c=jobinfo.getCounters();
		HCounter m=jobinfo.getmCounter();
		HCounter r=jobinfo.getrCounter();
		
		String conf="a";
		String jobName= jobinfo.getJob().getJobName();
		if(jobName.contains("j"))
			conf="j";
		else if(jobName.contains("c"))
			conf="c";

		double records =jobinfo.getJob().getData().getRecords();

		double m_fbr= m.get(CTag.FILE_BYTES_READ);
		double m_fbw=m.get(CTag.FILE_BYTES_WRITTEN);
		double m_spilled=m.get(CTag.SPILLED_RECORDS);
		double m_time= c.get(CTag.avMappersTime);

		double r_fbr= r.get(CTag.FILE_BYTES_READ);
		double r_fbw=r.get(CTag.FILE_BYTES_WRITTEN);
		double r_spilled=r.get(CTag.SPILLED_RECORDS);
		double r_time= c.get(CTag.avReducersTime);
		
		double j_fbr= c.get(CTag.FILE_BYTES_READ);
		double j_fbw=c.get(CTag.FILE_BYTES_WRITTEN);
		double j_spilled=c.get(CTag.SPILLED_RECORDS);
		double j_time= c.get(CTag.JOB_TOTAL_TIME);
		
		double shuffle= c.get(CTag.SHUFFLE);
		double c_input=c.get(CTag.COMBINE_INPUT_RECORDS);
		double c_output= c.get(CTag.COMBINE_OUTPUT_RECORDS);
		double r_input= c.get(CTag.REDUCE_INPUT_RECORDS);
		
		String out= conf+
					"\t"+records+
					
					"\t"+m_fbr+
					"\t"+m_fbw+
					"\t"+m_spilled+
					"\t"+m_time+
					
					"\t"+r_fbr+
					"\t"+r_fbw+
					"\t"+r_spilled+
					"\t"+r_time+
					
					"\t"+j_fbr+
					"\t"+j_fbw+
					"\t"+j_spilled+
					"\t"+j_time+
					
					"\t"+shuffle+
					"\t"+c_input+
					"\t"+c_output+
					"\t"+r_input;
		sb.append(out);
		return sb.toString();
	}
	
	void printResult(){
		assert resultJobs.size()>0;
		
		logger.info("++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++");
		
		System.out.println(printAllJobInfoHeader());
		
		for (JobInfo jobinfo : resultJobs) {
			System.out.println(printAllJobInfo(jobinfo));
		}
		
		System.out.println("\n-------------------+++++++++++++++++++++++++++-------------------");
	}
	
	public static void main(String[] args) throws Exception {
		test_start();
		HUser user=sim.jobTracker.getHUser();
		assert user != null;
		

		MRSimTest1 mrsim=new MRSimTest1();
//		mrsim.submitJob();
//		mrsim.submitJobWithUser(user);
		try {
			Thread.sleep(2000);
		} catch (Exception e) {
			// TODO: handle exception
		}
		user.submitJobTest(mrsim);
		
		
		

//		user.stopSimulation();
		logger.info("done");
	}

	
}


