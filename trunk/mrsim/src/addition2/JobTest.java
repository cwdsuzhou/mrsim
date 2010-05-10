package addition2;

import org.apache.log4j.Logger;

import gridsim.datagrid.File;
import hasim.HCounter;
import hasim.HJobTracker;
import hasim.HLogger;
import hasim.HTAG;
import hasim.HcopierTest;
import hasim.JobInfo;
import hasim.core.Datum;
import hasim.gui.HSimulator;
import hasim.gui.HUser;
import hasim.gui.JobTestInterface;
import eduni.simjava.Sim_entity;

public class JobTest implements JobTestInterface{
	/**
	 * Logger for this class
	 */
	private static final Logger logger = Logger.getLogger(JobTest.class);

	final String jobFile;
	final HSimulator sim;
	final int numTests;
	final String name;
	public JobTest(String name, HSimulator sim, 
			String jobFile, int numTests) {
		this.name=name;
		this.sim= sim;
		this.jobFile=jobFile;
		this.numTests=numTests;
		assert numTests>0;
		assert sim != null;
		
	}
	
	public void submit(Sim_entity user) {
		logger.info("submit using "+ user.get_name());
		HCounter mc=new HCounter();
		HCounter rc=new HCounter();
		HCounter jc=new HCounter();
		
		int numTests=5;
		for (int i = 0; i < numTests; i++) {
			logger.info("iteration = "+ i);
			JobInfo jobinfo=new JobInfo(jobFile, user);
			jobinfo.setReturnTag(HTAG.jobinfo_complete.id());
			
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
			
			logger.info("got job return");
			
			mc.addAll(jobinfo.getmCounter());
			rc.addAll(jobinfo.getrCounter());
			jc.addAll(jobinfo.getCounters());
		}
		logger.info("num of tests = "+ numTests);
		logger.info("Mappers:\n"+ mc.divide(numTests));
		logger.info("Reducers:\n"+ rc.divide(numTests));
		logger.info("Job:\n"+ jc.divide(numTests));
		
		logger.info("\n finish test");
	}

	


}
