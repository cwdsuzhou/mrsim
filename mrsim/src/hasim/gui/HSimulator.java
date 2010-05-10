package hasim.gui;


import org.apache.log4j.Logger;

import eduni.simjava.Sim_exception;
import eduni.simjava.Sim_system;

import hasim.core.NetEnd;
import hasim.gui.HMonitor;
import hasim.gui.HMonitor.DebugMode;

import java.io.File;
import java.io.IOException;
import java.text.DateFormat;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import javax.swing.DropMode;
import javax.swing.SwingUtilities;

import gridsim.GridSim;
import gridsim.parallel.gui.JobTypePanel;
import hasim.HJobTracker;
import hasim.HLogger;
import hasim.HMapperStory;
import hasim.HMapperTask;
import hasim.HReducerStory;
import hasim.HTAG;
import hasim.HTopology;
import hasim.JobInfo;
import hasim.RF;

public class HSimulator extends Thread {
	/**
	 * Logger for this class
	 */
	private static final Logger logger = Logger.getLogger(HSimulator.class);

//	public static Map<String, HMonitor> monitors=new LinkedHashMap<String, HMonitor>();

	public HSimulator() {
	}
	
	HJobTracker jobTracker;
//	HTopology topology;
	
	public static boolean running(){
		return Sim_system.running();
	}
	
	
	public static void initSimulator(boolean flowType){
		
		Sim_system.initialise();

		HTopology.initGridSim(flowType);
		
        resultDir=RF.newResultDir("results");
        logger.info("creat new Result dir "+ resultDir);
        
	}
	
	public void submitJob(String jobfile, JobInfo jobInfo){
		if(isPaused()){
			resumeSimulation();
			assert jobInfo.getUser() != null;
			jobTracker.submitJob(jobfile, jobInfo);
			pauseSimulation();
		}else{
			jobTracker.submitJob(jobfile, jobInfo);
		}
	}
	public void submitJob(String jobfile){
		if(isPaused()){
			resumeSimulation();
			jobTracker.submitJob(jobfile);
			pauseSimulation();
		}else{
			jobTracker.submitJob(jobfile);
		}
	}
	public static void sleep(int md){
		try {
			Thread.sleep(md);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	public void stopSimulator(){
		
		if(isPaused()){
			resumeSimulation();
		}
		if(jobTracker != null){
			jobTracker.stopSimulation();
//			jobTracker.saveHlog();
		}
//		if(topology != null){
//			topology.stopSimulation();
//			topology.saveHLog();
//		}
//		
//		jobTracker=null;
//		topology=null;
		
	}
	
	public static String resultDir;
	
	public void startSimulator(){
		
		
		logger.info("Starting simulator version");
        try {
          
  		  Sim_system.set_trace_detail(false, false, false);
          Sim_system.set_report_detail(true, true);
         // Sim_system.set_termination_condition(Sim_system.TIME_ELAPSED, 2000, false);
         
//          Sim_system.set_termination_condition(Sim_system.EVENTS_COMPLETED,jobTracker.get_name(),
//          		HTAG.END_OF_SIMULATION,1,false);
          
          Sim_system.set_trace_level(200);
          logger.info("trace level"+Sim_system.get_trace_level());
          
          String graphFile=RF.get(resultDir, RF.graph);
          logger.info("graph file: "+ graphFile);
          Sim_system.generate_graphs(graphFile);

          HMonitor.setDebugMode(DebugMode.NONE);
          
          logger.info("going to call Sim_system.run()");
          
          Sim_system.run();
            
          logger.info("simulator stopped");
        }
        catch (Exception e)
        {
        	e.printStackTrace();
            throw new NullPointerException("start hsim() :" +
                    " Error - you haven't initialized hsim.");
        }
        
        logger.info("end simulation");
        
	}

	
	

	public static void main(String[] args) {
		HSimulator sim=new HSimulator();
		sim.main();
	}


	
	

	
	
	@Override
	public void run() {
		startSimulator();
		logger.info("get out of thread");
	}
	
	public  void main() {
		System.out.println("start");
		try {
			
//			if(simoTree != null){
//				simoTree.addRack("rack 01", taskTrackers);
//				simoTree.addTopology(getTopology());
//				}
			HSimulator.initSimulator(true);
			
			jobTracker= new HJobTracker("jobTracker",
				"data/json/rack_working.json", null);
			
			
			
			
			
			logger.info("jobTracker.init()");
			jobTracker.createEntities(resultDir);

			
			logger.info("Start hsim");
			
			
			//test
	         //Sim_system.generate_graphs(true);

			//logger.info(topology.getNetends().keySet());
			startSimulator();
			
			

			
			logger.info("simulation ended");
			
			
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	
	private static AtomicBoolean paused=new AtomicBoolean(false);
	public static boolean isPaused() {
		return paused.get();
	}


	public static void pauseSimulation() {
		assert paused.get() == false;
		sleep(300);
		Sim_system.pauseSimulation();
		paused.set(true);
	}
	public static void resumeSimulation() {
		assert paused.get() == true;
		Sim_system.resumeSimulation();
		sleep(300);
		paused.set(false);
	}
	
	public static void pauseResume() {
		if(isPaused()){
			resumeSimulation();
			logger.info("Sim_system.resumed");
		}else{
			pauseSimulation();
			logger.info("Sim_system.paused");
		}
	}
	

	
}
