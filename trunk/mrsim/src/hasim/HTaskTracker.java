package hasim;

import org.apache.log4j.Logger;

import eduni.simjava.Sim_entity;
import eduni.simjava.Sim_event;
import eduni.simjava.Sim_system;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import gridsim.GridSim;
import gridsim.GridSimTags;
import hasim.core.CPU;
import hasim.core.Datum;
import hasim.core.HDD;
import hasim.core.NetEnd;
import hasim.gui.HMonitor;
import hasim.json.JsonMachine;

/**
 * Machine (Hardware)
 * @author hadoop
 *
 */
public class HTaskTracker implements HLoggerInterface{
	/**
	 * Logger for this class
	 */
	private static final Logger logger = Logger.getLogger(HTaskTracker.class);
	
	//public static Map<String, HTaskTracker> taskTrakers;

	//public HResourceStatus resStatus=new HResourceStatus();

	private HJobTracker jobTracker;
	
	public HJobTracker getJobTracker() {
		return jobTracker;
	}
	public void setJobTracker(HJobTracker jobTracker) {
		this.jobTracker = jobTracker;
	}
	
	public HMonitor monitor;
	
	public Lock lock=new ReentrantLock(true);
	
	public final JsonMachine jsonMachine;

	private final CPU cpu;
	public HMonitor getMonitor() {
		return monitor;
	}
	public void setMonitor(HMonitor monitor) {
		this.monitor = monitor;
	}
	public CPU getCpu() {
		return cpu;
	}
	public HDD getHdd() {
		return hdd;
	}
	public int getMaxMappers() {
		return maxMappers;
	}
	public int getMaxRedcuers() {
		return maxRedcuers;
	}
	private final NetEnd netend;
	private final HDD hdd;
	private final int maxMappers, maxRedcuers;
	final private HLogger hlog;
	//final Counter
	
	
	public HTaskTracker( JsonMachine jsonMachine, NetEnd netend) throws Exception{
		this(jsonMachine, netend, new HMonitor(jsonMachine.getHostName()));
		
	}
	public HTaskTracker( JsonMachine jsonMachine, NetEnd netend, HMonitor monitor) throws Exception{
		//super(jsonMachine.getHostName());
		assert monitor != null;

		this.cpu=new CPU("cpu_"+jsonMachine.getHostName() , jsonMachine.getCpu(), monitor);
		this.hdd=new HDD("hdd_"+jsonMachine.getHostName(), jsonMachine.getHardDisk(), monitor);
		this.netend=netend;
		this.jsonMachine=jsonMachine;
		
		
		this.maxMappers=jsonMachine.getMaxMapper();
		this.maxRedcuers=jsonMachine.getMaxReducer();
		
//		logger.debug("hostName: "+jsonMachine.getHostName()+"aMappers:"+ maxMappers+ ", aReducers:"+ maxRedcuers);
		
		this.monitor= monitor;
		hlog=new HLogger(jsonMachine.getHostName());
		
	}
	
//	@Override
//	public void body() {
//
//		while(Sim_system.running()){
//			
//			Sim_event ev=new Sim_event();
//			sim_get_next(ev);
//			int tag=ev.get_tag();
//			
//			if( tag== HTAG.END_OF_SIMULATION){
//				break;
//			}
//			
//			if( tag== HTAG.heartbeat.id()){
//				
//			}
//		}
//	}
	

	public String getHostName(){
		return jsonMachine.getHostName();
	}
	public int getaMapperSlots() {
		return maxMappers;
	}

	

	public int getaReducerSlots() {
		return maxRedcuers;
	}

	public String toString() {
		return jsonMachine.getHostName();
	}
	
	public String toStringInfo() {
		StringBuffer sb=new StringBuffer("tracker:"+getHostName());
		sb.append("\taMappers:"+ maxMappers+ "aReducers:"+ maxRedcuers);
		return sb.toString();
	}
	public HLogger getHlog() {
		// TODO Auto-generated method stub
		return hlog;
	}
	public NetEnd getNetend() {
		return netend;
	}
	
}
