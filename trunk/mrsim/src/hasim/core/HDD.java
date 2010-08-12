package hasim.core;

import org.apache.log4j.Logger;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.Vector;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import javax.print.attribute.standard.MediaSize.Engineering;
import javax.swing.DebugGraphics;
import javax.xml.crypto.Data;

import eduni.simjava.Sim_entity;
import eduni.simjava.Sim_event;
import eduni.simjava.Sim_stat;
import eduni.simjava.Sim_system;

import gridsim.GridSim;
import gridsim.GridSimTags;
import gridsim.Stat;
import gridsim.datagrid.File;
import hasim.CircularList;
import hasim.CTag;
import hasim.HCounter;
import hasim.HLogger;
import hasim.HLoggerInterface;
import hasim.HTAG;
import hasim.core.LocalMsg.OP_TYPE;
import hasim.gui.HMonitor;
import hasim.json.JsonHardDisk;
import hasim.json.JsonJob;



public class HDD extends Sim_entity implements HLoggerInterface{
	/**
	 * Logger for this class
	 */
	private static final Logger logger = Logger.getLogger(HDD.class);
	HMonitor monitor;
	final HLogger hlog;
	double utilization;
	double percent=1.0;
	int numWrites=0;

	
	final public static int NONE=-10;
	Lock lock=new ReentrantLock(true);
	final private HCounter counters;
	private static double DELTA=1;

	public static double getDELTA() {
		return DELTA;
	}
	public static void setDELTA(double DELTA) {
		assert DELTA > 0;
		HDD.DELTA = DELTA;
	}
	
	static double adjustPercent(int currentWrites){
//		if(true)return 1.0;
		double min=3, max=60, slop=100;
		assert currentWrites >=0;
		
		if(currentWrites<min)
			return 1.0;
		else if(currentWrites < max)
			return 1.0-(currentWrites-min)/slop;
		return 1.0- (max-min)/slop;
	}

	//	List<Disk> disks=new ArrayList<Disk>();
	//CircularList<Disk> disks=new CircularList<Disk>();
	final double readSpeed;
	final double writeSpeed;
	//id, progress
	//Vector< Filelet> files=new Vector< Filelet>();
	//	CircularList<Datum> files=new CircularList<Datum>();
	//	LinkedList<LocalMsg> msgs=new LinkedList<LocalMsg>();
	final Sim_stat stat;

	private void initStat(){

		stat.add_measure("usage", Sim_stat.STATE_BASED, 0);
        stat.add_measure(Sim_stat.UTILISATION);

		set_stat(stat);
	}

	public HDD(String name, JsonHardDisk j, HMonitor monitor) throws Exception{
		super(name);
		this.monitor=monitor;
		this.hlog=new HLogger(name);
		this.readSpeed=j.getRead();
		this.writeSpeed=j.getWrite();
		this.counters=new HCounter();
		this.stat=new Sim_stat();
		initStat();
	}

	public HDD(String name, String jsonFile) throws Exception{
		this(name, JsonJob.read(jsonFile, JsonHardDisk.class));
	}

	public HDD(String name, JsonHardDisk jsonDisk) throws Exception{
		this(name, jsonDisk, new HMonitor(name));
	}



	public void read(double size, Sim_entity user,int returnTag, Object object){
		double totalTime = size  / readSpeed;
		double deltaTime = DELTA / readSpeed;

		//		assert totalTime>0;
		if(totalTime==0){
			user.sim_schedule(user.get_id(), 0.0, returnTag,object);
			return;
		}

		LocalMsg msg=new LocalMsg(totalTime, deltaTime, user.get_id(),
					returnTag, object, OP_TYPE.READ);
		msg.setTag(HTAG.hdd_read.id());
		
		
		submit(msg);

		counters.inc(CTag.HDD_READ, size);
	}
	public void write(double size, Sim_entity user,int returnTag, Object object){
		double totalTime = size  / writeSpeed;
		double deltaTime = DELTA / writeSpeed;

		if(totalTime==0){
			user.sim_schedule(user.get_id(), 0.0, returnTag,object);
			return;
		}
		assert size>0;

		LocalMsg msg=new LocalMsg(totalTime, deltaTime, user.get_id(),
				returnTag, object, OP_TYPE.WRITE);
		msg.setTag(HTAG.hdd_write.id());
		counters.inc(CTag.HDD_WRITE, size);


		submit(msg);
	}

	private void submit(LocalMsg msg){
		if( msg==null)return;
		assert msg.hasNext();

		sim_schedule(get_id(), 0.0, HTAG.engine_add.id(), msg);
		hlog.debug("submit "+ msg);

		//		logger.debug("submit "+ msg+", time "+ Sim_system.clock());
	}


	public HLogger getHlog() {
		return hlog;
	}
	@Override
	public void body() {

		//		int slots=maxSlots;

		boolean isIdle=true;
		CircularList<LocalMsg> msgs=new CircularList<LocalMsg>(1000);

		while (Sim_system.running()) {

			Sim_event ev=new Sim_event();
			sim_get_next(ev);

			int tag= ev.get_tag();
			
			if( tag == HTAG.END_OF_SIMULATION){
				logger.info(get_name()+" END_OF_SIMULATION "+ Sim_system.clock());
				hlog.info("END_OF_SIMULATION");
				break;
			}
			//			monitor.log(get_name()+"-read/write", Sim_system.clock(), ""+msgs.size(), false);
			//			stat.update("usage", msgs.size(), Sim_system.clock());

			if( tag == HTAG.engine_add.id()){
				LocalMsg msg=(LocalMsg)ev.get_data();
				msgs.add(msg);
				if(msg.opType==OP_TYPE.WRITE){
					numWrites++;
					percent=adjustPercent(numWrites);
//					logger.info("percent ="+ percent);
				}
				//hlog.info(HTAG.toString(tag)+ ", "+ msg);
				if(isIdle ){
					isIdle=false;
					sim_schedule(get_id(), 0.0, HTAG.engine_check.id);
					

				}
				continue;
			}
			if( tag == HTAG.engine_check.id()){
				assert msgs.size()>0;
				//				logger.info("data "+ ev.get_data()+" , time:"+ Sim_system.clock() );
				LocalMsg msg=msgs.next();

				sim_process(msg.decTimesAndGet()*percent);
				//logger.info("hasNext: "+ msg.hasNext());
				if(! msg.hasNext()){

					msgs.remove(msg);
					if(msg.opType==OP_TYPE.WRITE){
						numWrites--;
						percent=adjustPercent(numWrites);
						
//						logger.info("id=" + get_name()+ ",percent ="+ percent);
					}
					//notify
					if(msg.user != HDD.NONE){
						sim_schedule(msg.user, 0.0, msg.returnTag,
								msg.object);
						hlog.debug("finish "+ msg);
					}

				}
				if(msgs.size()>0)
					sim_schedule(get_id(), 0.0, HTAG.engine_check.id);
				else{
					isIdle=true;
					assert numWrites==0;
				}
				
				continue;
			}
		}




	}

	
	@Override
	public String toString() {
		return get_name();
	}

	public void stopEntity() {
		sim_schedule(get_id(), 0.0, HTAG.END_OF_SIMULATION);
		hlog.info("counters: "+ counters);
		hlog.save();
	}

}




