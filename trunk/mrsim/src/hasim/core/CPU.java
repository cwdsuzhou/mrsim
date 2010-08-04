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

import eduni.simjava.Sim_entity;
import eduni.simjava.Sim_event;
import eduni.simjava.Sim_stat;
import eduni.simjava.Sim_system;
import eduni.simjava.distributions.Sim_uniform_obj;

import gridsim.GridSim;
import gridsim.GridSimTags;
import hasim.CircularList;
import hasim.CTag;
import hasim.HCounter;
import hasim.HLogger;
import hasim.HLoggerInterface;
import hasim.HTAG;
import hasim.core.LocalMsg.OP_TYPE;
import hasim.gui.HMonitor;
import hasim.json.JsonCpu;
import hasim.json.JsonHardDisk;
import hasim.json.JsonJob;

public class CPU extends Sim_entity implements HLoggerInterface{
	/**
	 * Logger for this class
	 */
	private static final Logger logger = Logger.getLogger(CPU.class);
	private static double DELTA=1;

	

	public static double getDELTA() {
		return DELTA;
	}
	public static void setDELTA(double dELTA) {
		CPU.DELTA = dELTA;
	}

	private final int cores;
	private final HLogger hlog;

	final private  double speed;//MIPS
	final private HCounter counters;
	final HMonitor monitor;
//
	final Sim_stat stat;
	
	public void showMonitor(){
		monitor.setVisible(true);
	}

	LinkedList<LocalMsg> msgs=new LinkedList<LocalMsg>();

	private void initStat(){
		
		stat.add_measure("usage", Sim_stat.STATE_BASED, 0);
        stat.add_measure(Sim_stat.UTILISATION);

		set_stat(stat);
	}
	public CPU(String name,JsonCpu jsnCpu, HMonitor monitor) throws Exception{
		super(name);
		this.cores=jsnCpu.getCores();
		this.speed=jsnCpu.getSpeed();
		this.monitor=monitor;
		this.hlog=new HLogger(name);
		this.counters=new HCounter();
		
		this.stat=new Sim_stat();
		
		initStat();
		
	}
	
	public CPU(String name,String jsonCpu) throws Exception{
		this(name, JsonJob.read(jsonCpu, JsonCpu.class));
	}

	public CPU(String name,JsonCpu jsnCpu) throws Exception{
		this(name, jsnCpu, new HMonitor(name));
	}
	
	public CPU(String name,int numOfCores, double speed) throws Exception {
		super(name);
		this.cores=numOfCores;
		this.speed= speed;
		this.monitor=new HMonitor(name);
		this.hlog=new HLogger(name);
		this.counters=new HCounter();

		
		this.stat=new Sim_stat();
	
		initStat();


	}

	
	public void submit(LocalMsg msg){
		if( msg==null)return;
		sim_schedule(get_id(), 0.0, HTAG.engine_add.id(), msg);
		hlog.debug("submit "+msg);
//		logger.debug("submit "+ msg+", time "+ Sim_system.clock());
	}

	public void work(double size, int user,int returnTag, Object object){
		work(size, user, returnTag, object,1.0);//default priority =1
	}
	
	public void work(double size, int user,int returnTag, Object object, double priority){
		double totalTime = size  / speed;
		double deltaTime = DELTA * priority/ speed ;

		if(totalTime==0){
			sim_schedule(user, 0.0, returnTag,object);
			return;
		}
		
		LocalMsg msg=new LocalMsg(totalTime, deltaTime, user, returnTag, object,OP_TYPE.CPU_WORK);
		counters.inc(CTag.CPU_WORK, size);
		submit(msg);
	}

	


	@Override
	public void body() {


//		HashSet<Datum> runningJoblets=new HashSet<Datum>(cores);
		int tcounter=0;

		while (Sim_system.running()) {

			
//			
			int slots=cores;

			while (Sim_system.running()) {

				Sim_event ev=new Sim_event();
				sim_get_next(ev);

				int tag= ev.get_tag();
				if( tag == HTAG.END_OF_SIMULATION){
					logger.info(get_name()+" END_OF_SIMULATION "+ Sim_system.clock());
					hlog.info("END_OF_SIMULATION");
					break;
				}
//			monitor.log(get_name()+"-cores", Sim_system.clock(), ""+runningJoblets.size(), false);
//			monitor.log(get_name()+"-jobs", Sim_system.clock(), ""+jobs.size(), false);
			stat.update("usage", msgs.size(), Sim_system.clock());

				if( tag == HTAG.engine_add.id()){
					LocalMsg msg=(LocalMsg)ev.get_data();
					msgs.add(msg);

					while(slots >0 && msgs.size()>0){
						LocalMsg msgToSend=msgs.removeFirst();
						sim_schedule(get_id(), msgToSend.decTimesAndGet(), 
								HTAG.engine_check.id(), msgToSend);
						slots--;

					}
					continue;
				}
				if( tag == HTAG.engine_check.id()){

//					logger.info("data "+ ev.get_data()+" , time:"+ Sim_system.clock() );
					LocalMsg msg=(LocalMsg)ev.get_data();
					//logger.info("hasNext: "+ msg.hasNext());
					if(! msg.hasNext()){

						//notify
						if(msg.user != -1){
							sim_schedule(msg.user, 0.0, msg.returnTag,
									msg.object);
							hlog.debug("finish "+ msg);
//							logger.debug(msg+" , count: "+msg.getTimes() );
						}

						//take to new msg if available
						if(msgs.size()>0){
							LocalMsg msgToP=msgs.removeFirst();
							assert msgToP.hasNext();
							sim_schedule(get_id(), msgToP.decTimesAndGet(), 
									HTAG.engine_check.id(), msgToP);
						}else{
							slots++;
						}
					}else{
						//msg.hasNext = true

						if(msgs.size()>0){
							//switch to new msgToP
							msgs.add(msg);
							LocalMsg msgToP=msgs.removeFirst();
							assert msgToP.hasNext();
							sim_schedule(get_id(), msgToP.decTimesAndGet(), 
									HTAG.engine_check.id(), msgToP);

						}else{
							//keep processing same msg
							sim_schedule(get_id(), msg.decTimesAndGet(), 
									HTAG.engine_check.id(), msg);

						}
					}
					continue;
				}
			}

		
		}
		



	}
	
	@Override
	public String toString() {
		return "CPU: speed:"+speed+", cores:"+cores;
	}

	public HLogger getHlog() {
		// TODO Auto-generated method stub
		return hlog;
	}
	
	public void stopEntity() {
		sim_schedule(get_id(), 0.0, HTAG.END_OF_SIMULATION);
		hlog.info("counters: "+ counters);
		hlog.save();
	}
}



