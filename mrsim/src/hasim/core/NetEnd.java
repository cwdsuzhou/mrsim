package hasim.core;

import gridsim.GridSim;
import gridsim.GridSimTags;
import gridsim.IO_data;
import gridsim.ParameterException;
import gridsim.net.FIFOScheduler;
import gridsim.net.InfoPacket;
import gridsim.net.Link;
import gridsim.net.RIPRouter;
import gridsim.net.Router;
import gridsim.net.SimpleLink;
import gridsim.net.flow.FlowLink;
import hasim.HCounter;
import hasim.HJobTracker;
import hasim.HLogger;
import hasim.HLoggerInterface;
import hasim.HTAG;
import hasim.json.JsonConfig;
import hasim.json.JsonMachine;
import hasim.json.JsonRealRack;

import java.util.Arrays;
import java.util.Calendar;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Vector;


import org.apache.log4j.Logger;

import dfs.Pair;


import eduni.simjava.Sim_entity;
import eduni.simjava.Sim_event;
import eduni.simjava.Sim_system;

public class NetEnd extends GridSim implements HLoggerInterface{
	/**
	 * Logger for this class
	 */
	private static final Logger logger = Logger.getLogger(NetEnd.class);
	public final HLogger hlog;
	final public static int USER_NONE=-1; 
	//public static double delay=0.1;

	private static double DELTA_SIZE=10000;
	final private HCounter counters;
	/**
	 * in MBytes/sec
	 */
	public final double baudSpeed;
	


	public static double getDELTA_SIZE() {
		return DELTA_SIZE;
	}
	public static void setDELTA(double dELTA) {
		DELTA_SIZE = dELTA;
	}


//	public NetEnd(String name, double baud, double propDelay) throws Exception {
//		super(name, new FlowLink("link_"+name, baud, 
//				propDelay, Integer.MAX_VALUE));
//		
//		hlog=new HLogger(name);
//
//	}
	public NetEnd(String name, Link link) throws Exception {
		super(name, link);
		hlog=new HLogger(name);
		
		this.baudSpeed = link.getBaudRate()/8.0;	
		this.counters=new HCounter();

	}
	

	@Override
	public void body() {

		while(Sim_system.running()){
			
			Sim_event ev=new Sim_event();

			sim_get_next(ev);
			int tag = ev.get_tag();

			if(tag == HTAG.END_OF_SIMULATION){
				hlog.info("END OF SIMULATION");
				logger.info(get_name()+" end simulation at time "+ Sim_system.clock());
				break;
			}

			
			
			if(tag == HTAG.msg_send.id()){
				LocalMsgNet msg=(LocalMsgNet)ev.get_data();
				
				//logger.debug(get_name()+" receive id:"+ o.id()+ " from:"+ev.get_src()+" at time : "+Sim_system.clock());
				if( msg.user != USER_NONE)
					send(msg.user,0.0, msg.returnTag, msg.object);
				continue;
			}
			
			////////////////////new addition by suhel
			
			if(tag == HTAG.sim_msg_send.id()){
				HSimMsg msg=(HSimMsg)ev.get_data();
				double interval=msg.deltaSize/baudSpeed;
				int icounter=0;
				while(msg.hasNext()){
					IO_data data=new IO_data(null, (long)msg.decTimesAndGet() , msg.to);
					super.send(super.output, icounter*interval, 
							HTAG.sim_msg_receive.id(),data);
					icounter++;

				}
				IO_data data=new IO_data(msg, (long)1 , msg.to);
				super.send(super.output, icounter*interval, 
						HTAG.sim_msg_receive.id(),data);
				continue;
				//logger.debug(get_name()+" receive id:"+ o.id()+ " from:"+ev.get_src()+" at time : "+Sim_system.clock());
			}
			if(tag == HTAG.sim_msg_receive.id()){
				if(ev.get_data()==null)continue;
				
				HSimMsg msg=(HSimMsg)ev.get_data();
				
				//logger.debug(get_name()+" receive id:"+ o.id()+ " from:"+ev.get_src()+" at time : "+Sim_system.clock());
				if( msg.user != USER_NONE)
					sim_schedule(msg.user,0.0, msg.returnTag, msg.object);
				continue;
			}
			
			//////////////////end new addition by suhel
			
			  // handle a ping requests. You need to write the below code
            // for every class that extends from GridSim or GridSimCore.
            // Otherwise, the ping functionality is not working.
            if (ev.get_tag() ==  GridSimTags.INFOPKT_SUBMIT)
            {
                processPingRequest(ev);                
            }
		
			logger.debug(" another tag received "+ HTAG.get(tag));

		}
		
		shutdownUserEntity();
		terminateIOEntities();
	}
	
	
	 /**
     * Handles ping request
     * @param ev    a Sim_event object
     */
    private void processPingRequest(Sim_event ev)
    {
        InfoPacket pkt = (InfoPacket) ev.get_data();
        pkt.setTag(GridSimTags.INFOPKT_RETURN);
        pkt.setDestID( pkt.getSrcID() );

        // sends back to the sender
        super.send(super.output, GridSimTags.SCHEDULE_NOW,
                   GridSimTags.INFOPKT_RETURN,
                   new IO_data(pkt,pkt.getSize(),pkt.getSrcID()) );
    }
	
	
	//	public boolean sendData(String to, Netlet ntlt){
	//		return sendData(to, ntlt.size,ntlt);
	//	}

    @Deprecated
	public void msg(String tos, double size,
			int user, int returnTag, Object o, double delay){
		int to=GridSim.getEntityId(tos);
		msg(to,size,user,returnTag,o,delay);
	}
    
    @Deprecated
	public void msg(int to, double size,
			int user, int returnTag, Object o, double delay){
		LocalMsgNet netlet=new LocalMsgNet(size, get_id(), to, user, returnTag, o);
		msg(netlet,delay);		
	}

    @Deprecated
    public void msg( LocalMsgNet msg,double delay){
		IO_data data=new IO_data(msg, (long)msg.size , msg.to);
		super.send(super.output, delay, HTAG.msg_send.id(),data);
	}
	
    @Deprecated		
	public boolean simpleSend(NetEnd dist, double size, Datum datum, double delay, double speed){
		if(dist==null)return false;
		if( size==0 ){
			logger.info("send zero size ");
		}
		double time=delay + size / speed; 
//		logger.debug("time:"+ time +", clock:"+ Sim_system.clock());
		
		hlog.info("simpleSend "+ dist.get_name()+ ", size:"+ size+ ", data:"+datum);
		hlog.info("simple send schedule: "+ time);
		
		for (Sim_entity user : datum.getUsers()) {
			sim_schedule(user.get_id(), time, datum.getDoneTag() ,datum);
		}

		return true;
	}

	public void sim_msg(String tos, double size,
			int user, int returnTag, Object o){
		int to=GridSim.getEntityId(tos);
		sim_msg(to, size, user, returnTag, o, 0.0);
	}	
	public void sim_msg(int to, double size,
			int user, int returnTag, Object o, double delay){
		HSimMsg msg=new HSimMsg(get_id(),to, size, DELTA_SIZE, user, returnTag, o);
		sim_msg(msg,delay);		
	}
	public void sim_msg( HSimMsg msg,double delay){
		sim_schedule(get_id(), 0.0, HTAG.sim_msg_send.id(),msg);
	}
	
	@Override
	public String toString() {
		return "netend:"+get_name()+", baudeRate:"+getLink().getBaudRate();
	}
	public void stopEntity() {
		logger.info("stop entity");
		send(get_id(), 0, HTAG.END_OF_SIMULATION);
		hlog.infoCounter("counters", counters);
		hlog.save();
		
	}
	@Override
	public HLogger getHlog() {
		return hlog;
	}

	
}


