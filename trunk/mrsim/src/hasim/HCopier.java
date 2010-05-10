package hasim;

import org.apache.log4j.Logger;
import org.codehaus.jackson.JsonToken;


import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;

import eduni.simjava.Sim_entity;
import eduni.simjava.Sim_event;
import eduni.simjava.Sim_system;
import gridsim.GridSim;
import hasim.CopyObject.Step;
import hasim.CopyObject.Type;
import hasim.core.CPU;
import hasim.core.Datum;
import hasim.core.HDD;
import hasim.core.HSimMsg;
import hasim.core.NetEnd;
import hasim.json.JsonJob;
import hasim.json.JsonRealRack;

public class HCopier extends Sim_entity implements HLoggerInterface{
	/**
	 * Logger for this class
	 */
	private static final Logger logger = Logger.getLogger(HCopier.class);


	final private HLogger hlog;
	final private Map<String, HTaskTracker> taskTrackers;
	
	public HCopier(String name, Map<String, HTaskTracker> taskTrackers) {
		super(name);
		hlog=new HLogger("hcopier");
		this.taskTrackers =taskTrackers;
	}
	
	public void hdfsReplicate(int replication, String from, double size, Sim_entity user,
			int returnTag, Object object){
		CircularList<String> cl=new CircularList<String>(taskTrackers.keySet());
		cl.remove(from);
		
		HDD localHdd=taskTrackers.get(from).getHdd();
		localHdd.write( replication* size, user, returnTag, object);
//		localHdd.read(size, HDD.NONE, HDD.NONE, object);
		
//		if(replication==1)
//			return;
//		
//		if(replication > taskTrackers.size()) 
//			return;
//		for (int i = 0; i < replication-1; i++) {
//			String to=cl.next();
//			copy(from, to, size	, null, returnTag, object, Type.mem_hard);
//			from=to;
//		}

		
	}
	public CopyObject copy(String from,String to, double size, Sim_entity user,
			int returnTag, Object object, Type type){
		assert taskTrackers.keySet().contains(from);
		assert taskTrackers.keySet().contains(to);
		
		CopyObject cpo=new CopyObject(size, type, from, to, user, returnTag, object);
		logger.debug("copy "+ cpo);
		copy(cpo);
		return cpo;
	}
	public HTaskTracker getTracker(String name){
		return taskTrackers.get(name);
	}
	public HDD getHDD(String name){
		if(getTracker(name) == null)return null;
		return getTracker(name).getHdd();
	}
	public CPU getCPU(String name){
		if(getTracker(name) == null)return null;
		return getTracker(name).getCpu();
	}
	public NetEnd getNetEHdd(String name){
		if(getTracker(name) == null)return null;
		return getTracker(name).getNetend();
	}
	private boolean copyIsValid(String from, String to){
		assert taskTrackers.keySet().contains(from);
		assert taskTrackers.keySet().contains(to);
		if( getHDD(from)==null || getHDD(to)==null ||
				getCPU(from)==null || getCPU(to)==null ||
				getNetEHdd(from)==null || getNetEHdd(to)==null){
			return false;
		}else
			return true;
	}

	public boolean copy(CopyObject cpo){
		assert cpo != null;
		if(! copyIsValid(cpo.from, cpo.to)){
			logger.error("copy is not valid "+ cpo);
			return false;
		}else{
			cpo.start_Time=Sim_system.clock();
			sim_schedule(get_id(), 0.0, HTAG.cp_add_object.id(), cpo);
		return true;
		}

	}
	@Override
	public HLogger getHlog() {
		return hlog;
	}

	@Override
	public void body() {

		while (Sim_system.running()) {

			Sim_event ev=new Sim_event();
			sim_get_next(ev);

			int tag= ev.get_tag();
			
			if( tag == HTAG.END_OF_SIMULATION){
				logger.info(get_name()+" END_OF_SIMULATION "+ Sim_system.clock());
				hlog.info("END OF SIMULATION");
				break;
			}

			if( tag == HTAG.cp_add_object.id()){
				CopyObject cpo=(CopyObject)ev.get_data();
				
				if(cpo.steps.size()==0){
					sendBackAck(cpo);
					continue;
				}
				
				if(cpo.steps.contains(Step.net))
					submitNet(cpo);
				
				if(cpo.steps.contains(Step.hard_from))
					submitHddFrom(cpo);
				
				if(cpo.steps.contains(Step.hard_to))
					submitHddTo(cpo);
			
				continue;
			}
			

			if( tag == HTAG.cp_net.id()){
				CopyObject cpo=(CopyObject)ev.get_data();
				
				cpo.getHlog().info("return net");
				hlog.info("return net "+ cpo);
				double speed=cpo.size/(Sim_system.clock()-cpo.start_Time);
				hlog.info("speed "+ speed);

				cpo.steps.remove(Step.net);
				if(cpo.steps.size()==0)
					sendBackAck(cpo);
				
//				logger.debug("cp_net "+ cpo+ ", "+Sim_system.clock());
			};
			
			if( tag == HTAG.cp_hard_from.id()){
				CopyObject cpo=(CopyObject)ev.get_data();
				cpo.getHlog().info("return hard_from");
				hlog.info("return hard_from "+ cpo );
				double speed=cpo.size/(Sim_system.clock()-cpo.start_Time);
				hlog.info("speed "+ speed);

				cpo.steps.remove(Step.hard_from);
				if(cpo.steps.size()==0)
					sendBackAck(cpo);
//				logger.debug("cp_hard_from "+ cpo + ", "+Sim_system.clock());

			}
			if( tag == HTAG.cp_hard_to.id()){
				CopyObject cpo=(CopyObject)ev.get_data();
				cpo.getHlog().info("return hard_to");
				hlog.info("return hard_to "+ cpo);
				double speed=cpo.size/(Sim_system.clock()-cpo.start_Time);
				hlog.info("speed "+ speed);

				cpo.steps.remove(Step.hard_to);
				if(cpo.steps.size()==0)
					sendBackAck(cpo);

//				logger.debug("cp_hard_to "+ cpo+ ", "+Sim_system.clock());
				
			}
			

		}

	}

	private void sendBackAck(CopyObject cpo){
//		cpo.getHlog().info("retrun all ");
//		hlog.info("return all "+ cpo);
		if(cpo.user !=null){
			
			sim_schedule(cpo.user.get_id(), 0.0, cpo.returnTag, cpo.object);
//			logger.info("cpo.id = "+cpo.id+" ,speed test "+ cpo.type+" ="+ speed);
		}

//		logger.debug("ack back "+ cpo+ ", "+Sim_system.clock());
//		logger.info("send ACK back "+ cpo.object+" , "+ Sim_system.clock());
		
	}
	private void submitNet(CopyObject cpo){
//		getNetEHdd(cpo.getFrom()).
//			sendData(getNetEHdd(cpo.getTo()), cpo.getSize(), netlet);
//		
		getNetEHdd(cpo.from).sim_msg(getNetEHdd(cpo.to).get_id(),
				cpo.size, get_id(),
				HTAG.cp_net.id(), cpo, 0.0);
		
		cpo.getHlog().info("submitNet");
		hlog.debug("submitNet "+ cpo);
	}
	private void submitHddFrom(CopyObject cpo){
		
		getHDD(cpo.from).read(cpo.size,this,
				HTAG.cp_hard_from.id() , cpo);
		cpo.getHlog().info("submitHddFrom");
		hlog.debug("submitHddFrom "+cpo);

	}
	private void submitHddTo(CopyObject cpo){
		
		getHDD(cpo.to).write(cpo.size,this, 
				HTAG.cp_hard_to.id(), cpo);
		cpo.getHlog().info("submitHddTo");
		hlog.debug("submitHddTo "+cpo);
	}
	
	public static void main(String[] args) throws Exception{
		
		//String[] ends = { "a", "b", "c", "d", "e","f","g" };
		String rackFile="data/json/testRack.json";
		NetEnd.setDELTA(10000);
		HDD.setDELTA(10000);
		
		JsonRealRack rack=JsonJob.read(rackFile, JsonRealRack.class);
		
		HTopology topo=new HTopology(rack);

		HTopology.initGridSim(rack.isFlowType());
		
//		logger.info(topo.rack);
		topo.createTopology();
		
		Map<String, HTaskTracker> trackers=HJobTracker.createTaskTrackers(rack, topo);
		logger.info("trackers size="+trackers.size());
		
		HCopier copier=new HCopier("copier", trackers);
		
		HcopierTest copierTest=new HcopierTest("test", copier, topo);
		
//		if(true)return;
		
//		NetEndTest test = new NetEndTest("test", topo);
		
        GridSim.startGridSimulation();
	}

}
