package hasim;

import dfs.Pair;
import eduni.simjava.Sim_entity;
import eduni.simjava.Sim_predicate;
import eduni.simjava.Sim_system;
import gridsim.GridSim;
import gridsim.GridSimTags;
import gridsim.IO_data;
import gridsim.ParameterException;
import gridsim.net.FIFOScheduler;
import gridsim.net.Link;
import gridsim.net.RIPRouter;
import gridsim.net.Router;
import gridsim.net.SimpleLink;
import gridsim.net.flow.FlowLink;
import gridsim.net.flow.FlowRouter;
import hasim.core.Datum;
import hasim.core.LocalMsg;
import hasim.core.LocalMsgNet;
import hasim.core.NetEnd;
import hasim.json.JsonConfig;
import hasim.json.JsonJob;
import hasim.json.JsonMachine;
import hasim.json.JsonRealRack;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.sql.rowset.Predicate;

import org.apache.log4j.Logger;

public class HTopology implements HLoggerInterface{

	public static Set<Link> links = new LinkedHashSet<Link>();

	// public static double delay=0.1;

	/**
	 * Logger for this class
	 */
	private static final Logger logger = Logger.getLogger(HTopology.class);
	
	private Map<String, NetEnd> netends = new LinkedHashMap<String, NetEnd>();

	Set<Router> routers = new LinkedHashSet<Router>();

	HLogger hlog=new HLogger();
	

	final private JsonRealRack rack;
	final private boolean flowType;
	
	public HTopology(String rackFile){
		this(JsonJob.read(rackFile, JsonRealRack.class));
	}

	public HTopology(JsonRealRack rack){
		this.rack=rack;
		this.flowType=rack.isFlowType();
	}

	public HTopology(List<String> sEnds, double baudRate, double propDelay,
			int maxIM, boolean flowType){
		this.flowType=flowType;
		rack=new JsonRealRack();
		rack.setRouter("router_1");
		
		rack.setPropDelay(propDelay);
		rack.setMaxIM(maxIM);
		
		List<JsonMachine> machines=new ArrayList<JsonMachine>();
		for (String mName : sEnds) {
			JsonMachine machine=new JsonMachine();
			machine.setHostName(mName);
			machine.setBaudRate(baudRate);
			machines.add(machine);
		}
		
		rack.setMachines(machines);
	}
	public HTopology(String[] sEnds, double baudRate, double propDelay,
			int maxIM, boolean isFlowType){
	
		this(Arrays.asList(sEnds), baudRate, propDelay, maxIM, isFlowType);
	}
	
	public NetEnd getNetEnd(String name){
		return getNetends().get(name);
	}
//	public void initTopology(String rackFile, boolean isFlowType)throws Exception{
//		JsonRealRack rack=JsonJob.read(rackFile, JsonRealRack.class);
//		initTopology(rack, isFlowType);
//	}
	
	public static void initGridSim(boolean isFlowType){
		Calendar calendar = Calendar.getInstance();
		boolean trace_flag = false;
		
		if(isFlowType){
            GridSim.initNetworkType(GridSimTags.NET_FLOW_LEVEL);
            //trace_flag =true;
		}
		// Initialize the GridSim package
		logger.info("Initializing GridSim package");
		GridSim.init(8, calendar, trace_flag);
	}
	
	public void createTopology()
			throws Exception {
		
		//initGridSim(isFlowType);
		
		assert rack != null;
		Router router = flowType? 
			new FlowRouter(rack.getRouter(),false):
			new RIPRouter(rack.getRouter(), false);
		routers.add(router);

		for (JsonMachine m : rack.getMachines()) {
			Link link = flowType?
				new FlowLink("link_" + m.getHostName(), m.getBaudRate(), rack.getPropDelay(), rack.getMaxIM()):
				new SimpleLink("link_" + m.getHostName(), m.getBaudRate(), rack.getPropDelay(), rack.getMaxIM());// HJobTracker.config.getMaxIM());

			links.add(link);

			NetEnd netend = new NetEnd( m.getHostName(), link);
			
//			netends.put("+ m.getHostName()+","+ netend);
			
			getNetends().put(m.getHostName(), netend);

			FIFOScheduler userSched = new FIFOScheduler("sched_"
					+ m.getHostName());
			router.attachHost(netend, userSched);
		}
		
		
	}

	public static void main(String[] args) throws Exception {

		boolean flowType=false;
		
		String[] ends = { "a", "b", "c", "d", "e","f","g" };
		HTopology topo=new HTopology(ends, 1000000, 30 , 100000,flowType);

		HTopology.initGridSim(flowType);
		
//		logger.info(topo.rack);
		topo.createTopology();
		
//		if(true)return;
		
		NetEndTest test = new NetEndTest("test", topo);
		
        GridSim.startGridSimulation();

//		Sim_system.run();

	}

	
	public void sim_msg(String from, String to, double size,
			int user, int returnTag, Object o){
		sim_msg(from, to, size, user, returnTag, o, 0.0);
	}
	public void sim_msg(String from, String to, double size,
			int user, int returnTag, Object o, double delay) {
		NetEnd ntFrom = getNetends().get(from);
		assert ntFrom.get_id() == GridSim.getEntityId(from);
		NetEnd ntTo = getNetends().get(to);
		assert ntTo.get_id() == GridSim.getEntityId(to);

		if (ntFrom == null || ntTo == null) {
			logger.error("No netends with the name:" + from + "," + to
					+ " in the system");
			return;
		}
		
		ntFrom.sim_msg(ntTo.get_id(), size, user, returnTag,o, delay);
	}	
	
	@Deprecated
	public void msg(String from, String to, double size,
			int user, int returnTag, Object o){
		msg(from, to, size, user, returnTag, o, 0.0);
	}
	
	@Deprecated
	public void msg(String from, String to, double size,
			int user, int returnTag, Object o, double delay) {
		NetEnd ntFrom = getNetends().get(from);
		assert ntFrom.get_id() == GridSim.getEntityId(from);
		NetEnd ntTo = getNetends().get(to);
		assert ntTo.get_id() == GridSim.getEntityId(to);

		if (ntFrom == null || ntTo == null) {
			logger.error("No netends with the name:" + from + "," + to
					+ " in the system");
			return;
		}
		
		ntFrom.msg(ntTo.get_id(), size, user, returnTag,o, delay);
	}	
	

	public double simpleSendSpeed = 1000;

	

	public boolean simpleSend(String from, String to, double size, Datum obj,
			double delay) {
		
		assert getNetends().containsKey(from);
		assert getNetends().containsKey(to);
		hlog.info("simpleSend from:"+from+", to:"+ to+ ", size:"+ size+ ", "+obj);
		logger.debug("simpleSend from:"+from+", to:"+ to+ ", size:"+ size+ ", "+obj);

		NetEnd ntFrom = getNetends().get(from);
		NetEnd ntTo = getNetends().get(to);
		if (ntFrom == null || ntTo == null) {
			logger.error("No netends with the name:" + from + "," + to
					+ " in the system");
			return false;
		}

		if( size==0 ){
			logger.info("send zero size ");
		}
		double time=delay + size / simpleSendSpeed; 
		logger.info("time:"+ time +", clock:"+ Sim_system.clock());
		logger.info("simpleSend from"+ from+ ", to:"+to+", size:"+ size+ ", data:"+obj);

		hlog.info("simpleSend from"+ from+ ", to:"+to+", size:"+ size+ ", data:"+obj);
		hlog.info("simple send schedule: "+ time);
		
		for (Sim_entity user : obj.getUsers()) {
			ntTo.sim_schedule(user.get_id(), time, obj.getDoneTag() ,obj);
		}
		return true;
	}

	public void stopSimulation() {
		logger.info("Stopping NetEnd Simulation");
		for (NetEnd netend : getNetends().values()) {
			netend.stopEntity();
		}
		
		logger.info("Stopping Routers");
		for (Router r : routers) {
			r.sim_schedule(r.get_id(), 0.0, HTAG.END_OF_SIMULATION);
		}
		
		logger.info("stop links");
		for (Link link : links) {
			link.sim_schedule(link.get_id(), 0.0, HTAG.END_OF_SIMULATION);
		}
//		hlog.save();
		
	}
	
	
	@Override
	public HLogger getHlog() {
		return hlog;
	}
	
	@Override
	public String toString() {
		return "Topology";
	}

	public void setNetends(Map<String, NetEnd> netends) {
		this.netends = netends;
	}

	public Map<String, NetEnd> getNetends() {
		return netends;
	}
	
}

class NetEndTest extends GridSim {
	/**
	 * Logger for this class
	 */
	private static final Logger logger = Logger.getLogger(NetEndTest.class);
	
	final HTopology topology;
	public NetEndTest(String name, HTopology topology) throws Exception {
		super(name);
		this.topology=topology;
	}

	@Override
	public void body() {
		//gridSimHold(2000);


		//topology.simpleSend("a", "b", 9900, n1, 1.0);
		
//
//		topology.msg("a", "b", 5500000, this.get_id(), 666, "msg 1");
//		
//		topology.msg("a", "b", 10000, this.get_id(), 666, "msg 2");

//		topology.msg("a", "b", 5500000, this.get_id(), HTAG.test_1.id(), "msg 2");
//		topology.msg("a", "b", 5500000, this.get_id(), HTAG.test_1.id(), "msg 3");
//		topology.msg("a", "b", 5500000, this.get_id(), HTAG.test_1.id(), "msg 4");
//		topology.msg("c", "d", 10000, this.get_id(), HTAG.test_1.id(), "msg 5");
//		
		topology.sim_msg("a", "b", 5500000, this.get_id(), HTAG.test_1.id(), "msg 2");
		topology.sim_msg("a", "b", 5500000, this.get_id(), HTAG.test_1.id(), "msg 3");
		topology.sim_msg("a", "b", 5500000, this.get_id(), HTAG.test_1.id(), "msg 4");
		topology.sim_msg("c", "d", 10000  , this.get_id(), HTAG.test_1.id(), "msg 5");
		
		
		//topology.sendData("d", "e", 3300000, n3);
		for (int i = 0; i < 20; i++) {
			String msg=(String)
				Datum.collectOne(this, HTAG.test_1.id());
			logger.info("msg "+ msg+ " clock:"+Sim_system.clock());
		}
		
		

		this.gridSimHold(1000000);
		
		topology.stopSimulation();
	}
	

}


