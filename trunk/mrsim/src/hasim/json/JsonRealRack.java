package hasim.json;

import java.io.File;
import java.lang.reflect.Field;
import java.util.List;
import org.codehaus.jackson.map.ObjectMapper;

public class JsonRealRack{
	private String name="rack 0";
	
	private String router;
	private double propDelay;

	private  double heartbeat;
	private String hlogLevel="info";
	
	public String getHlogLevel() {
		return hlogLevel;
	}
	public void setHlogLevel(String hlogLevel) {
		this.hlogLevel = hlogLevel;
	}
	public double getHeartbeat() {
		return heartbeat;
	}
	public void setHeartbeat(double heartbeat) {
		this.heartbeat = heartbeat;
	}

	private double deltaCPU,deltaHDD,deltaNEt;

	
	public double getDeltaNEt() {
		return deltaNEt;
	}
	public void setDeltaNEt(double deltaNEt) {
		this.deltaNEt = deltaNEt;
	}
	public double getDeltaCPU() {
		return deltaCPU;
	}
	public void setDeltaCPU(double deltaCPU) {
		this.deltaCPU = deltaCPU;
	}
	public double getDeltaHDD() {
		return deltaHDD;
	}
	public void setDeltaHDD(double deltaHDD) {
		this.deltaHDD = deltaHDD;
	}

	private int maxIM;
	private boolean flowType=false;
	
	
	public boolean isFlowType() {
		return flowType;
	}
	public void setFlowType(boolean flowType) {
		this.flowType = flowType;
	}
	public int getMaxIM() {
		return maxIM;
	}
	public void setMaxIM(int maxIM) {
		this.maxIM = maxIM;
	}
	public double getPropDelay() {
		return propDelay;
	}
	public void setPropDelay(double propDelay) {
		this.propDelay = propDelay;
	}

	private List<JsonMachine> machines;
	public String getRouter() {
		return router;
	}
	public void setRouter(String router) {
		this.router = router;
	}
	public List<JsonMachine> getMachines() {
		return machines;
	}
	public void setMachines(List<JsonMachine> machines) {
		this.machines = machines;
	}
	public String getName() {
		return name;
	}
	public void setName(String name) {
		this.name = name;
	}

	
	@Override
	public String toString(){
		String result="";
		try {
			Class cls=Class.forName(this.getClass().getName());
			Field[] flds= cls.getDeclaredFields();
			for (Field fld : flds) 
				result+= "\n"+fld.getName()+":"+fld.get(this);
		} catch (Exception e) {
			e.printStackTrace();
		}
		return result;
	}

	
	
	
	public static void main(String[] args) {
		JsonRealRack rack=JsonJob.read("data/json/rack_working.json", JsonRealRack.class);
		System.out.println(rack);
		//JsonJob.save("data/rackout", rack);
	}
}