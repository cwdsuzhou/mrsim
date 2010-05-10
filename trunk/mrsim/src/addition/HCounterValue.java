package addition;

import hasim.CTag;
import hasim.HCounter;

public class HCounterValue {
	private final HCounter counter;
	private final CTag tag;
	public HCounterValue(HCounter counter, CTag tag) {
		this.counter=counter;
		this.tag=tag;
	}
	
	//TODO need to synchronize
	synchronized public double inc(double value){
		double r= counter.inc(tag, value);
		return r;
	}
	
	synchronized public void set(CTag tag, double value){
		counter.put(tag,value);
	}
	
	@Override
	public String toString() {
		return "counter "+tag+":"+counter.get(tag);
	}
	
	public double get(){
		return counter.get(tag);
	}
}
