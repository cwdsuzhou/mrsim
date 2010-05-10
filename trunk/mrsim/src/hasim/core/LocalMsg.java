package hasim.core;

import java.util.concurrent.atomic.AtomicInteger;

public class LocalMsg{
	private static AtomicInteger ID=new AtomicInteger();
	
	public static enum OP_TYPE{WRITE,READ,CPU_WORK};
	final OP_TYPE opType;
	final public int id;
	final public double deltaTime;
	final public int user;
	final public int returnTag;
	final public Object object;
	
	private double totalTime;
	private int times=0;
	private int tag;
	
	public int getTag() {
		return tag;
	}
	public void setTag(int tag) {
		this.tag = tag;
	}
	public int getTimes(){
		return times;
	}
	synchronized public double decTimesAndGet(){
		times++;
		if(totalTime >= deltaTime){
			totalTime -= deltaTime;
			assert totalTime>=0;
			
			return deltaTime;
		}else{
			double last=totalTime;
			totalTime=0;
			return last;
		}
	}
	
	synchronized public boolean hasNext(){
		return totalTime > 0;
	}
	public double getTotalTime(){
		return totalTime;
	}
	
	public LocalMsg(double totalTime, double deltaTime, 
			int user, int returnTag, Object object, OP_TYPE optype) {
		assert totalTime>0;
		assert deltaTime>0;
		
		this.id=ID.incrementAndGet();

//		if(id== 297 ||id== 689){
//			assert false;
//			System.err.println(" id= "+id);
//		}
		this.totalTime=totalTime;
		this.deltaTime=deltaTime;
		this.user=user;
		this.returnTag=returnTag;
		this.object=object;
		this.opType=optype;
	}

	@Override
	public String toString() {
		return "LocalMsg"+ id+", totalTime:"+totalTime+" , times:"+times;
	}
}
