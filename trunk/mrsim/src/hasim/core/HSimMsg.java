package hasim.core;

import java.util.concurrent.atomic.AtomicInteger;

public class HSimMsg{
	private static AtomicInteger ID=new AtomicInteger();
	final public int id;

	final public int from,to; 
	final public double deltaSize;
	final public int user;
	final public int returnTag;
	final public Object object;

	private double size;
	private int times=0;

	public int getTimes(){
		return times;
	}
	public double decTimesAndGet(){
		times++;
		if(size>= deltaSize){
			size-=deltaSize;
			return deltaSize;
		}else{
			double last=size;
			size=0;
			return last;
		}
	}
	
	public boolean hasNext(){
		return size > 0;
	}
	public double getSize(){
		return size;
	}
	
	public HSimMsg(int from, int to, double size, double deltaSize, 
			int user, int returnTag, Object object) {
		this.id=ID.incrementAndGet();
		
		this.from=from;
		this.to=to;
		this.size=size;
		this.deltaSize=deltaSize;
		this.user=user;
		this.returnTag=returnTag;
		this.object=object;
	}

	@Override
	public String toString() {
		return "LocalMsg"+ id+", totalTime:"+size+" , times:"+times;
	}
}
