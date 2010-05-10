package hasim;


import java.util.LinkedHashSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.log4j.Logger;

import eduni.simjava.Sim_entity;

public class CopyObject implements HLoggerInterface{
	/**
	 * Logger for this class
	 */
	private static final Logger logger = Logger.getLogger(CopyObject.class);
	public static enum Type{hard_hard,hard_mem, mem_hard, mem_mem};
	public static enum Step{hard_from,hard_to,net};

	private static AtomicInteger ID=new AtomicInteger();

	final public int id;
	final public Set<Step> steps=new LinkedHashSet<Step>();
	public final double size;
	public final String from;
	public final String to;
	public final Sim_entity user;
	public final int returnTag;
	public final Object object;
	public final Type type;
	final private HLogger hlog;
	
	public double start_Time=0;

	public CopyObject(double size, Type type, String from,
			String to, Sim_entity user, int returnTag, Object o) {

		assert from != null;
		assert to != null;
		this.id=ID.incrementAndGet();
		this.from=from;
		this.to=to;
		this.size=size;
		this.user=user;
		this.returnTag=returnTag;
		this.object=o;
		this.hlog=new HLogger(from+"_"+to+"_"+type);
		this.type=type;

		if(to.equalsIgnoreCase(from)){
			switch (type) {
			case hard_mem:
				steps.add(Step.hard_from);
				break;
			case mem_hard:
				steps.add(Step.hard_to);
			default:
				break;
			}
		}else{
			switch (type) {
			case hard_hard:
				steps.add(Step.hard_from);
				steps.add(Step.net);
				steps.add(Step.hard_to);
				break;
			case hard_mem:
				steps.add(Step.hard_from);
				steps.add(Step.net);
				break;
			case mem_mem:
				steps.add(Step.net);
				break;
			case mem_hard:
				steps.add(Step.net);
				steps.add(Step.hard_to);
				break;
			default:
				logger.error(type.toString() +" is not included");
				break;
			}
		}
	}

	//	public Set<Step> getSteps() {
	//		return steps;
	//	}
	//
	//	public double getSize() {
	//		return size;
	//	}
	//
	//	public String getFrom() {
	//		return from;
	//	}
	//
	//	public String getTo() {
	//		return to;
	//	}
	//
	//	public Sim_entity getUser() {
	//		return user;
	//	}
	//
	//	public int getReturnTag() {
	//		return returnTag;
	//	}
	//
	//	public Object getO() {
	//		return o;
	//	}

	@Override
	public HLogger getHlog() {
		return hlog;
	}

	@Override
	public String toString() {
		String result="cpo "+id+" ,from:"+from+" ,to:"+to+", size:"+size+", type:"+type+
		", steps:"+steps;
		return result;
	}
}
