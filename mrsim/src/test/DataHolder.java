package test;

import java.text.DecimalFormat;

public class DataHolder implements Comparable<Object>{
	static DecimalFormat f = new DecimalFormat(); 
	{

		f.setGroupingUsed(false);
		f.setDecimalSeparatorAlwaysShown(true);
		f.setMaximumFractionDigits(4);
		f.setMinimumFractionDigits(3);
		f.setMinimumIntegerDigits(5);
	}
	
	int id;
	double interval;
	double job;
	
	double submitTime;
	double startTime;
	double stopTime;
	double exeTime;
	double queu;
	
	@Override
	public boolean equals(Object obj) {
		DataHolder dh=(DataHolder)obj;
		int dif = id-dh.id;
		if(dif != 0 )return false;
		double e=1e-6;
		return job-dh.job<e &&
		interval-dh.interval <e &&
		submitTime -dh.submitTime<e  &&
		startTime - dh.startTime<e  &&
		stopTime - dh.stopTime<e  &&
		exeTime - dh.exeTime<e  &&
		queu -dh.queu<e ;
	}
	public DataHolder(int id, double interval, double job) {
		this.id=id;
		this.interval=interval;
		this.job=job;
	}
	
	@Override
	public String toString() {
		StringBuffer sb=new StringBuffer();
		sb.append(f.format(id)+"\t");
		sb.append(f.format(interval)+"\t");
		sb.append(f.format(job)+"\t");
		//		sb.append(f.format(submitTimeTheory)+"\t");
		sb.append(f.format(submitTime)+"\t");
		sb.append(f.format(startTime)+"\t");
		sb.append(f.format(stopTime)+"\t");
		sb.append(f.format(exeTime)+"\t");
//		sb.append(waitingJobs.get()+"\t");

		return sb.toString();
	}

	@Override
	public int compareTo(Object o) {
		if(! (o instanceof DataHolder))return -1;
		DataHolder b=(DataHolder)o;
		
		double dif=0.0;
		
		dif = startTime -b.startTime;
		if(dif !=0)return (int)Math.signum(dif);
		
		dif= stopTime -b.stopTime;
		if(dif !=0)return (int)Math.signum(dif);
		
		dif = job-b.job;
		if(dif !=0)return -(int)Math.signum(dif);

		return 0;
	}
	
}