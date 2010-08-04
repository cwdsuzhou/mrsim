package hasim.core;

import hasim.HTAG;

import static hasim.Tools.format;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import javax.sql.rowset.Predicate;


import org.apache.log4j.Logger;

import addition2.TestRandom;

import dfs.Pair;

import eduni.simjava.Sim_entity;
import eduni.simjava.Sim_event;
import eduni.simjava.Sim_predicate;
import eduni.simjava.Sim_system;
import eduni.simjava.Sim_type_p;


public class Datum implements Comparable<Object>{
	
	
	/**
	 * Logger for this class
	 */
	@SuppressWarnings("unused")
	private static final Logger logger = Logger.getLogger(Datum.class);


	
	//HTAG.values().length
	private static AtomicInteger totalId=new AtomicInteger();
	

	public static List<Object> collect( int times, Sim_entity entity,  int... tags){
		List<Object> result=new ArrayList<Object>();
		for (int i = 0; i < times; i++) {
			result.addAll(collect(entity, tags));
		}
		return result;
	}
	public static List<Object> collect( Sim_entity entity,  int... tags){
		List<Object> result=new ArrayList<Object>();
		for (int i = 0; i < tags.length; i++) {
			result.add(collectOne(entity, tags[i]));
		}
		return result;
	}
	public static Object collectAny(Sim_entity entity, int... tags){
		Sim_event ev=new Sim_event();
		Sim_predicate p=new Sim_type_p(tags);
		entity.sim_get_next(p,ev);
		return ev.get_data();
	}
	
	public static Object collectOne(Sim_entity entity, int tag){
		Sim_event ev=new Sim_event();
		entity.sim_get_next( new Sim_type_p(tag), ev);
		return ev.get_data();
		
	}

//	public final double delta;
	

	private Object data;
	public int times=0;
	public double speed=0;


	public double orgRecords;
	boolean inMemory=false;
	
	public boolean isInMemory() {
		return inMemory;
	}
	public void setInMemory(boolean inMemory) {
		this.inMemory = inMemory;
	}
	public Object getData() {
		return data;
	}
	public void setData(Object data) {
		this.data = data;
	}

	private int doneTag;
	
	//private LinkedList<Double> deltaList=new LinkedList<Double>();
	
	final private int id;
	private String location;
	
	public final String name;
	public final double records; 
	public final double size;

	public HTAG tag;

	public double tmpDelta=0.25;

	private Set<Sim_entity> users=new LinkedHashSet<Sim_entity>();

	
	public Datum(Datum d){
		this(d.getName(), d.size, d.records, d.getLocation());
		this.inMemory =d.inMemory;
		this.orgRecords = d.orgRecords;
	}

	public Datum(double size, double records){
		this("datum-",size,records, "");
	}
	public Datum(String name, Datum data){
		this(name, data.size,data.records, data.getLocation());
	}
	public Datum(String name,Datum d, double fraction){
		this(name,d.size*fraction,  d.records*fraction, d.getLocation());
	}
	public Datum(Datum d, double fraction){
		this(d.name,d.size*fraction,  d.records*fraction, d.getLocation());
	}
	
	
	
	public Datum(String name, double size, double records){
		this(name, size, records,"");
	}

	
	public Datum(String name, double size, double records,String location){
		this.id= totalId.incrementAndGet();

		this.name=name;
		this.size=size;
		this.records=records;
		this.location=location;
		
		this.orgRecords =records;
	}
	public Datum(Datum mapOutputLoc, boolean inMemory) {
		this(mapOutputLoc);
		this.inMemory=inMemory;
	}
	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		Datum other = (Datum) obj;
		if (id != other.id)
			return false;
		return true;
	}	
	
//	public boolean registerBlocking(Sim_entity user, int returnTag){
//		this.returnTag=returnTag;
//		return blockingUsers.add(user);
//	}
	public int getDoneTag() {
		return doneTag;
	}
	
	
	public String getName() {
		return name;
	}
//	public int getReturnTag() {
//		return returnTag;
//	}
	public Set<Sim_entity> getUsers() {
		return users;
	}
	

	@Override
	public int hashCode() {
		return id;
//		final int prime = 31;
//		int result = 1;
//		result = prime * result + (int) (id ^ (id >>> 32));
//		return result;
	}
	
	public int id(){
		return this.id;
	}
	
	
	
	
	

	
	private Sim_predicate predicate(int i){
		return new Sim_type_p(i);
		
	}
	public boolean register(Sim_entity user, int doneTag){
		this.doneTag=doneTag;
		return users.add(user);
	}
	@Override
	public String toString() {
		return this.getClass().getName()+" :"+name+"\tid: "+ id+
		", size:"+format(size)+ ", r:"+format(records)+", L:"+location;
	}
	
	public boolean unregister(Sim_entity user){
		return users.remove(user);
	}
	public void setLocation(String location) {
		this.location = location;
	}
	public String getLocation() {
		return location;
	}
	
	
	@Override
	public int compareTo(Object o) {
		// TODO Auto-generated method stub
		Datum od=(Datum)o;
		double dif=size-od.size;
		if(dif != 0)
			return (int)Math.signum(dif);
		return id-od.id;
	}

	
	public static void main(String[] args) {
		Datum d1=new Datum(20, 20);
		Datum d2=new Datum(20, 20);
//		Datum d2=new Datum(19.99999, 19.99999);
		
		System.out.println(""+d1.compareTo(d2));
		System.out.println(""+d2.compareTo(d1));
	}
}
