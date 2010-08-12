package hasim;

import eduni.simjava.Sim_entity;
import addition2.TestRandom;
import hasim.core.CPU;
import hasim.core.Datum;

public class HSimpleCombiner implements HCombiner {

	final private int groups;
	final private double combineCost;
	
	public HSimpleCombiner(int groups,double combineCost) {
		this.groups = groups;
		this.combineCost=combineCost;
	}
	
	
	public int getGroups() {
		return groups;
	}

	



	@Override
	public Datum combine(Datum... d1) {
		return combine(groups,d1);
	}

	/**
	 * combine size cost is not considered now. To be added later.
	 * @param groups
	 * @param varDatum
	 * @return
	 */
	public static Datum combine(int groups, Datum ... varDatum  ){
		double orgRecords = 0, records=0, size=0;
		
		for (Datum d : varDatum) {
			assert d.orgRecords >0 ;
			orgRecords+= d.orgRecords;
			records += d.records;
			size += d.size;
		}
		
		double combinedRecords = TestRandom.getSizePowerSum(groups,
				(int)orgRecords);
//		assert combinedRecords > records;
		
		double combinedSize = size * combinedRecords/records; 
		Datum result= new Datum(varDatum[0].name+"-c",
				combinedSize, combinedRecords);
		//very important do not delete
		result.orgRecords = orgRecords;
		
		return result;
		
	}
	
	public static void main(String[] args) {
		
		HCombiner combiner = new HSimpleCombiner(10000,30.0);
		Datum d=new Datum("d", 100, 10000);
		
		System.out.println(d);
		
		System.out.println(combiner.combine(d));
		System.out.println(combiner.combine(d,d));
//		System.out.println(combiner.combine(d));
//		System.out.println(combiner.combine(d));
	}


	@Override
	public double cost(Datum d) {
		return combineCost*d.records;
	}
}
