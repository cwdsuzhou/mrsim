package hasim;

import org.apache.log4j.Logger;

import addition.HCounterValue;

import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.util.Formatter;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.TreeMap;


@SuppressWarnings("serial")
public class HCounter extends TreeMap<CTag, Double>{
	/**
 * Logger for this class
 */
private static final Logger logger = Logger.getLogger(HCounter.class);
	
	final private  DecimalFormat format;
    final Formatter fmt;

	public HCounter() {
		super(CTag.ctagComp);
		for (CTag tag : CTag.values()) {
			inc(tag, 0.0);
		}
		format = new DecimalFormat();
		format.setDecimalSeparatorAlwaysShown(true);
		format.setGroupingSize(3);
		format.setMaximumFractionDigits(3);
		
		fmt= new Formatter();
	}
	
	//TODO need to synchronize
	synchronized public double inc(CTag tag, double value){
		double cValeu=get(tag)+value;
		put(tag, cValeu);
		return cValeu;
	}
	
	synchronized public void set(CTag tag, double value){
		get(tag);
		put(tag,value);
	}
	
	synchronized public double get(CTag key) {
		Double v=super.get(key);
		if(v==null){
			put(key, 0.0);
			return 0;
		}
		return v;
	}
	
	public void addAll(HCounter c) {
		for (Map.Entry<CTag, Double> iter : c.entrySet()) {
			inc(iter.getKey(), iter.getValue());
		}
	}
	
	public void addAll(HCounter c, double d) {
		for (Map.Entry<CTag, Double> iter : c.entrySet()) {
			inc(iter.getKey(), iter.getValue()/(0.0+d));
		}
	}

	public HCounter divide(double d){
		HCounter result=new HCounter();
		result.addAll(this,d);
		return result;
		
	}
	
	public HCounterValue hValue(CTag tag){
		return new HCounterValue(this, tag);
	}
	
	@Override
	public String toString() {
		return toString("\n");
	}
	public String toString(String pre) {
		StringBuffer sb=new StringBuffer();
		for (Map.Entry<CTag, Double> e:entrySet()){
			
			if(Math.abs(e.getValue())< 1e-6)
				continue;
//			String key=String.format("%25s", e.getKey()+"\t\t");
			sb.append(pre+e.getKey()+"\t"+format.format(e.getValue()));
		}
		return sb.toString();
	}
	public static void main(String[] args) {
		HCounter c=new HCounter();
		logger.info(c.inc(CTag.COMBINE_INPUT_RECORDS, 454543535.845984958495948));
		logger.info(c.inc(CTag.COMBINE_OUTPUT_GROUPS, 0.1));
		Double d=c.get(CTag.COMBINE_INPUT_RECORDS);

		logger.info("d="+ d);
		d+=10;
		logger.info(c.get(CTag.COMBINE_INPUT_RECORDS));
		logger.info(c);
	}
	
	
	
}
