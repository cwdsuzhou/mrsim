package test;

public class Mem {

	static double[] opt={0,5	,10	,20	,50,	100,	150,	200	,256,	500	,512,
			700,	1000,	1024,	2000, 4000};
	
	static double[] mem={0,7864320,	9371648,	18677760,	46661632,	93257728,	139853824,
			186449920,	238616576,	466092032,	477233152,	652476416,
			932118528,	954466304,	1864171520, 3728343040.0};
	
	{
		assert opt.length== mem.length;
	}
	
	public static double calc(double k){
		
		int i=1;
		for (; i < opt.length-1; i++) {
			if(k<opt[i])break;
		}
		double a=(mem[i]-mem[i-1])/(opt[i]-opt[i-1]);
		
		double result=mem[i-1]+a*(k-opt[i-1]);
		return result;
	}
	
	public static double memLimit(double x){
		
		return 0;
	}
	public static void main(String[] args) {
		long memd=Runtime.getRuntime().maxMemory();
		System.out.println(memd);
	
		System.out.println(""+calc(128));
//		assert opt.length==mem.length;
//		for (int i = 0; i < opt.length; i++) {
//			double d=(calc(opt[i])-mem[i]);
//			System.out.println(""+d);
//		}
		
	}
}
