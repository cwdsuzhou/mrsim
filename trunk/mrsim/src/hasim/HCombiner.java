package hasim;

import hasim.core.CPU;
import hasim.core.Datum;

public interface HCombiner {
	public Datum combine(Datum ... d1  );
	public double cost(Datum d);
}
