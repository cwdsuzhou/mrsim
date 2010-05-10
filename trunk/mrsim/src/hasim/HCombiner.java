package hasim;

import hasim.core.Datum;

public interface HCombiner {
	public Datum combine(Datum inDatum);
}
