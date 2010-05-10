package hasim;

import hasim.core.Datum;

public interface HDiskUser {

	public int readFile(Datum file, HDiskUser user);

	public int writeFile(Datum file, HDiskUser user);

	public void update(Datum file, HTAG type);

	public int get_id();
}
