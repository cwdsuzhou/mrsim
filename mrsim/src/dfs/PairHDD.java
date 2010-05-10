package dfs;


public class PairHDD{
	enum Type{READ,WRTIE};
	Type type;
	double size;

	public PairHDD(Type type, double size) {
		super();
		this.type = type;
		this.size = size;
	}
	
	@Override
	public String toString() {
		// TODO Auto-generated method stub
		return "Pair("+type.name()+", "+ size+") ";
	}
}