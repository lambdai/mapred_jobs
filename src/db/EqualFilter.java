package db;

public class EqualFilter implements RowFilter {

	int columnId;
	int dest;
	
	EqualFilter(int columnId, int dest) {
		this.columnId = columnId;
		this.dest = dest;
	}
	
	@Override
	public boolean filtered(String[] row) {
		return Integer.parseInt(row[columnId]) == dest;
	}

}
