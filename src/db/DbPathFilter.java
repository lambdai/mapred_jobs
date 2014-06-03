package db;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;

public class DbPathFilter implements PathFilter {

	@Override
	public boolean accept(Path path) {
		return !path.getName().equals("_SUCCESS")
				&& !path.getName().equals("_logs");
	}

}
