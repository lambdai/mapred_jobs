package db.table;

import java.io.IOException;

public class RowFormatException extends RuntimeException {
	
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	public RowFormatException(IOException ioe) {
		super(ioe);
	}
	
	public RowFormatException(String cause) {
		super(cause);
	}
}
