package store;

// throw Exception when key is not in arrayDB, using in ArrayDB
public class KeyNotFoundException extends Exception {
	
	public KeyNotFoundException(String msg) {
		super(msg);
	}
	
	public KeyNotFoundException(Throwable t) {
		super(t);
		
	}

}
