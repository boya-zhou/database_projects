package log;

// throw Exception when key is not in Tuple, using in Tuple
public class KeyNotFoundException extends Exception {
	
	public KeyNotFoundException(String msg) {
		super(msg);
	}
	
	public KeyNotFoundException(Throwable t) {
		super(t);
		
	}

}
