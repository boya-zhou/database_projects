package store;


import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Calendar;
import java.util.Random;


public class Main {
	static ArrayDB arrayDB =new ArrayDB();
	private final static String fileLocation = "/Users/boyazhou/Desktop/database/CS542_Internal_HW1/bin/store/CS542.db";
	public static final  Integer sizeOfByteArray=1048576;//1MB
	
//	This 12 variable is to record the result of test
	static byte[] arr1 = {0};
	static byte[] arr2 = {0};
	static byte[] arr3 = {0};
	static byte[] arr4 = {0};
	static byte[] arr5 = {0};
	static byte[] arr6 = {0};

	static String s1;
	static String s2;
	static String s3;
	static String s4;
	static String s5;
	static String s6;



	public static void main(String[] args) {
//		Path path = Paths.get(fileLocation);
//		try {
//			Files.delete(path);
//		} catch (IOException e) {
//			e.printStackTrace();
//		}
//				
		try {
			arrayDB.clearData();
			putRandomByteArray('A');
			getAndPut();
			putRandomByteArray('A');
			removeAndGet();
			arrayDB.clearData();
			putAndGet();	
			arrayDB.clearData();
			removeAndPutFragmentation();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		
		System.out.println("-----Completed-----");
	}
	
//	
	public static void putRandomByteArray(int key) {
		byte[] data= new byte[sizeOfByteArray];
		new Random().nextBytes(data);
		arrayDB.put(key, data);
	}
	
	public static void putRandomByteArrayE(int key) {
		byte[] data= new byte[sizeOfByteArray/2];
		new Random().nextBytes(data);
		arrayDB.put(key, data);
	}
	

	public static byte[] getOneTurpleOfArrayDB(int key) 
			throws KeyNotFoundException{
		return arrayDB.get(key);
	}
	
	public static void removeOneTupleOfArrayDB(int key) {
		arrayDB.remove(key);
	}
	
	
	public static void getAndPut() throws InterruptedException {
		System.out.println("--------------------------");

		System.out.println("-----    getAndPut   -----");
		// get
		Thread t0 = new Thread() {
			public void run() {
				this.setName("t0");
				try {
					arr1 = getOneTurpleOfArrayDB('A');
					s1 = new String(arr1);
				} catch (KeyNotFoundException e) {
					s1 = new String();
				}	

				}
		};

// put
		Thread t1 = new Thread() {	
			public void run() {
				this.setName("t1");
				putRandomByteArray('A');
			} 	
		};
		
		t0.start();
		t1.start();	
		t0.join();
		t1.join();
		
//		System.out.println(new String(arrayDB.get('A')));
		arrayDB = ArrayDB.readFromDisk();
		try {
			arr2 = arrayDB.get('A');
			s2 = new String(arr2);
		} catch (KeyNotFoundException e) {
			s2 = new String();
		}


//		this result is to show the after the put thread, the result of getting key 'A'(s2) from database is different from the 
//		beginning(s1), which is gained by get thread. Which means read commited and no read dirty.
		System.out.println(s1.equals(s2));
		System.out.println("--------------------------");
		System.out.println("--------------------------");	
	}
	
	
	
	// get() does not need to wait for remove() to complete, therefore it
	// will successfully get() before remove() completes
	public static void removeAndGet() throws InterruptedException {
		System.out.println("-----------------------------");

		System.out.println("-----    removeAndGet   -----");
		// remove
		try {
			arr3=getOneTurpleOfArrayDB('A');
			s3=new String(arr3);
		} catch (KeyNotFoundException e1) {
			s3 = new String();
		}
		
		Thread t0 = new Thread() {
			public void run() {
				this.setName("t0");
				removeOneTupleOfArrayDB('A');
			}	
		};

// get
		Thread t1 = new Thread() {	
			public void run() {
				this.setName("t1");
				try {
//	after  a millisecond
					Thread.sleep(Calendar.MILLISECOND);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
				try {
					arr4 = getOneTurpleOfArrayDB('A');
					s4 = new String(arr4);
				} catch (KeyNotFoundException e) {
					System.out.println(e.getMessage());
				}
			} 	
		};
		
//	because the method of remove is synchronized,so even if t0.join() is after t1.start(), it will still gived expected result
		t0.start();
		t1.start();
		t0.join();
		t1.join();

//		System.out.println(s3.equals(s4));
		System.out.println("-----------------------------");
		System.out.println("-----------------------------");
		


		
	}
	
	// This test is the same as putAndGet() except that it shows the circumstance where 
		// the put() terminates before the get()
		public static void putAndGet() throws InterruptedException {
			System.out.println("----------------------------");
			System.out.println("-----    putAndGet()   -----");
			// put
			
			Thread t0 = new Thread() {
				public void run() {
					this.setName("t0");
					putRandomByteArray('A');
					try {
						arr5=getOneTurpleOfArrayDB('A');
						s5=new String(arr5);
					} catch (KeyNotFoundException e) {
						s5 = new String();
					}
					arrayDB.clearData();

				}
			};

			// get
			Thread t1 = new Thread() {	
				public void run() {
					this.setName("t1");
					arrayDB = ArrayDB.readFromDisk();
					try {
						arr6= getOneTurpleOfArrayDB('A');
						s6 = new String(arr6);
					} catch (KeyNotFoundException e) {
						s6 = new String();
					}

				} 	
			};
//          Because there is reboot of mechine between 2 transcation, so t0.join() is before t1.start();
			t0.start();
			t0.join();

			t1.start();
			t1.join();
			
			System.out.println(s5.equals(s6));
			System.out.println("----------------------------");
			System.out.println("----------------------------");


		}
	
	public static void removeAndPutFragmentation() throws InterruptedException {
//		Fragmentation.  Put() 4 values, byte arrays of 1 MB each, with keys A, B, C and D.
//		Remove key B. Put() ½ MB in size for key E. Validate that a Put() 1 MB in size for key F fails.
//		Remove C and now validate that a Put() 1 MB in size for key G succeeds.
//		Remove E and try Put() 1 MB in size for key H. With a naive implementation,
//		it will fail even though there is room in store.db. 
//		An extra bonus point if you can modify your code such that Put("H", …) succeeds.
		System.out.println("------------------------------------------");
		System.out.println("-----    removeAndPutFragmentation   -----");

		putRandomByteArray('A');
		putRandomByteArray('B');
		putRandomByteArray('C');
		putRandomByteArray('D');		
		removeOneTupleOfArrayDB('B');
		
//		Put() ½ MB in size for key E
		putRandomByteArrayE('E');
		
		
		
		putRandomByteArray('F');
		
		
		removeOneTupleOfArrayDB('C');
		
		putRandomByteArray('H');
		
		System.out.println("-----    the key with the index of array   -----");

		System.out.println(arrayDB.keyByteArray);
		System.out.println("------------------------------------------");
		System.out.println("------------------------------------------");

		
	}

	
	
}
