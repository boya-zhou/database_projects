package store;
import java.util.HashMap;

import store.ArrayDB;


public class LayoutOfCS542db {

//	show the layout of CS542.db, the content is after using removeAndPutFragmentation() in main()
//	because it's difficult to show the real content of File(1MB of random byte), we use the address in memory to replace the content
	private static final String fileLocation = "/Users/boyazhou/Desktop/database/CS542_Internal_HW1/bin/store/CS542.db";


	public static void main(String[] args){
		ArrayDB arrayDB = ArrayDB.readFromDisk();
		byte[][] dbFileContent = arrayDB.arrayFile;
		HashMap<Integer, Integer> dbKeyToIndex=arrayDB.keyByteArray;
		boolean[] dbFileOccuipied=arrayDB.occupied;
		
		System.out.println("Key"+"\t"+"Index"+"\t"+"dbFileContent"+"\t"+"dbFileOccuipied");
		
		int index = 0;
		byte[] content = {0};
		boolean bool=false;
		for (int key : dbKeyToIndex.keySet()) {
			
			index = arrayDB.keyToIndex(key);
			content = dbFileContent[index];
			bool=dbFileOccuipied[index];

		    System.out.println(String.valueOf((char)key)+"\t"+index+"\t"+content+"\t"+bool);

		}	
	}
}
