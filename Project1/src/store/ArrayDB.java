package store;

import java.util.*;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;

public class ArrayDB implements Serializable{

	private static final long serialVersionUID = 5279520027567712726L;


//	The value byte array can be arbitrarily large, up to 1 MB
	
	public final Integer sizeOfByteArray=1048576;


//  4 MB for actual data	
	public byte[][] arrayFile=new byte[4][sizeOfByteArray];

//  1 MB for metadata, we have tested the metadata(which are stored in keyByteArray and occupied ) of arrayFile are 
//	smaller than 1MB, and it's also directly to figure out
	public byte[][] arrayMetaFile=new byte[0][sizeOfByteArray];

	
//	to link the index of valueOfArray and the given Key like 'A','B','C'
	public HashMap<Integer,Integer> keyByteArray= new HashMap<>();
	
//  the boolean array is used in Fragmentation,false means the place is not used by other data 
	boolean[] occupied = new boolean[] {false,false,false,false};
	
//  the location where we put CS542.db
	
	private static final String fileLocation = "/Users/boyazhou/Desktop/database/CS542_Internal_HW1/bin/store/CS542.db";

//  find if the space of 1MB was occupied	
	protected boolean isOccupied(int index) {
		return occupied[index];
	}
	
//  use the key('A','B' and so on) to find the index of array(0,1,2,3)
	public int keyToIndex(int key) {
		if(keyByteArray.containsKey(key)) {
		//  throw an exception if key doesn't exist
			return keyByteArray.get(key);
		} else {
			throw new RuntimeException("Key doesn't exist!");
		}
	}
	
// the get interface : byte[] Get(int key)
	public byte[] get(int key) throws KeyNotFoundException {
		if(keyByteArray.containsKey(key)){
		int index = keyToIndex(key);
		return arrayFile[index];
		}
		else{
			throw new KeyNotFoundException("cannot find key for " 
					+ String.valueOf((char)key));
		}
	}

	

	
//  stores data under the given key : void put(int key, byte[] data)	
	public synchronized void put(int key, byte[] eachRowInArrayFile) {
	//  first check if key exists, if exist, update this row
		if(keyByteArray.containsKey(key)) {
			int index = keyByteArray.get(key);
			arrayFile[index] = eachRowInArrayFile;
			ArrayDB.saveToDisk(this);
			System.out.println("Successfully putting "+String.valueOf((char)key));
		}
		else {
		//  second find if there is space for a new row to insert
			int i;
			for(i=0;i<4;i++){
				if (isOccupied(i)==false){
					arrayFile[i]=eachRowInArrayFile;
					occupied[i]=true;
					keyByteArray.put(key, i);
					ArrayDB.saveToDisk(this);
					System.out.println("Successfully putting "+String.valueOf((char)key));
					break;
				}
			}
			if(i >= 4) {
			//  indicate out of space
				System.out.println("There is no space for "+ String.valueOf((char)key));
			}
		}
	}

	
//	deletes the key : void remove(int key)
	public synchronized void remove(int key){
		if(keyByteArray.containsKey(key)) {
			int index=keyToIndex(key);
			arrayFile[index]=new byte[sizeOfByteArray];
			occupied[index]=false;
			keyByteArray.remove(key);
			ArrayDB.saveToDisk(this);
			System.out.println("Successfully removing "+String.valueOf((char)key));

		}
		else{
		System.out.println("No key exist when removing"+String.valueOf((char)key));
		}
	}
	
//	write the arrayDB into CS542.db
	public static void saveToDisk(ArrayDB arrayDB) {
	//  Write object with ObjectOutputStream
		ObjectOutputStream saveArrayDBToDisk;
		try {
			saveArrayDBToDisk = new ObjectOutputStream(new FileOutputStream(fileLocation));
		//  Write object out to disk
			saveArrayDBToDisk.writeObject(arrayDB);
			saveArrayDBToDisk.flush();
			saveArrayDBToDisk.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
//	read the arrayDB from CS542.db
	public static ArrayDB readFromDisk() {
		try {
			ObjectInputStream fromDiskIntoArrayDB = new ObjectInputStream (new FileInputStream(fileLocation));
			Object obj = fromDiskIntoArrayDB.readObject();
			ArrayDB data = (ArrayDB) obj;
			fromDiskIntoArrayDB.close();
			return data;
		} catch (IOException | ClassNotFoundException e) {
			throw new RuntimeException(e);
		}
	}
	
//  to initialize the arrayDB	
	public void clearData() {
		this.arrayFile = new byte[4][sizeOfByteArray]; 
		this.keyByteArray=new HashMap<>();
		this.occupied=new boolean[] {false,false,false,false};
	}

}
