package query;

import java.util.*;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.lang.reflect.Array;

public class Tuple implements Serializable{

	private static final long serialVersionUID = 5279520027567712726L;


//	for simple, each max size of attribute is 16 byte
	
	public final Integer sizeOfAttri = 16;

	public ArrayList<String> tupleValue=new ArrayList<>();

//  this place is use to put attribute name of a relation
	public ArrayList<String> attributeName=new ArrayList<>();

	
//	to link the attribute name and it's value of one tuple
	public HashMap<Integer,Integer> keyByteArray= new HashMap<>();
	
//  the boolean array is used in Fragmentation,false means the place is not used by other data, it can show N/A or not
	boolean[] occupied = new boolean[] {false,false,false,false,false,false,false,false,false,false,false,false,false,false,false,false,false,false,false,false};
	

//  find if the space of 16byte was occupied	
	protected boolean isOccupied(int attriName) {
		return occupied[attriName];
	}
	
//  use attribute name to find the one part of each tuple
	public int keyToIndex(String key) {
		if(keyByteArray.containsKey(key)) {
		//  throw an exception if key doesn't exist
			return keyByteArray.get(key);
		} else {
			throw new RuntimeException("Key doesn't exist!");
		}
	}
	
// the get interface : byte[] Get(int key)
	public String getValue(String key) throws KeyNotFoundException {
		if(keyByteArray.containsKey(key)){
		int attriName = keyToIndex(key);
		return tupleValue.get(attriName);
		}
		else{
			throw new KeyNotFoundException("cannot find key for " 
					+ key);
		}
	}

	
//  stores data under the given key : void put(int key, byte[] data)	
	public void put(String key, String value) {
	//  first check if key exists, if exist, update this row
		if(keyByteArray.containsKey(key)) {
			int index = keyByteArray.get(key);
			tupleValue.set(index, value);
		} else {
			
		}
	}

	
//	deletes the key : void remove(int key)
	public synchronized void remove(String key){
		if(keyByteArray.containsKey(key)) {
			int index=keyToIndex(key);
			tupleValue.remove(index);
			occupied[index]=false;
			keyByteArray.remove(key);
			System.out.println("Successfully removing "+key);

		}
		else{
		System.out.println("No key exist when removing"+key);
		}
	}
	
	
//  to initialize the arrayDB	
	public void clearData() {
		this.tupleValue = new ArrayList<>();
		this.keyByteArray=new HashMap<>();
		this.occupied=new boolean[] {false,false,false,false,false,false,false,false,false,false,false,false,false,false,false,false,false,false,false,false};
	}

}
