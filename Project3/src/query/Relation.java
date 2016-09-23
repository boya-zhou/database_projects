package query;

import java.util.*;
import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.io.FileReader;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.ArrayList;



public class Relation implements Serializable{
	
//make disk to save the whole relation considering block 
	private static final long serialVersionUID = 5279520027567712726L;
	
    int maxNumTuples = 0;
	public List<Tuple> tuples;
	public String[] nameLine = new String[0];
	int numOfBlock=0;
	String path;
	int currentIndex = 0;
	int currentBlockId = 0;
	public List<Tuple> tuplesInMemory = new ArrayList<>();
//	The size of each block is 4kb = 4096 byte, so each block can put 12 tuple 
//  one disk contain numOfBlock blocks
	public Relation(String path){
		tuples = new ArrayList<>();
		this.numOfBlock = 0;
		this.path = path;
		readInDisk(path);
	}
	
	
	public void open() {
		int fromIndex = 12*currentBlockId;
		int toIndex = fromIndex + 12;
		tuplesInMemory = tuples.subList(fromIndex, toIndex);
	}
	
	public Tuple getNext() {
		if(currentIndex < 12) {
			Tuple tmp = tuplesInMemory.get(currentIndex);
			currentIndex++;
			return tmp;
		} else {
			return null;
		}
	}
	
	public void close() {
		currentIndex = 0;
		currentBlockId++;
	}
	
//	read the block size of csv data from csv sheet and put each line in tuple
	 public List<Tuple> readInDisk(String path) {
			BufferedReader bufferedReader = null;
			String line = "";
			String cvsSplitBy = "  ";
			
			try {
				bufferedReader = new BufferedReader(new InputStreamReader(this.getClass().getResourceAsStream(path)));
				line = bufferedReader.readLine();
				nameLine = line.split(cvsSplitBy, -1);
				while ((line = bufferedReader.readLine()) != null) {
					String[] eachTuple = line.split(cvsSplitBy, -1);
						for(int i=0; i<nameLine.length;i++){
							Tuple tuple = new Tuple();
							tuple.put(nameLine[i], eachTuple[i]);
							tuples.add(tuple);
							
						}				
				}
			} catch (IOException e) {
				e.printStackTrace();
			}
		numOfBlock = (tuples.size()-1)/12 + 1;
 		return tuples;
	 }
	 
//	 each time,read 1 block contains 12 tuple from disk and record the place where to read next
	 public List<Tuple> readFromDisk(int blockId){

		if(numOfBlock == 0 || blockId > numOfBlock){
			return Collections.emptyList();
		}	
		int startIndex = (blockId-1)*12;
		return tuples.subList(startIndex, startIndex+12);
	 }
	 
}
