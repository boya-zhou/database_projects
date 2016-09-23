package log;

import java.util.*;
import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.io.FileReader;
import java.io.FileWriter;
import java.net.URISyntaxException;
import java.net.URL;
import java.io.File; 
import java.io.FileWriter;
import log.KeyNotFoundException;




public class Relation implements Serializable{
	
//make disk to save the whole relation considering block 
	private static final long serialVersionUID = 5279520027567712726L;	

    private List<Tuple> tuples;
    private String[] nameLine = new String[0];
    private String path;
    private int currentIndex = 0;
    private int currentBlockId = 0;
    private int blockNum=0;
    private List<Tuple> tuplesInMemory = new ArrayList<>();
    private File logFile=null;
	private int numOfBlock;
    
//	The size of each block is 4kb = 4096 byte, so each block can put 12 tuple 
//  one disk contain numOfBlock blocks
	public Relation(String path){
		tuples = new ArrayList<>();
		this.path = path;
		this.blockNum=blockNum;
		this.logFile = new File("/Users/boyazhou/Desktop/project 4（bzhou/"+path+".log");
		this.readInDisk(path+".db");
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
	
	public void update(String path){
		
		final String tmpPath = path;
		final int blockId=0;
		//read origin csv file
		try {
			final FileWriter log = new FileWriter(logFile, true);
			
//			List<Tuple> tuplesForUpdate=this.tuplesInMemory;
			while(numOfBlock!=blockId){
				this.readInDisk(path);
				this.open();
				try {
					log.write("START");
					int j=0;
					int countId=0;
					while(getNext()!= null){
						String oldValue= tuples.get(j).getValue("Population");
						double newValue= Float.valueOf(oldValue)*(1.02);
						tuples.get(j).put("population", String.valueOf(newValue));
						countId=blockId+j;
						log.write("<t0,"+countId+","+oldValue+","+newValue+">");
						j=j+1;
						if(j==12){
							break;
						}
					}
				log.write("COMMIT");
				
				} catch(Exception e) {
					e.printStackTrace();
				}
				
				saveToDisk(tmpPath);
				this.close();
			}
			log.flush();
			log.close();
		}catch(Exception e) {
			e.printStackTrace();
		}
	}
				/*
				Thread t0 = new Thread() {
					public void run() {
						this.setName("t0");
					try {
						log.write("<START t0>");
						int j=0;
						int countId=0;
						while(getNext()!= null){
							String oldValue= tuples.get(j).getValue("population");
							double newValue= Float.valueOf(oldValue)*(1.02);
							tuples.get(j).put("population", String.valueOf(newValue));
							countId=blockId+j;
							log.write("<t0,"+countId+","+oldValue+","+newValue+">");
							j=j+1;
							if(j==12){
								break;
							}
						}
					log.write("COMMIT");
					
					} catch(Exception e) {
						e.printStackTrace();
					}
					
					saveToDisk(tmpPath);
					}
			};
			
			Thread t1 = new Thread() {
				public void run() {
					this.setName("t1");
					try {
					log.write("<START t1>");
					int j=0;
					int countId=0;
					while(getNext()!=null){
						String oldValue = tuples.get(j).getValue("population");
						double newValue = Float.valueOf(oldValue)*(1.02);
						tuples.get(j).put("population", String.valueOf(newValue));
						countId=blockId+j;
						log.write("<t1,"+countId+","+oldValue+","+newValue+">");
						j=j+1;
						if(j==12){
							break;
						}
					}
					log.write("COMMIT");
					} catch(Exception e) {
						e.printStackTrace();
					}
					saveToDisk(tmpPath);
					}
			};
			
			t0.start();
			t1.start();	
			t0.join();
			t1.join();
			log.flush();
			log.close();
			} 
		}catch(Exception e) {
			e.printStackTrace();
		}
	}*/
	
	public void undoRedo(){
		// read log file
		Scanner log = null;
		try {
			log = new Scanner(logFile);
		} catch (FileNotFoundException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		List<Tuple> backUp = this.readInDisk(path + "A.db");

		boolean b = false;
		while (log.hasNextLine()) {
			String line = log.nextLine();
			ArrayList<String> logs = new ArrayList<>();

			if (line.equals("START")) {
				b = true;
				continue;
			}
			if (b) {
				logs.add(line);
				// come to the end of this commit
				if (line.equals("COMMIT")) {
					b = false;

					for (int j = 0; j < logs.size(); j++) {
						String logLine = logs.get(j);
						String[] logArr = logLine.split(",");
						boolean match = false;
						for (int i = 0; i < backUp.size() && !match; i++) {
							try {
								if (backUp.get(i).getValue("ID").equals(logArr[0])) {
									match = true;
									backUp.get(i).put("Population", logArr[3]);
								}
							} catch (KeyNotFoundException e) {
								// TODO Auto-generated catch block
								e.printStackTrace();
							}
						}
					}
					this.saveToDisk(path + "A");

				}
			}
		}

		// release all allocated resources
		log.close();
	}
//	read the block size of csv data from csv sheet and put each line in tuple
	 public List<Tuple> readInDisk(String path) {
			BufferedReader bufferedReader = null;
			String line = "";
			String cvsSplitBy = ",";
			
			try {
				bufferedReader = new BufferedReader(new InputStreamReader(this.getClass().getResourceAsStream(path)));
				line = bufferedReader.readLine();
				nameLine = line.split(cvsSplitBy, -1);
				while ((line = bufferedReader.readLine()) != null) {
					String[] eachTuple = line.split(cvsSplitBy, -1);
					Tuple tuple = new Tuple();	
					for(int i=0; i<nameLine.length;i++){
							
						tuple.put(nameLine[i].trim(), eachTuple[i]);
							
					}
					tuples.add(tuple);
				}
			} catch (IOException e) {
				e.printStackTrace();
			}
		numOfBlock = (tuples.size()-1)/12 + 1;
 		return tuples;
	 }
	 
	 public void saveToDisk(String path) {
			//  Write object with ObjectOutputStream
				ObjectOutputStream saveArrayDBToDisk;
				try {
					saveArrayDBToDisk = new ObjectOutputStream(new FileOutputStream(path));
				//  Write object out to disk
					saveArrayDBToDisk.writeObject(this);
					saveArrayDBToDisk.flush();
					saveArrayDBToDisk.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
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
