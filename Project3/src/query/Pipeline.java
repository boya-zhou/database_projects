package query;


import java.util.*;
import java.util.ArrayList;

// use as the pipeline, if pipeline is full,then pass to the next operator
// for simplicity, this case only has to use pipeline when pass the output from join to project, so only on kind of pipeline is defined
public class Pipeline{
		
		
	static int maxNumTuples = 12;
	public List<Tuple> tuples;
	public String[] nameLine = new String[0];
	int count = 0;
	
	public Pipeline(){
		tuples = new ArrayList<>();
		
	}
		
	 public void readInPipeline(List<Tuple> newTuple) {
		tuples = newTuple;
	 }
	 
	 public List<Tuple> readFromPipeline(){

		 return tuples;
	 }
	 
	 public int getCount() {
		 return count;
	 }

}
