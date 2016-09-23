package query;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class Project {
	
	// project from other operator
	
	public List<Tuple> project(String[] name,Pipeline tank) throws KeyNotFoundException{
		List<Tuple> tupleFromPipeline = tank.readFromPipeline();
		Tuple tmp = new Tuple();
		List<Tuple> newTuples = new ArrayList();
		
		for(int i =0; i < name.length; i++) {
			for( int index =0;index< tupleFromPipeline.size();index++){
				tmp.put(name[i],tupleFromPipeline.get(index).getValue(name[i]));
				newTuples.add(tmp);
			}
			
		}
		
		return newTuples;
	}
	
}
