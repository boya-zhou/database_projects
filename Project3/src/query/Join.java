package query;

import java.util.ArrayList;
import java.util.List;

import com.sun.xml.internal.ws.policy.privateutil.PolicyUtils.Collections;

public class Join {
	
	private Relation leftRelation = null;
    private Relation rightRelation = null;
    
    private List<Tuple> leftTuples = null;
    private List<Tuple> rightTuples = null;
    private Tuple leftTuple = null;
    private Tuple rightTuple = null;
    int leftBlockId = 0;
    int rightBlockId = 0;
    
	List<Tuple> results = new ArrayList<>();
	Pipeline pipeline = new Pipeline();

    public Join(Relation rOnDisk1, Relation rOnDisk2) {

        this.leftRelation = rOnDisk1;
        this.rightRelation = rOnDisk2;
        leftRelation.open();
        rightRelation.open();
        
    }

    

	public List<Tuple> getNext() throws KeyNotFoundException {
        
		leftTuple = leftRelation.getNext();
		if(leftTuple == null) {
			return results;
		}
		rightRelation.open();

        results = null;
        while (true) {
        	rightTuple = rightRelation.getNext();
        	
            if (rightTuple==null) {
                rightRelation.close();
                
                break;
            }

            // if this two tuples meet the relation
			if (Integer.valueOf(leftTuple.getValue("population").toString()) < 0.4
					* Integer.valueOf((rightTuple.getValue("population").toString()))
					&& leftTuple.getValue("code").toString().equals(rightTuple.getValue("countrycode").toString())) {
				Tuple tmp = new Tuple();
				for(int i =0; i < leftTuple.attributeName.size(); i++) {
					tmp.put(leftTuple.attributeName.get(i), leftTuple.getValue(leftTuple.attributeName.get(i)));
				}
				for(int i =0; i < rightTuple.attributeName.size(); i++) {
					tmp.put(rightTuple.attributeName.get(i), rightTuple.getValue(rightTuple.attributeName.get(i)));
				}
				results.add(tmp);
			}
        }
        return getNext();
        
    }
	
	public void close(){
		pipeline.readInPipeline(results);;
		
		
	}
}
