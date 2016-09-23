package Index;

/*void Put(string key, Number data_value); or void Put(string key, string data_value); adds the index entry.
 
string Get(Number data_value); or string Get(string data_value); retrieves the key given the index and

void Remove(string key); deletes the index.*/

public class IndexInterface<E extends Comparable<E>> {
	
	BPTree<E> bpTree;
	
	public IndexInterface(BPTree<E> bpTree) {
		this.bpTree = bpTree;
	}
	
	public void put(String key, E data_value){
		
		bpTree.insertNode(data_value, key);

	}
	
	@SuppressWarnings("unchecked")
	public String get(E data_value) {
		Node<E> nodeToFind=bpTree.search(data_value);
		String key = "";
		Node<E> tmp = nodeToFind;
		if(nodeToFind == null) {
			key = "Not Found!";
			return key;
		}
		while(nodeToFind.getNext() != null) {
			boolean b = false;
			for (int i = 0; i < nodeToFind.getKeyValue().size(); i++) {
	            if (data_value.compareTo(nodeToFind.getKeyValue().get(i)) == 0) {
	            	key +=(String) nodeToFind.getPointers().get(i) + " | ";
	            } else {
	            	b = true;
	            }
	        }
			if(b) {
				break;
			} else {
				nodeToFind = nodeToFind.getNext();
			}
			
		}
		nodeToFind = tmp.getPrev();
		while(nodeToFind.getPrev() != null) {
			boolean b = false;
			for (int i = 0; i < nodeToFind.getKeyValue().size(); i++) {
	            if (data_value.compareTo(nodeToFind.getKeyValue().get(i)) == 0) {
	            	key +=(String) nodeToFind.getPointers().get(i) + " | ";
	            } else {
	            	b = true;
	            	break;
	            }
	        }
			if(b) {
				break;
			} else {
				nodeToFind = nodeToFind.getPrev();
			}
		}
		return key;
	}
	
	public void remove(String key){
		Node<E> node = bpTree.getRoot();
		while(!node.isLeaf) {
			node = (Node<E>)node.getPointers().get(0);
		}
		Node<E> tmp = node;
		while(node.getNext() != null) {
			for(int i=0; i < node.getPointers().size(); i++) {
				String s = (String)node.getPointers().get(i);
				if(key.equals(s)) {
					bpTree.delete(node.getKeyValue().get(i), key);
				}
			}
			node = node.getNext();
		}
	}
}
