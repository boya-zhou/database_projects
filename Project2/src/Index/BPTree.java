package Index;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.ArrayList;
import java.util.LinkedList;

public class BPTree<E extends Comparable<E>> implements Serializable{

	/**
	 * 
	 */
	private static final long serialVersionUID = 8428149029504418297L;
	int fanOut;
    private Node<E> root;
    private int size;
    private BasicMethod<E> basicMethod;
    private LinkedList<Node<E>> buffer;
    private int bufferPoolSize;
	private static final String fileLocation = "CS542.db";

    public BPTree(int fanOut, int bufferbool) {
        super();
        this.fanOut = fanOut;
        this.root = new Node<E>(fanOut, true);
        basicMethod = new BasicMethod<E>();
        buffer = new LinkedList<Node<E>>();
        this.bufferPoolSize = bufferbool;
    }
    
    public Node<E> getRoot() {
        return root;
    }

    public void setRoot(Node<E> root) {
        this.root = root;
    }

    public void saveToDisk() {
    	//  Write object with ObjectOutputStream
    		ObjectOutputStream saveArrayDBToDisk;
    		URL url = this.getClass().getResource(fileLocation);
    		try {
    			saveArrayDBToDisk = new ObjectOutputStream(new FileOutputStream(url.toURI().getPath()));
    		//  Write object out to disk
    			saveArrayDBToDisk.writeObject(this);
    			saveArrayDBToDisk.flush();
    			saveArrayDBToDisk.close();
    		} catch (IOException | URISyntaxException e) {
    			e.printStackTrace();
    		}
    	}

    // -----------------------------search-------------------------------------
    @SuppressWarnings("unchecked")
	public Node<E> search(E data_value) {
        // if element is in the buffer pool
        for (int i = 0; i < buffer.size(); i++) {
            ArrayList<E> find = buffer.get(i).getKeyValue();
            if (find.contains(data_value)) {
                return buffer.get(i);
            }
        }
        // if the element isn't in buffer pool
        Node<E> nodeToFind = root;
        while (!nodeToFind.isLeaf) {
            //searching for the element
        	//less than the least key_value
            if (data_value.compareTo(nodeToFind.getKeyValue().get(0)) < 0) {
                nodeToFind = (Node<E>) nodeToFind.getPointers().get(0);
            //if larger than the largest key_value
            } else if (data_value.compareTo(nodeToFind.getKeyValue().get(nodeToFind.getKeyValue().size() - 1)) >= 0) {
            	nodeToFind = (Node<E>) nodeToFind.getPointers().get(nodeToFind.getPointers().size() - 1);
            } else {
                for (int i = 0; i < nodeToFind.getKeyValue().size() - 1; i++) {
                    if (data_value.compareTo(nodeToFind.getKeyValue().get(i)) >= 0 && data_value.compareTo(nodeToFind.getKeyValue().get(i + 1)) < 0 && nodeToFind.getKeyValue().size() > 1) {
                        nodeToFind = (Node) nodeToFind.getPointers().get(i + 1);
                        if(nodeToFind.isLeaf){
                        	break;
                        }
                    }
                }
            }
        }
        // adding new node to buffer pool
        for (int i = 0; i < nodeToFind.getKeyValue().size(); i++) {
            if (data_value.compareTo(nodeToFind.getKeyValue().get(i)) == 0) {
            	//if buffer is full
                if (buffer.size() == bufferPoolSize) {
                    buffer.removeFirst();
                    buffer.add(nodeToFind);
                } else {
                    buffer.add(nodeToFind);
                }
                return nodeToFind;
            }
        }
        return null;
    }
    
    // -------------------------insert----------------------------------
    @SuppressWarnings("unchecked")
    public void insertNode(E keyValue, Object data) {
        // record the trace of search
        LinkedList<Node<E>> nodeTrace = new LinkedList<Node<E>>();
        Node<E> nodeToFind = root;
        //search for the keyValue
        while (!nodeToFind.isLeaf) {
            nodeTrace.push(nodeToFind);
            //less than the least key_value
            if (keyValue.compareTo(nodeToFind.getKeyValue().get(0)) < 0) {  
                nodeToFind = (Node<E>) nodeToFind.getPointers().get(0);
            //if larger than the largest key_value
            } else if (keyValue.compareTo(nodeToFind.getKeyValue().get(nodeToFind.getKeyValue().size() - 1)) >= 0) {
                nodeToFind = (Node<E>) nodeToFind.getPointers().get(nodeToFind.getPointers().size() - 1);
            } else {
                for (int i = 0; i < nodeToFind.getKeyValue().size() - 1; i++) { 
                    if (nodeToFind.getKeyValue().size() > 1 && keyValue.compareTo(nodeToFind.getKeyValue().get(i)) >= 0 && keyValue.compareTo(nodeToFind.getKeyValue().get(i + 1)) < 0) {
                        nodeToFind = (Node) nodeToFind.getPointers().get(i + 1);
                        if(nodeToFind.isLeaf){
                        break;
                        }
                    }
                }
            }
        }
        // check if the keyValue in the node or not
        for (int i = 0; i < nodeToFind.getKeyValue().size(); i++) {
            if (keyValue == nodeToFind.getKeyValue().get(i)) {
                return;
            }
        }
        // if node is not full
        if (nodeToFind.getKeyValue().size() < fanOut) {
            basicMethod.insert(keyValue, data, nodeToFind);
        } else {
            // split two leaf nodes
            // copying all current node contents in tmp node then insert the new element on it
            Node<E> temp = new Node(fanOut, true);
            temp.setKeys(new ArrayList<E>(nodeToFind.getKeyValue()));
            temp.setPointers(new ArrayList<Object>(nodeToFind.getPointers()));
            basicMethod.insert(keyValue, data, temp);
            Node newNode = new Node(fanOut, true);
            int j = (int) Math.ceil(nodeToFind.getPointers().size() / (double) 2);
            //take the first half of the tmp nde in current node
            nodeToFind.setKeys(new ArrayList<E>(temp.getKeyValue().subList(0, j)));
            nodeToFind.setPointers(new ArrayList<Object>(temp.getPointers().subList(0, j)));
            // next and pre
            if (nodeToFind.getNext() != null) {
                nodeToFind.getNext().setPrev(newNode);
            }
            newNode.setNext(nodeToFind.getNext());
            nodeToFind.setNext(newNode);
            // copying the rest of tmp node in new node
            newNode.setPrev(nodeToFind);
            newNode.setKeys(new ArrayList<E>(temp.getKeyValue().subList(j, temp.getKeyValue().size())));
            newNode.setPointers(new ArrayList<Object>(temp.getPointers().subList(j, temp.getPointers().size())));
            // keeping the key that will be inserting in parent node
            keyValue = temp.getKeyValue().get(j);
            boolean finished = false;
            do {
                // if the parent is null (root case)
                if (nodeTrace.isEmpty()) {
                    root = new Node(fanOut, false);
                    ArrayList<Object> point = new ArrayList<Object>();
                    point.add(nodeToFind);
                    point.add(newNode);
                    ArrayList<E> keys_ = new ArrayList<E>();
                    keys_.add(keyValue);
                    root.setKeys(keys_);
                    root.setPointers(point);
                    finished = true;
                } else {
                    // if there's parent
                    nodeToFind = nodeTrace.pop();
                    // if there's no need for splitting internal
                    if (nodeToFind.getKeyValue().size() < fanOut) {
                        basicMethod.insertInternal(keyValue, newNode, nodeToFind);
                        finished = true;
                    } else {
                        /* splitting two internal nodes by copying them into new node and insert
                        new element in the tmp node then divide it between current node and new node
                         */
                        temp.setLeaf(false);
                        temp.setKeys(new ArrayList<E>(nodeToFind.getKeyValue()));
                        temp.setPointers(new ArrayList<Object>(nodeToFind.getPointers()));

                        basicMethod.insertInternal(keyValue, newNode, temp);
                        newNode = new Node(fanOut, false);
                        j = (int) Math.ceil(temp.getPointers().size() / (double) 2);

                        nodeToFind.setKeys(new ArrayList<E>(temp.getKeyValue().subList(0, j - 1)));
                        nodeToFind.setPointers(new ArrayList<Object>(temp.getPointers().subList(0, j)));
                        if (nodeToFind.getNext() != null) {
                            nodeToFind.getNext().setPrev(newNode);
                        }
                        newNode.setNext(nodeToFind.getNext());
                        nodeToFind.setNext(newNode);
                        newNode.setPrev(nodeToFind);
                        newNode.setKeys(new ArrayList<E>(temp.getKeyValue().subList(j, temp.getKeyValue().size())));
                        newNode.setPointers(new ArrayList<Object>(temp.getPointers().subList(j, temp.getPointers().size())));

                        keyValue = temp.getKeyValue().get(j - 1);
                    }
                }
            } while (!finished);
        }
    }

    

    // -------------------------delete----------------------------------

    @SuppressWarnings("unchecked")
    public void delete(E key, Object dataValue) {
        LinkedList<Node<E>> stack = new LinkedList<Node<E>>();
        Node<E> n = root;
        //searching for the required node
        while (!n.isLeaf) {
            stack.push(n);
            // ===================================================
            if (key.compareTo(n.getKeyValue().get(0)) < 0) {
                n = (Node<E>) n.getPointers().get(0);
            } else if (key.compareTo(n.getKeyValue().get(n.getKeyValue().size() - 1)) >= 0) {
                n = (Node) n.getPointers().get(n.getPointers().size() - 1);
            } else {
                for (int i = 0; i < n.getKeyValue().size(); i++) {
                    if (key.compareTo(n.getKeyValue().get(i)) >= 0 && key.compareTo(n.getKeyValue().get(i + 1)) < 0) {
                        n = (Node) n.getPointers().get(i + 1);
                        break;
                    }
                }
            }
        }
        // END OF WHILE
        boolean flag = false;
        for (int i = 0; i < n.getKeyValue().size(); i++) {
            if (n == root && key == n.getKeyValue().get(i)) {
                basicMethod.deleteNode(n, key, dataValue);
                return;
            } else if (key == n.getKeyValue().get(i)) {
                flag = true;
                break;
            }
        }
        //searching to determine if the element is found in leaf node or not
        if (flag) {
            //if the node isn't under flow
            if (n.getKeyValue().size() - 1 >= Math.ceil(fanOut / 2.0)) {
                basicMethod.deleteNode(n, key, dataValue);
                Node<E> parent = stack.peek();
                for (int i = 0; i < parent.getKeyValue().size(); i++) {
                    if (key.compareTo(parent.getKeyValue().get(i)) == 0) {
                        parent.getKeyValue().set(i, n.getKeyValue().get(0));
                        break;
                    }
                }
            } else {
                // if node is in underflow
                Node<E> parent = stack.peek();
                // determine if the next node is from the same parent or not to borrow from it
                int deter = basicMethod.sameParent(n, stack.peek(), fanOut);
                // if next from the same parent
                if (deter == 1) {
                    // delete the node
                    basicMethod.deleteNode(n, key, dataValue);
                    // borrow from the next leaf node
                    E element = n.getNext().getKeyValue().remove(0);
                    Object obj = n.getNext().getPointers().remove(0);
                    n.getKeyValue().add(element);
                    n.getPointers().add(obj);
                    for (int i = 0; i < parent.getKeyValue().size(); i++) {
                        if (element.compareTo(parent.getKeyValue().get(i)) == 0) {
                            parent.getKeyValue().set(i, n.getNext().getKeyValue().get(0));
                            break;
                        }
                    }
                    for (int i = 0; i < parent.getKeyValue().size(); i++) {
                        if (key.compareTo(parent.getKeyValue().get(i)) == 0) {
                            parent.getKeyValue().set(i, n.getKeyValue().get(0));
                            break;
                        }
                    }
                    return;
                } else if (deter == 2) {
                    // borrow from the previous node
                    basicMethod.deleteNode(n, key, dataValue);
                    E element = n.getPrev().getKeyValue().remove(n.getPrev().getKeyValue().size() - 1);
                    Object obj = n.getPrev().getPointers().remove(n.getPrev().getPointers().size() - 1);
                    n.getKeyValue().add(0, element);
                    n.getPointers().add(0, obj);
                    for (int i = 0; i < parent.getKeyValue().size(); i++) {
                        if (element.compareTo(parent.getKeyValue().get(i)) == 0) {
                            parent.getKeyValue().set(i, n.getPrev().getKeyValue().get(n.getPrev().getKeyValue().size() - 1));
                            break;
                        }
                    }
                    for (int i = 0; i < parent.getKeyValue().size(); i++) {
                        if (key.compareTo(parent.getKeyValue().get(i)) == 0) {
                            parent.getKeyValue().set(i, n.getKeyValue().get(0));
                            break;
                        }
                    }
                    return;
                } else {
                    // there will be merging for internal nodes
                    boolean prevB = true;
                    if (key == n.getKeyValue().get(0)) {
                        prevB = false;
                    }

                    basicMethod.deleteNode(n, key, dataValue);
                    int tempKey = 0;
                    int tempPointer = 0;
                    // if the merging will be with the next node
                    // then copying all elements of the current node in the next node
                    // delete the first element from the next node in the parent node
                    if (basicMethod.sameParent2(parent, n)) {
                        Node<E> next = n.getNext();
                        if (n.getPrev() != null) {
                            n.getPrev().setNext(next);
                        }
                        if (next != null) {
                            next.setPrev(n.getPrev());
                        }
                        n.setNext(null);
                        n.setPrev(null);
                        next.getKeyValue().addAll(0, n.getKeyValue());
                        next.getPointers().addAll(0, n.getPointers());
                        for (int i = 0; i < parent.getKeyValue().size(); i++) {
                            if (next.getKeyValue().get(n.getKeyValue().size()).compareTo(parent.getKeyValue().get(i)) == 0) {
                                tempKey = i;
                                tempPointer = i;
                                break;
                            }
                        }
                        if (tempKey > 0 && parent.getKeyValue().get(tempKey - 1) == key) {
                            parent.getKeyValue().set(tempKey - 1, next.getKeyValue().get(0));
                        }
                    } else {
                        // if the merging will be with the prev node
                        // then copying all elements of the node in the prev node
                        // delete the first element from the current node in the parent node
                        Node<E> prev = n.getPrev();
                        if (prev != null) {
                            prev.setNext(n.getNext());
                        }
                        if (n.getNext() != null) {
                            n.getNext().setPrev(prev);
                        }
                        n.setNext(null);
                        n.setPrev(null);
                        prev.getKeyValue().addAll(n.getKeyValue());
                        prev.getPointers().addAll(n.getPointers());
                        if (prevB) {
                            for (int i = 0; i < parent.getKeyValue().size(); i++) {
                                if (n.getKeyValue().get(0).compareTo(parent.getKeyValue().get(i)) == 0) {
                                    tempKey = i;
                                    tempPointer = i + 1;
                                    break;
                                }
                            }
                        } else {
                            for (int i = 0; i < parent.getKeyValue().size(); i++) {
                                if (key.compareTo(parent.getKeyValue().get(i)) == 0) {
                                    tempKey = i;
                                    tempPointer = i + 1;
                                    break;
                                }
                            }
                        }
                    }
                    boolean finished = false;
                    do {
                        // if we get the root
                        if (stack.isEmpty()) {
                            root.getKeyValue().remove(tempKey);
                            root.getPointers().remove(tempPointer);
                            finished = true;
                        } else {
                            n = stack.pop();
                            //try borrowing from the sibling
                            if (n.getKeyValue().size() - 1 >= 1) {
                                n.getKeyValue().remove(tempKey);
                                n.getPointers().remove(tempPointer);
                                finished = true;
                            } else {
                                // if the root have one sibling
                                // the tree level will decrease
                                if (n == root) {
                                    n.getKeyValue().remove(tempKey);
                                    n.getPointers().remove(tempPointer);
                                    if (n.getPointers().size() == 1) {
                                        root = (Node<E>) n.getPointers().get(0);
                                    }
                                    finished = true;
                                } else {
                                    n.getKeyValue().remove(tempKey);
                                    n.getPointers().remove(tempPointer);
                                    deter = basicMethod.nexOrPrev(n, stack.peek(), fanOut);
                                    parent = stack.peek();
                                    // borrowing from next internal node
                                    if (deter == 1) {
                                        int index = -1;
                                        for (int i = 0; i < parent.getPointers().size(); i++) {
                                            if (parent.getPointers().get(i) == n.getNext()) {
                                                index = i;
                                                break;
                                            }
                                        }
                                        E tempkey = parent.getKeyValue().remove(index - 1);
                                        n.getKeyValue().add(tempkey);
                                        Node<E> tempNext = (Node<E>) n.getNext().getPointers().remove(0);
                                        E nextKey = n.getNext().getKeyValue().remove(0);
                                        n.getPointers().add(tempNext);
                                        parent.getKeyValue().add(index - 1, nextKey);
                                        finished = true;
                                        // borrow from prev internal node
                                    } else if (deter == 2) {
                                        int index = -1;
                                        for (int i = 0; i < parent.getPointers().size(); i++) {
                                            if (parent.getPointers().get(i) == n) {
                                                index = i;
                                                break;
                                            }
                                        }
                                        E tempkey = parent.getKeyValue().remove(index - 1);
                                        n.getKeyValue().add(0, tempkey);
                                        Node<E> tempPrev = (Node<E>) n.getPrev().getPointers().remove(n.getPrev().getPointers().size() - 1);
                                        E prevKey = n.getPrev().getKeyValue().remove(n.getPrev().getKeyValue().size() - 1);
                                        n.getPointers().add(0, tempPrev);
                                        parent.getKeyValue().add(index - 1, prevKey);
                                        finished = true;
                                    } else {
                                        // merge two internal nodes
                                        if (basicMethod.sameParent2(parent, n)) {
                                            for (int i = 0; i < parent.getPointers().size(); i++) {
                                                if (n == parent.getPointers().get(i)) {
                                                    tempKey = i;
                                                    tempPointer = i;
                                                    break;
                                                }
                                            }
                                            Node<E> next = n.getNext();
                                            if (n.getPrev() != null) {
                                                n.getPrev().setNext(next);
                                            }
                                            if (next != null) {
                                                next.setPrev(n.getPrev());
                                            }
                                            next.getKeyValue().add(0, parent.getKeyValue().get(tempKey));
                                            next.getKeyValue().addAll(0, n.getKeyValue());
                                            next.getPointers().addAll(0, n.getPointers());

                                        } else {
                                            for (int i = 0; i < parent.getPointers().size(); i++) {
                                                if (n == parent.getPointers().get(i)) {
                                                    tempKey = i - 1;
                                                    tempPointer = i;
                                                    break;
                                                }
                                            }
                                            Node<E> prev = n.getPrev();
                                            if (prev != null) {
                                                prev.setNext(n.getNext());
                                            }
                                            if (n.getNext() != null) {
                                                n.getNext().setPrev(prev);
                                            }
                                            prev.getKeyValue().add(parent.getKeyValue().get(tempKey));
                                            prev.getKeyValue().addAll(n.getKeyValue());
                                            prev.getPointers().addAll(n.getPointers());
                                        }
                                    }
                                }
                            }
                        }
                    } while (!finished);

                }
            }
        } else { 
        	// if the element isn't found
            return;
        }
    }
    
    
    
    public static void main(String[] args) {
        
    }
}
