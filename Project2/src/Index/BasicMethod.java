package Index;

import java.io.Serializable;
import java.util.ArrayList;


public class BasicMethod<E extends Comparable<E>> implements Serializable {

    /**
	 * 
	 */
	private static final long serialVersionUID = 3522296078907930861L;

	public void insert(E data_value, Object key, Node<E> nodeToFind) {
        ArrayList<E> keyValue = nodeToFind.getKeyValue();
        ArrayList<Object> pointer = nodeToFind.getPointers();
        if (keyValue.isEmpty()) {
            keyValue.add(data_value);
            pointer.add(key);
            return;
        } else {
            if (data_value.compareTo(keyValue.get(0)) < 0) {
                keyValue.add(0, data_value);
                pointer.add(0, key);
            } else {
                boolean bool = true;
                for (int i = 0; i < keyValue.size(); i++) {
                    if (data_value.compareTo(keyValue.get(i)) < 0) {
                        bool = false;
                        keyValue.add(i, data_value);
                        pointer.add(i, key);
                        break;
                    }
                }
                if (bool) {
                    keyValue.add(data_value);
                    pointer.add(key);
                }
            }
        }
    }
   
    
    public void insertInternal(E data_value, Object key, Node<E> nodeToFind) {
        ArrayList<E> keyValue = nodeToFind.getKeyValue();
        ArrayList<Object> pointer = nodeToFind.getPointers();
        if (data_value.compareTo(keyValue.get(0)) < 0) {
            keyValue.add(0, data_value);
            pointer.add(1, key);
        } 
        else {
            boolean bool = true;
            for (int i = 0; i < keyValue.size(); i++) {
                if (data_value.compareTo(keyValue.get(i)) < 0) {
                    keyValue.add(i, data_value);
                    pointer.add(i + 1, key);
                    bool = false;
                    break;
                }
            }
            if (bool) {
                keyValue.add(data_value);
                pointer.add(key);
            }
        }
    }

    public void deleteNode(Node<E> nodeToFind, E data_value, Object key) {
        for (int i = 0; i < nodeToFind.getKeyValue().size(); i++) {
            if (data_value.compareTo(nodeToFind.getKeyValue().get(i)) == 0 && key.equals(nodeToFind.getPointers().get(i))) {
                nodeToFind.getKeyValue().remove(i);
                nodeToFind.getPointers().remove(i);
            }
        }
    }

    public void internalDelete(E data_value, Node<E> nodeToFind, Node<E> tempNode) {
        for (int i = 0; i < nodeToFind.getKeyValue().size(); i++) {
            if (nodeToFind.getKeyValue().get(i).compareTo(data_value) == 0) {
                nodeToFind.getKeyValue().remove(i);
                nodeToFind.getPointers().remove(i + 1);
            }
        }
    }

    public int sameParent(Node<E> n, Node<E> parent, int size) {
        ArrayList<E> keys = parent.getKeyValue();
        boolean isNext = false;
        boolean isPrev = false;
        Node<E> next = n.getNext();
        Node<E> prev = n.getPrev();
        if (sameParent2(parent, n)) {
            for (int i = 0; i < parent.getPointers().size(); i++) {
                if (next == parent.getPointers().get(i)) {
                    isNext = true;
                    break;
                }
            }
        }
        if (!sameParent2(parent, n)) {
            for (int i = 0; i < parent.getPointers().size(); i++) {
                if (prev == parent.getPointers().get(i)) {
                    isPrev = true;
                    break;
                }
            }
        }
        if (isNext && next.getKeyValue().size() - 1 >= Math.ceil(size / 2.0)) {
            return 1;
        } else if (isPrev && prev.getKeyValue().size() - 1 >= Math.ceil(size / 2.0)) {
            return 2;
        } else {
            return 0;
        }
    }

    public int nexOrPrev(Node<E> n, Node<E> parent, int size) {
        boolean isNext = false;
        boolean isPrev = false;
        Node<E> next = n.getNext();
        Node<E> prev = n.getPrev();
        if (next != null) {
            for (int i = 0; i < parent.getPointers().size(); i++) {
                if (next == parent.getPointers().get(i)) {
                    isNext = true;
                    break;
                }
            }
        }
        if (prev != null) {
            for (int i = 0; i < parent.getPointers().size(); i++) {
                if (prev == parent.getPointers().get(i)) {
                    isPrev = true;
                    break;
                }
            }
        }
        if (next != null && isNext && next.getKeyValue().size() - 1 >= 1) {
            return 1;
        } else if (prev != null && isPrev && prev.getKeyValue().size() - 1 >= 1) {
            return 2;
        } else {
            return 0;
        }
    }

    public boolean sameParent2(Node<E> parent, Node<E> n) {
        boolean isNext = false;
        boolean isPrev = false;
        Node<E> next = n.getNext();
        Node<E> prev = n.getPrev();
        if (next != null) {
            for (int i = 0; i < parent.getPointers().size(); i++) {
                if (next == parent.getPointers().get(i)) {
                    isNext = true;
                    break;
                }
            }
        }
        if (prev != null) {
            for (int i = 0; i < parent.getPointers().size(); i++) {
                if (prev == parent.getPointers().get(i)) {
                    isPrev = true;
                    break;
                }
            }
        }
        if (isNext) {
            return true;
        } else {
            return false;
        }


    }
}
