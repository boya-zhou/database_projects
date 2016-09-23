package log;

import java.util.List;


import log.Relation;
import log.Tuple;

public class Main {
	public static void main(String args[]) {
        try {
            Relation city = new Relation("city");
            city.update("city.db");
            city.undoRedo();
            Relation country = new Relation("country");
            country.update("country.db");
            country.undoRedo();
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(1);
        }
        //TODO:check if the two are same 
    }
}
