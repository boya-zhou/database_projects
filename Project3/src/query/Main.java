package query;

import java.util.List;

import query.Join;
import query.Tuple;
import query.Relation;
import query.Pipeline;

public class Main {
    public static void main(String args[]) {
        try {
            Relation country = new Relation("country.csv");
            country.open();
            Relation city = new Relation("city.csv");
            city.open();
            Join join = new Join(country, city);
            Project projectAttri = new Project();
            Pipeline pipeline = new Pipeline();
            while (true) {
            	List<Tuple> records = join.getNext();
                if (records==null) {
                	join.close();
                    break;
                }
                List<Tuple> recordProject = projectAttri.project(new String[]{"Name", "Population"},pipeline);
                System.out.println(recordProject);
            }
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(1);
        }
        System.exit(0);
    }
}
