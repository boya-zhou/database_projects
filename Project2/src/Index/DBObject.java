package Index;

import java.io.BufferedReader;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.ArrayList;

public class DBObject implements Serializable{

	private static final long serialVersionUID = -2141060261129697474L;

	private static final String fileLocation = "CS542.db";

	//Movies(Title, Year, Format, Genre, Director, Writer, Country, Studio, Price, Catalog No)
    public String title;
    public int year;
    public String format;
    public String genre;
    public String director;
    public String writer;
    public String country;
    public String studio;
    public String price;
    public String catalogNo;
    
	ArrayList<DBObject> listToStoreDB= new ArrayList<DBObject>();

//	write the arrayDB into CS542.db
	public void saveToDisk(ArrayList<DBObject> listToStoreDB) {
	//  Write object with ObjectOutputStream
		ObjectOutputStream saveArrayDBToDisk;
		URL url = this.getClass().getResource(fileLocation);
		try {
			saveArrayDBToDisk = new ObjectOutputStream(new FileOutputStream(url.toURI().getPath()));
		//  Write object out to disk
			saveArrayDBToDisk.writeObject(listToStoreDB);
			saveArrayDBToDisk.flush();
			saveArrayDBToDisk.close();
		} catch (IOException | URISyntaxException e) {
			e.printStackTrace();
		}
	}
	
	 public ArrayList<DBObject> read() {
			String csvFile = "movies.txt";
			BufferedReader bufferedReader = null;
			String line = "";
			String cvsSplitBy = ",";
			int i=0;
			try {
				bufferedReader = new BufferedReader(new InputStreamReader(this.getClass().getResourceAsStream(csvFile)));
				while ((line = bufferedReader.readLine()) != null) {
					DBObject dbObject = new DBObject();
					String[] movies = line.split(cvsSplitBy, -1);
					//Movies(Title, Year, Format, Genre, Director, Writer, Country, Studio, Price, Catalog No)

					dbObject.title=movies[0];
					dbObject.year= Integer.valueOf(movies[1]);
					dbObject.format=movies[2];
					dbObject.genre=movies[3];
					dbObject.director=movies[4];
					dbObject.writer=movies[5];
					dbObject.country=movies[6];
					dbObject.studio=movies[7];
					dbObject.price=movies[8];
					dbObject.catalogNo=movies[9];
					listToStoreDB.add(i,dbObject);
					i++;

				}
			} catch (IOException e) {
				e.printStackTrace();
			} 
			try {
				bufferedReader.close();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			saveToDisk(listToStoreDB);	

			return listToStoreDB;
			}
    
    public static void main(String[] args)  {
        
    }
}
