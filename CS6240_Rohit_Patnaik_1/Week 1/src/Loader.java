import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class Loader {
	public static List<String> loaderR(String csvFile){
	//	 String csvFile = "C:/Users/Rohit Patnaik/Documents/Parallel Data Processing - Map Reduce/HW1/testing1.csv";
	        String line = "";
	        List<String> res = new ArrayList<String>();
	        try (BufferedReader br = new BufferedReader(new FileReader(csvFile))) {

	            while ((line = br.readLine()) != null) {
	            	res.add(line);
	            }
	            
	        } catch (IOException e) {
	            e.printStackTrace();
	        }
			return res;
	}
	
    

}