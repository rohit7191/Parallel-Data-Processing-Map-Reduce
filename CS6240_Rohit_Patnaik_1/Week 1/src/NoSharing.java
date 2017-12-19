import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

class NoSharingThread extends Thread{
	public Map<String, List<Double>> hm = new HashMap<String,List<Double>>(); //Map with Station ID as key, Sum of TMAX and count of StationID as value(List<Double>)
	public List<String> res;
	
	public NoSharingThread(List<String> res,Map<String, List<Double>> map){
		this.res = res;
		this.hm = map;
	}
	
	public NoSharingThread(){
		
	}
	
	public Map<String, List<Double>> sumOfTmax(){
		return hm;
	}
	
	public void run() {
		
		for(int i=0; i<res.size(); i++) {
			String oneline = res.get(i);
			String [] splitArr = oneline.split(",");	
			
			/*Condition to check if the third field contains the value TMAX
			If the map is empty, we just add the station id as key and TMAX and count as 1 as value	
			If the map already has the station id, we add the current sum with the existing TMAX value in the map and increase the count*/
				
			if(splitArr[2].equalsIgnoreCase("TMAX")) {
				if(!hm.containsKey(splitArr[0])){
					double count = 1;
					double val = Integer.parseInt(splitArr[3]);
					List<Double> list = new ArrayList<>();
					list.add(val);
					list.add(count);
					hm.put(splitArr[0], list);
				}
				else
					{
					List<Double> lst = new ArrayList<>();
					lst = hm.get(splitArr[0]);
					double val = lst.get(0);
					double count = lst.get(1);
					val += Integer.parseInt(splitArr[3]);
					count++;
					lst.clear();
					lst.add(val);
					lst.add(count);
					hm.put(splitArr[0],lst);
				}
			}
		}
		
	}
}

public class NoSharing extends Loader{
	//Two different data structures for two different threads
	public static Map<String, List<Double>> hm1 = new HashMap<String,List<Double>>();
	public static Map<String, List<Double>> hm2 = new HashMap<String,List<Double>>();
	
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		String csvFile = "src/1912.csv";
		
		List<String> res = loaderR(csvFile);
		long initial_time = System.currentTimeMillis();
		int inputLength = res.size();
		
		NoSharingThread td1 = new NoSharingThread(res.subList(0, inputLength/2),hm1);
		NoSharingThread td2 = new NoSharingThread(res.subList(inputLength/2, inputLength), hm2);
		
		Thread t1 = new Thread(td1);
		t1.start();
		
		Thread t2 = new Thread(td2);
		t2.start();
		
		try {
			t1.join();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		try {
			t2.join();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		hm1 = td1.sumOfTmax();
		hm2 = td2.sumOfTmax();
		
		//Combining the Data Structures
		for(String key : hm2.keySet()) {
			if(hm1.containsKey(key)) {
				List<Double> l1 = hm1.get(key);
				List<Double> l2 = hm2.get(key);
				double sum = l1.get(0) + l2.get(0);
				double count = l1.get(1) + l2.get(1);
				List<Double> li = new ArrayList<>();
				li.add(sum);
				li.add(count);
				hm1.put(key, li);
			}
			else{
				hm1.put(key, hm2.get(key));
			}
		}
		
		//Calculating TMAX Average
		Map<String,Double> fmap = new HashMap<String,Double>();
		for(String key : hm1.keySet()) {
			List<Double> li = new ArrayList<>();
			li = hm1.get(key);
			double tmaxAvg = li.get(0)/li.get(1);
			fmap.put(key, tmaxAvg);
			
		}
		
		
		long final_time = System.currentTimeMillis();
		long exec_time = final_time - initial_time;
		System.out.println(exec_time);
	}

}