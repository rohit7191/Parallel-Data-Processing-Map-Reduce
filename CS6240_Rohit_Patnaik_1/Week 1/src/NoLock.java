import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

class NoLockThread extends Thread{

	public List<String> res;
	public static Map<String, List<Double>> hm = new HashMap<String,List<Double>>(); //Map with Station ID as key, Sum of TMAX and count of StationID as value(List<Double>)
	
	public NoLockThread(List<String> res){
		this.res = res;
	}
	
	public NoLockThread(){
		
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
				
			if(splitArr[2].equals("TMAX")) {
				if(!hm.containsKey(splitArr[0])){
					double count = 1;
					double val = Integer.parseInt(splitArr[3]);
					List<Double> lst = new ArrayList<>();
					lst.add(val);
					lst.add(count);
					hm.put(splitArr[0], lst);
				}
				else
					{
					List<Double> lst = new ArrayList<>();
					lst = hm.get(splitArr[0]);
					double val = lst.get(0);
					double count = lst.get(1);
					val += Integer.parseInt(splitArr[3]);
					count++;
					List<Double> li = new ArrayList<>();
					li.add(val);
					li.add(count);
					hm.put(splitArr[0],li);
				}
			}
		}
	}
}

public class NoLock extends Loader{

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		String csvFile = "src/1912.csv";
		List<String> res = loaderR(csvFile);
		long initial_time = System.currentTimeMillis();
		int inputLength = res.size();
		
		NoLockThread td1 = new NoLockThread(res.subList(0, inputLength/2));
		NoLockThread td2 = new NoLockThread(res.subList(inputLength/2, inputLength));
		
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
		
		NoLockThread o = new NoLockThread();
		Map<String,List<Double>> hm  = o.sumOfTmax();
		Map<String,Double> map = new HashMap<String,Double>();
		//Calculating TMAX Average
		for(String key : hm.keySet()) {
			List<Double> li = new ArrayList<>();
			li = hm.get(key);
			double tmaxAvg = li.get(0)/li.get(1);
			map.put(key, tmaxAvg);
		}
		long final_time = System.currentTimeMillis();
		long exec_time = final_time - initial_time;
		System.out.println(exec_time);
	}

}