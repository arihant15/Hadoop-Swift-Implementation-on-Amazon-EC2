import java.util.Comparator;
import java.util.HashMap;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;


public class SortHelper {

	
	public TreeMap<String , Integer> sortMap(ConcurrentHashMap<String, Integer> hm)
	{
		MapSorter ms= new MapSorter(hm);
		TreeMap<String , Integer> linkedMap=new TreeMap<String,Integer>(ms);
		linkedMap.putAll(hm);
		ms=null;
		return linkedMap;
	}
}


class MapSorter implements Comparator
{
	
	ConcurrentHashMap<String, Integer> hm=null;
	MapSorter(ConcurrentHashMap<String, Integer> hm2)		
	{
			this.hm=hm2;
	}
		@Override
		public int compare(Object a, Object b) {
			// TODO Auto-generated method stub
			if(hm.get(a)>hm.get(b))
			{
				return -1;
			}
			else
			{
				return 1;
			}
			
		}
}