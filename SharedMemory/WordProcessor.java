
import java.io.IOException;
import java.io.LineNumberReader;

import java.nio.charset.CharsetDecoder;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Scanner;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


public class WordProcessor  implements Runnable{
	
	/* Static Variable Declaration*/
	static ConcurrentHashMap<String , Integer> hm= new ConcurrentHashMap<String , Integer>();	
	static ConcurrentHashMap<Integer,String> lineCheck=new ConcurrentHashMap<Integer,String>();
	static Path path=null;
	static LineNumberReader lr=null;

	public void run()
	{	
		try {		
			String line=null;
			System.out.println("Runnning Thread "+Thread.currentThread().getName()+"");
			if(lineCheck.isEmpty() && lineCheck!=null )
				{		
						line = lr.readLine();
						addLine(lr.getLineNumber(),Thread.currentThread().getName());
				}
			
			while(lr !=null  &&line != null )
			{	
				if(!lineCheck.containsKey(lr.getLineNumber()))
				{	addLine(lr.getLineNumber(),Thread.currentThread().getName());
					tokenizeString(line);
					line = lr.readLine();
				}
				else{
					lr.skip(lr.getLineNumber());
				}
				
			}
			System.out.println("Run complete: "+ Thread.currentThread().getName());
		
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	public static void main(String arg[])
	{
		
		long t1=0,t2=0;
		String filePath=arg[0];
		path=FileSystems.getDefault().getPath(filePath);
		System.out.println("File Path:"+filePath);
		System.out.println("Length of file "+path.toFile().length());
		int nt=Integer.parseInt(arg[1]);
		System.out.println("No of Threads:"+nt);
		Thread t[]=new Thread[nt];
		
		CharsetDecoder decipher = StandardCharsets.UTF_8.newDecoder();
		decipher.onMalformedInput(CodingErrorAction.IGNORE);
		
		try {
			long start=System.currentTimeMillis();
			lr= new LineNumberReader(Files.newBufferedReader(path,decipher.charset()));
			lr.setLineNumber(0);
		
			for(int i=0;i<nt;i++)
			{	t[i]=new Thread(new WordProcessor(),"Thread"+(i));
			}
			System.out.println("starting thread...");

			for(int j=0;j<nt;j++)
			{	t[j].start();
			
				System.out.println("Thread"+(j)+" Started");
			
			}
			System.out.println("joining thread...");

			for(int k=0;k<nt;k++)
			{	t[k].join();
			System.out.println("Thread"+(k)+" joined");
		
			}
		
			long total=System.currentTimeMillis()-start;

			System.out.println("########################################################################");
			System.out.println("\nWord Count Hash map created");
			System.out.println("\nTotal time taken to map#(millisec):"+total);
			System.out.println("\n########################################################################");
	
			System.out.println("Free memory (bytes): " + 
					  Runtime.getRuntime().freeMemory());
			System.out.println("\nSorting hash map");
			t1=System.currentTimeMillis();
		
			ConcurrentHashMap<String , Integer> localmap=hm;
			lineCheck=null;hm=null;
			TreeMap<String , Integer>sMap=(new SortHelper()).sortMap(localmap);
			t2=System.currentTimeMillis()-t1;
	    		System.out.println("\nTime taken to sort(millisec):"+t2);
	    		System.out.println("\n########################################################################");
	    		System.out.println("Total Unique words"+sMap);
	    		sMap=null;
			System.out.println("end Of main");
			
		} catch (InterruptedException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		
	}
	//Count words
	public  static String tokenizeString(String line)
	{
		line.trim();
		Pattern p=Pattern.compile("\'?([a-zA-z0-9'-]+)\'?");
		int i=0;
		Matcher matcher = p.matcher(line);
		while(matcher.find())
		{	String w=matcher.group();
			Integer temp=hm.get(w);
			hm.put(w, (temp!=null?temp+1:1));
		}
		
		return "";
	}
	//update memoised HashMap
	public static void addLine(int a, String b)
	{
		lineCheck.put(a,b);
	}
	//User Input	
	public static String askUser(String q)
	{
		 System.out.println(q);
		 Scanner reader = new Scanner(System.in);
		 String resp=reader.nextLine();
		return resp.trim();
	}
}
