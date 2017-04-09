package hpcTwitterGeoProcessing;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.FileNotFoundException;
import java.io.InputStreamReader;

import mpi.*;

import com.google.gson.*;

public class Main {
	private static String mgFilePath="./melbGrid.json";	//The path of file "melbGrid.json"
	private static String twitterFilePath="./bigTwitter.json";	//The path of file "bigTwitter.json"
	
	public static void main(String[] args) throws Exception{
		
		MPI.Init(args);
		int me = MPI.COMM_WORLD.Rank();
		int size = MPI.COMM_WORLD.Size();
		int tag=99;
		int master=0;
		
		MPI.COMM_WORLD.Barrier();
		if ( me == master ){	//If current node is master node
			long startTime=System.currentTimeMillis(); 
			
			MelbGrid master_mg=new MelbGrid();	
			LoadMelbGrid(master_mg);	//Read in melbGrid.json into master_mg
			
			int numberOfTwitters=NumberOfTwitters();	//Number of twitters in bigTwitter.json
			if(size>1){	//Case more than 1 task, parallelized
				int workerNodes=size-1;	//Number of worker nodes
				int twittersForWorker=(numberOfTwitters+workerNodes)/(workerNodes);	//Number of twitters each worker needs to handle 
				for(int i=1;i<size;i++){
					int[] sendBuf={(i-1)*twittersForWorker,twittersForWorker};	//sendBuf is an int array of size 2, specifying the location of the file to start and the number of twitters to read. 
					MPI.COMM_WORLD.Send(sendBuf, 0, 2, MPI.INT, i, tag);	//Allocate sendBufs(tasks) to worker nodes
				}
				
				for(int i=1;i<size;i++){	//Get the count results from all the workers and aggregate the results 
					int[] recvBuf=new int[master_mg.size()];
					MPI.COMM_WORLD.Recv(recvBuf, 0, master_mg.size(), MPI.INT, i, tag);	//Get the count results from ith worker.
					for(int j=0;j<master_mg.size();j++){
						master_mg.get(j).count+=recvBuf[j];	//Accumulate the counts from worker i with respect to the Grids.
					}
				}
			}
			else if(size==1){	//Case 1 task, serialized 
				MatchTwitters(master_mg,0,numberOfTwitters);	//Find the corresponding Grids for twitters starts from 0 with size of numberOfTwitters, and store the results in master_mg
			}
			ShowRankInEachBox(master_mg);
			System.out.println();
			ShowRankInEahcRow(master_mg);
			System.out.println();
			ShowRankInEahcColumn(master_mg);
			long endTime=System.currentTimeMillis(); 
			System.out.println("Running time:"+(endTime-startTime)/1000+"s");
		}
		else{
			MelbGrid worker_mg=new MelbGrid();	
			LoadMelbGrid(worker_mg);	//Read in melbGrid.json into worker_mg
			int[] recvBuf=new int[2];	//recvBuf[0] indicates the starting place of bigTwitter.json to read, and recvBuf[1] is the number of twitters to read
			MPI.COMM_WORLD.Recv(recvBuf, 0, 2, MPI.INT, master, tag);	
			
			MatchTwitters(worker_mg,recvBuf[0],recvBuf[1]);	//Find the corresponding Grids for twitters starts from recvBuf[0] with size of recvBuf[1], and store the results in worker_mg
			int[] sendBuf=new int[worker_mg.size()];
			for(int i=0;i<worker_mg.size();i++){
				sendBuf[i]=worker_mg.get(i).count;	//Copy the counts of each Grids to sendBuf 
			}
			MPI.COMM_WORLD.Send(sendBuf, 0, worker_mg.size(), MPI.INT, master, tag);	//Send sendBuf back to master_node
		}
		
		MPI.Finalize();
	}
	
	public static void LoadMelbGrid(MelbGrid mg)throws Exception{	//Read in melbGrid.json and initialise mg 
		JsonParser parse =new JsonParser();
		try {
			File f=new File(mgFilePath);
            JsonObject json=(JsonObject) parse.parse(new FileReader(f)); 
            JsonArray features=json.get("features").getAsJsonArray();
            
            for(int i=0;i<features.size();i++){
            	JsonObject feature=features.get(i).getAsJsonObject();
            	JsonObject properties=feature.get("properties").getAsJsonObject();
            	String id=properties.get("id").getAsString();
            	double xmin=properties.get("xmin").getAsDouble();
            	double xmax=properties.get("xmax").getAsDouble();
            	double ymin=properties.get("ymin").getAsDouble();
            	double ymax=properties.get("ymax").getAsDouble();
            	
            	GridBox gb=new GridBox(id,xmin,xmax,ymin,ymax);
            	mg.add(gb);
            }
        } 
		catch (JsonIOException e) {
            e.printStackTrace();
        } 
		catch (JsonSyntaxException e) {
            e.printStackTrace();
        } 
		catch (FileNotFoundException e) {
            e.printStackTrace();
        }
	}
	
	public static int NumberOfTwitters() throws Exception{	//Get the number of twitters in twitterFilePath
		int number=0;
		try {
			File tFile=new File(twitterFilePath);
			InputStreamReader reader=new InputStreamReader(new FileInputStream(tFile));
			BufferedReader br=new BufferedReader(reader);
			String twitterStr=null;
			while( (twitterStr=br.readLine())!=null ){
				if(twitterStr.startsWith("[") || twitterStr.endsWith("]"))
					continue;
				else{
					number++;
				}
			}
			br.close();
		}
		catch (JsonIOException e) {
            e.printStackTrace();
        } 
		catch (JsonSyntaxException e) {
            e.printStackTrace();
        } 
		catch (FileNotFoundException e) {
            e.printStackTrace();
        }
		return number;
	}
	
	public static void MatchTwitters(MelbGrid mg,int start,int length) throws Exception{//Find the corresponding Grids for twitters starts from start with size of length, and store the results in worker_mg
		JsonParser parse =new JsonParser();
		try {
			File tFile=new File(twitterFilePath);
			InputStreamReader reader=new InputStreamReader(new FileInputStream(tFile));
			BufferedReader br=new BufferedReader(reader);
			
			//skip the twitters until it gets to the position start
			for(int i=0;i<start+1;i++){
				br.readLine();
			}
			
			String twitterStr=null;
			for( int i=0;i<length;i++){// read twitters from tFile with Gson
				twitterStr=br.readLine();
				if(twitterStr.endsWith("]"))
					break;
				else if(twitterStr.endsWith(",")){
					twitterStr=twitterStr.substring(0, twitterStr.length()-1);
				}
				
				JsonObject twitter=(JsonObject)parse.parse(twitterStr);
				JsonObject meta=twitter.get("meta").getAsJsonObject();
	            String id=meta.get("id").getAsString();
	            	
	            JsonObject json=twitter.get("json").getAsJsonObject();
	            JsonObject coordinates=json.get("coordinates").getAsJsonObject();
	            JsonArray point=coordinates.get("coordinates").getAsJsonArray();
	            double x=point.get(0).getAsDouble();
	            double y=point.get(1).getAsDouble();
	            	
	            Twitter t=new Twitter(id,x,y);
	            mg.findMatchedGrid(t);	//find matched grid for twitter t
			}
			br.close();
		}
		catch (JsonIOException e) {
            e.printStackTrace();
        } 
		catch (JsonSyntaxException e) {
            e.printStackTrace();
        } 
		catch (FileNotFoundException e) {
            e.printStackTrace();
        }
	}
	
	public static void ShowRankInEachBox(MelbGrid mg){
		mg.Rank();	//rank the Grids in mg according to their counts
		for(int i=0;i<mg.size();i++){
			System.out.println(mg.get(i).id+":"+mg.get(i).count+" twitts");
		}
	}
	
	public static void ShowRankInEahcRow(MelbGrid mg){//rank the rows in mg
		int[] countInEachRow=new int[4];
		for(int i=0;i<mg.size();i++){
			if(mg.get(i).id.contains("A")){
				countInEachRow[0]+=mg.get(i).count;
			}
			else if(mg.get(i).id.contains("B")){
				countInEachRow[1]+=mg.get(i).count;
			}
			else if(mg.get(i).id.contains("C")){
				countInEachRow[2]+=mg.get(i).count;
			}
			else if(mg.get(i).id.contains("D")){
				countInEachRow[3]+=mg.get(i).count;
			}
		}
		
		for(int i=0;i<countInEachRow.length;i++){
			int max_count=-1;
			int max_index=-1;
			for(int j=0;j<countInEachRow.length;j++){
				if(max_count<countInEachRow[j]){
					max_count=countInEachRow[j];
					max_index=j;
				}
			}
			
			if(max_index==0)
				System.out.print("A");
			else if(max_index==1){
				System.out.print("B");
			}
			else if(max_index==2){
				System.out.print("C");
			}
			else if(max_index==3){
				System.out.print("D");
			}
			System.out.println("-Row:"+countInEachRow[max_index]+" twitts");
			
			countInEachRow[max_index]=-1;
		}
	}
	
	public static void ShowRankInEahcColumn(MelbGrid mg){//rank the columns in mg
		int[] countInEachColumn=new int[5];
		for(int i=0;i<mg.size();i++){
			if(mg.get(i).id.contains("1")){
				countInEachColumn[0]+=mg.get(i).count;
			}
			else if(mg.get(i).id.contains("2")){
				countInEachColumn[1]+=mg.get(i).count;
			}
			else if(mg.get(i).id.contains("3")){
				countInEachColumn[2]+=mg.get(i).count;
			}
			else if(mg.get(i).id.contains("4")){
				countInEachColumn[3]+=mg.get(i).count;
			}
			else if(mg.get(i).id.contains("5")){
				countInEachColumn[4]+=mg.get(i).count;
			}
		}
		
		for(int i=0;i<countInEachColumn.length;i++){
			int max_count=-1;
			int max_index=-1;
			for(int j=0;j<countInEachColumn.length;j++){
				if(max_count<countInEachColumn[j]){
					max_count=countInEachColumn[j];
					max_index=j;
				}
			}
			
			System.out.println("Column "+(max_index+1)+":"+countInEachColumn[max_index]+" twitts");
			
			countInEachColumn[max_index]=-1;
		}
	}
}
