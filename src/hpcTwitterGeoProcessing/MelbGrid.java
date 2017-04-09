package hpcTwitterGeoProcessing;
import java.util.ArrayList;

public class MelbGrid {
	private ArrayList<GridBox> melbGrids= new ArrayList<GridBox>();	//An arrayList of GridBox that contains all the GridBoxes in Melb
	
	public void add(GridBox gb){
		melbGrids.add(gb);
	}
	
	public GridBox get(int index){
		return melbGrids.get(index);
	}
	
	public int size(){
		return melbGrids.size();
	}
	
	public String findMatchedGrid(Twitter t){	//find the Grid t is in.
		for(int i=0;i<melbGrids.size();i++){
			if(melbGrids.get(i).Contain(t)){
				melbGrids.get(i).count++;
				return melbGrids.get(i).id;
			}
		}
		return "Wrong Twitter Coordinates!";
	}
	
	public void Rank(){	//Rank the GridBox based on their counts
		ArrayList<GridBox> rankedMelbGrids= new ArrayList<GridBox>();
		while(!melbGrids.isEmpty()){
			int i;
			int max_count=melbGrids.get(0).count;
			int max_index=0;
			for(i=1;i<melbGrids.size();i++){
				if(melbGrids.get(i).count>max_count){
					max_count=melbGrids.get(i).count;
					max_index=i;
				}
			}
			rankedMelbGrids.add(melbGrids.get(max_index));
			melbGrids.remove(max_index);
		}
		melbGrids=rankedMelbGrids;
	}
}
