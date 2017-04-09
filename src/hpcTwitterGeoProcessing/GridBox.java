package hpcTwitterGeoProcessing;

public class GridBox {
	String id;
	double xmin;
	double xmax;
	double ymin;
	double ymax;
	int count;	//the number of twitters in this GridBox
	
	public GridBox(String Id, double Xmin, double Xmax, double Ymin, double Ymax){
		id=Id;
		xmin=Xmin;
		xmax=Xmax;
		ymin=Ymin;
		ymax=Ymax;
		count=0;
	}
	
	public boolean Contain(Twitter t){	//return if twitter t is in this GridBox
		if(t.x>=xmin && t.x<=xmax && t.y>=ymin && t.y<=ymax)
			return true;
		return false;
	}
}
