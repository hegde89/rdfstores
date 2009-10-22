package edu.unika.aifb.MappingIndex;

public class Mapping {
	public String ds1;
	public String ds2;
	public String file;
	
	Mapping(String ds_source, String ds_destination, String filename) {
		ds1 = ds_source;
		ds2 = ds_destination;
		file = filename;
	}
}
