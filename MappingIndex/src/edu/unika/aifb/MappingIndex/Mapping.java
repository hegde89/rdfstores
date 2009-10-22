package edu.unika.aifb.MappingIndex;

public class Mapping {
	// Origin datasource
	public String ds1;
	// Target datasource
	public String ds2;
	// Mapping file
	public String file;
	
	Mapping(String ds_source, String ds_destination, String filename) {
		ds1 = ds_source;
		ds2 = ds_destination;
		file = filename;
	}
}
