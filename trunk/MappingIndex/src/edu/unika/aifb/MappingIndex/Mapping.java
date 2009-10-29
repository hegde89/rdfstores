package edu.unika.aifb.MappingIndex;

public class Mapping {
	// Source Datasource
	public String ds1;
	// Destination Datasource
	public String ds2;
	// Mapping file
	public String file;
	
	/**
	 * Represents a Mapping and consists out of two data source information (the source and the destination)
	 * and the filename of the mapping file.
	 * @param ds_source
	 * @param ds_destination
	 * @param filename
	 */
	Mapping(String ds_source, String ds_destination, String filename) {
		ds1 = ds_source;
		ds2 = ds_destination;
		file = filename;
	}
}
