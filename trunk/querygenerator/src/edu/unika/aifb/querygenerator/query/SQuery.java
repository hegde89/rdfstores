package edu.unika.aifb.querygenerator.query;

import java.util.Set;

public interface SQuery {
	
	public String getQuery();
	
	public String getVariables(int max);
	
	public void addVariables(String var);
	
	public void generateVariables();
	
	public int getNumAtom();
	
	public int getNumVar();
	
	public int getNumDvar(); 
}
