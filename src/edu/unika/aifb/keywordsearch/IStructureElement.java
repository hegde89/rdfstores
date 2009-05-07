package edu.unika.aifb.keywordsearch;

import java.io.Serializable;

public interface IStructureElement extends Serializable {
	
	public int getType();
	
	public double getEF();
	
	public double getMatchingScore();
	
	public double getTotalCost();
}
