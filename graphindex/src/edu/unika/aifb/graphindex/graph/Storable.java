package edu.unika.aifb.graphindex.graph;

@Deprecated
public interface Storable {
	public static int STATUS_NEW = 0;
	public static int STATUS_LOADED = 1;
	public static int STATUS_STUB = 2;
	public static int STATUS_REMOVED = 3;
	
	public void load();
	public int getStatus();
	public void remove();
	public void store();
	public void unload();
	
	public boolean isLoaded();
	public boolean isRemoved();
	public boolean isStubbed();
	public boolean isNew();
}
