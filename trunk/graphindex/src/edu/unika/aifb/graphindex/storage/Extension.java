package edu.unika.aifb.graphindex.storage;

import java.util.List;
import java.util.Set;

import edu.unika.aifb.graphindex.data.Subject;
import edu.unika.aifb.graphindex.data.Triple;

public interface Extension {
	public String getUri();
	
	public void flush() throws StorageException;
	public void remove() throws StorageException;
	
	public void addTriples(Set<Subject> subjects, String property, String object) throws StorageException;

	public List<Triple> getTriplesList(String propertyUri) throws StorageException;
	public List<Triple> getTriplesList(String propertyUri, String object) throws StorageException;

//	public void mergeExtension(Extension extension) throws StorageException;
}
