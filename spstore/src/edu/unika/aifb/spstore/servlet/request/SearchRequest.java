package edu.unika.aifb.spstore.servlet.request;

import java.io.IOException;

import edu.unika.aifb.graphindex.data.Table;
import edu.unika.aifb.graphindex.index.IndexReader;
import edu.unika.aifb.graphindex.storage.StorageException;

import org.json.simple.JSONObject;

public abstract class SearchRequest {
	protected JSONObject m_requestObject;
	protected IndexReader m_reader;
	
	protected SearchRequest(IndexReader reader, JSONObject obj) {
		m_requestObject = obj;
		m_reader = reader;
	}

	public abstract String getResult() throws StorageException, IOException;
}
