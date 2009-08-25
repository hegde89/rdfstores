package src.edu.unika.aifb.spstore.test;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.StringBufferInputStream;
import java.io.StringReader;

import org.junit.Before;
import org.junit.Test;
import org.xml.sax.SAXException;

import com.meterware.httpunit.PostMethodWebRequest;
import com.meterware.httpunit.WebRequest;
import com.meterware.httpunit.WebResponse;
import com.meterware.servletunit.ServletRunner;
import com.meterware.servletunit.ServletUnitClient;

import edu.unika.aifb.spstore.servlet.Searcher;

public class SearcherServletTest {
	private ServletRunner sr;
	
	@Before
	public void setup() {
		sr = new ServletRunner();
		sr.registerServlet("spstore/Searcher", Searcher.class.getName());
	}
	
	@Test
	public void testStructuredQuery() throws IOException, SAXException {
		String json = "{\"structured-query\":{\"triple-patterns\":[[\"?x\",\"http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#name\",\"Course35\"]], \"select-variables\":[\"?x\"]}}";
		
		ServletUnitClient sc = sr.newClient();
		PostMethodWebRequest request = new PostMethodWebRequest("http://localhost:8080/spstore/Searcher", new StringBufferInputStream(json), "application/json");
		WebResponse response = sc.getResponse(request);
		
		BufferedReader in = new BufferedReader(new InputStreamReader(response.getInputStream()));
		String input;
		while ((input = in.readLine()) != null)
			System.out.println(input);
	}

	@Test
	public void testKeywordQuery() throws IOException, SAXException {
		String json = "{\"keyword-query\":\"Publication0 publicationAuthor GraduateStudent1@Department10.University0.edu\", \"translation\":{\"interpretations\":1,\"with-results\":1}}";
		
		ServletUnitClient sc = sr.newClient();
		PostMethodWebRequest request = new PostMethodWebRequest("http://localhost:8080/spstore/Searcher", new StringBufferInputStream(json), "application/json");
		WebResponse response = sc.getResponse(request);
		
		BufferedReader in = new BufferedReader(new InputStreamReader(response.getInputStream()));
		String input;
		while ((input = in.readLine()) != null)
			System.out.println(input);
	}

	@Test
	public void testHybridQuery() throws IOException, SAXException {
		String json = "{"+
			"\"keyword-query\":\"GraduateStudent36 teachingAssistantOf\", \"translation\":{\"interpretations\":1,\"with-results\":1}, " +
			"\"structured-query\":{\"triple-patterns\":[" +
				"[\"?y\", \"http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#name\",\"Course35\"]," +
				"[\"?x\", \"http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#teacherOf\",\"?y\"]," +
				"[\"?x\", \"http://www.w3.org/1999/02/22-rdf-syntax-ns#type\",\"http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#AssistantProfessor\"]" +
			"], \"select-variables\":[\"?x\", \"?y\"]} " +
		"}";
		System.out.println(json);
		ServletUnitClient sc = sr.newClient();
		PostMethodWebRequest request = new PostMethodWebRequest("http://localhost:8080/spstore/Searcher", new StringBufferInputStream(json), "application/json");
		WebResponse response = sc.getResponse(request);
		
		BufferedReader in = new BufferedReader(new InputStreamReader(response.getInputStream()));
		String input;
		while ((input = in.readLine()) != null)
			System.out.println(input);
	}
}
