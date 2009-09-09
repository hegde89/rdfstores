package edu.unika.aifb.graphindex.evaluation;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.httpclient.methods.GetMethod;
import org.apache.log4j.Logger;

import joptsimple.OptionParser;
import joptsimple.OptionSet;
import edu.unika.aifb.graphindex.index.IndexDirectory;
import edu.unika.aifb.graphindex.index.IndexReader;
import edu.unika.aifb.graphindex.storage.StorageException;

public class CombinedNewEval {
	private static final Logger log = Logger.getLogger(CombinedNewEval.class);
	
	private static long execute(Object o, String method) throws IOException, StorageException, IllegalArgumentException, SecurityException, IllegalAccessException, InvocationTargetException, NoSuchMethodException, InterruptedException {
		long start = System.currentTimeMillis();
		o.getClass().getMethod(method).invoke(o);
		return System.currentTimeMillis() - start;
	}
	
	public static void main(String[] args) throws IOException, InterruptedException, StorageException, SecurityException, NoSuchMethodException, IllegalArgumentException, IllegalAccessException, InvocationTargetException {
		OptionParser op = new OptionParser();
		op.accepts("o", "output directory")
			.withRequiredArg().ofType(String.class).describedAs("directory");
		op.accepts("qf", "query file")
			.withRequiredArg().ofType(String.class);
		op.accepts("q", "query name")
			.withRequiredArg().ofType(String.class);
		op.accepts("s", "system")
			.withRequiredArg().ofType(String.class);
		op.accepts("rf", "result file")
			.withRequiredArg().ofType(String.class);
		op.accepts("r", "repeats")
			.withRequiredArg().ofType(Integer.class);
		op.accepts("dc", "drop caches script")
			.withRequiredArg().ofType(String.class);
		op.accepts("qn", "qn").withRequiredArg().ofType(Integer.class);
		op.accepts("sk", "type").withRequiredArg().ofType(String.class);
		
		OptionSet os = op.parse(args);
		
		if (!os.has("o") || !os.has("s")) {
			op.printHelpOn(System.out);
			return;
		}

		String dir = (String)os.valueOf("o");
		String dropCachesScript = (String)os.valueOf("dc");
		int repeats = os.has("r") ? (Integer)os.valueOf("r") : 3;
		String type = os.has("type") ? (String)os.valueOf("type") : "graph";
		String sk = os.has("sk") ? (String)os.valueOf("sk") : "1";

		List<String> spcQueries = Arrays.asList(
			"query_q1_spc_" + sk, 
			"query_q2_spc_" + sk, 
			"query_q3_spc_" + sk,
			"query_q4_spc_" + sk, 
			"query_q5_spc_" + sk, 
			"query_q6_spc_" + sk, 
			"query_q7_spc_" + sk, 
			"query_q8_spc_" + sk, 
			"query_q9_spc_" + sk,
			"query_q10_spc_" + sk
		);

		List<String> vpQueries = Arrays.asList(
			"query_q1_vp", 
			"query_q2_vp", 
			"query_q3_vp",
			"query_q4_vp", 
			"query_q5_vp", 
			"query_q6_vp", 
			"query_q7_vp", 
			"query_q8_vp", 
			"query_q9_vp",
			"query_q10_vp"
		);
		

		List<List<Long>> vp = new ArrayList<List<Long>>();
		List<List<Long>> spc = new ArrayList<List<Long>>();
		for (int i = 0; i < spcQueries.size(); i++) {
			vp.add(new ArrayList<Long>());
			spc.add(new ArrayList<Long>());
		}

		for (int i = 0; i < repeats; i++) {
			for (int j = 0; j < spcQueries.size(); j++) {
				if (os.has("qn") && (Integer)os.valueOf("qn") != j)
					continue;
				
				System.gc();
				if (dropCachesScript != null && new File(dropCachesScript).exists()) {
					Process p = Runtime.getRuntime().exec(dropCachesScript);
					p.waitFor();
				}
	
				IndexReader reader = new IndexReader(new IndexDirectory(dir));
				
				Object o;
				if (type.equals("entity"))
					o = new EntityQuery(reader);
				else if (type.equals("path"))
					o = new PathQuery(reader);
				else if (type.equals("star"))
					o = new StarQuery(reader);
				else if (type.equals("graph"))
					o =  new GraphQuery(reader);
				else
					return;
				long time = execute(o, spcQueries.get(j));
				log.info(spcQueries.get(j) + ": " + time);
				spc.get(j).add(time);

				
				System.gc();
				if (dropCachesScript != null && new File(dropCachesScript).exists()) {
					Process p = Runtime.getRuntime().exec(dropCachesScript);
					p.waitFor();
				}
	
				reader = new IndexReader(new IndexDirectory(dir));
				
				if (type.equals("entity"))
					o = new EntityQuery(reader);
				else if (type.equals("path"))
					o = new PathQuery(reader);
				else if (type.equals("star"))
					o = new StarQuery(reader);
				else if (type.equals("graph"))
					o =  new GraphQuery(reader);
				else
					return;
				time = execute(o, vpQueries.get(j));
				log.info(vpQueries.get(j) + ": " + time);
				vp.get(j).add(time);
			}
		}

		log.debug(vp);
		log.debug(spc);
		
		List<Double> vp_avgs = new ArrayList<Double>();
		List<Double> spc_avgs = new ArrayList<Double>();
		for (int i = 0; i < vp.size(); i++) {
			double vp_avg = 0, spc_avg = 0;
			for (int j = 0; j < vp.get(i).size(); j++) {
				vp_avg += vp.get(i).get(j);
				spc_avg += spc.get(i).get(j);
			}
			vp_avgs.add(vp_avg / vp.get(i).size());
			spc_avgs.add(spc_avg / spc.get(i).size());
		}
		
		log.debug("vp: " + vp_avgs);
		log.debug("spc: " + spc_avgs);
	}
}
