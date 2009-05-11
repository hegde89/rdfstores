package edu.unika.aifb.graphindex;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import javax.swing.event.ListSelectionEvent;

import joptsimple.OptionParser;
import joptsimple.OptionSet;

import org.apache.log4j.Logger;

import edu.unika.aifb.graphindex.query.IQueryEvaluator;
import edu.unika.aifb.graphindex.query.QueryEvaluator;
import edu.unika.aifb.graphindex.query.model.Literal;
import edu.unika.aifb.graphindex.query.model.Query;
import edu.unika.aifb.graphindex.storage.StorageException;
import edu.unika.aifb.graphindex.util.QueryLoader;
import edu.unika.aifb.graphindex.util.Timings;
import edu.unika.aifb.graphindex.vp.LuceneStorage;
import edu.unika.aifb.graphindex.vp.VPQueryEvaluator;

public class TestRunner {
	private static final Logger log = Logger.getLogger(TestRunner.class);
	
	private static class Result {
		public Map<String,Integer> q2rs = new HashMap<String,Integer>();
		public Map<String,List<Long>> runs = new HashMap<String,List<Long>>();
		public Map<String,List<Long>> q2match = new HashMap<String,List<Long>>();
		public Map<String,List<Long>> q2data = new HashMap<String,List<Long>>();
		public Map<String,List<Long>> q2join = new HashMap<String,List<Long>>();
		
		public void print(List<Query> ordered, PrintWriter out) {
			String runResults = "";
			for (Query q : ordered) {
				String s = "";
				runResults += "query: " + q.getName() + "\n";
				List<Long> tt = runs.get(q.getName());
				List<Long> mt = q2match.get(q.getName());
				List<Long> dt = q2data.get(q.getName());
				List<Long> jt = q2join.get(q.getName());
				runResults += tt.toString() + "\n";
				runResults += mt.toString() + "\n";
				runResults += dt.toString() + "\n";
				runResults += jt.toString() + "\n";
				log.debug(tt);
				log.debug(mt);
				log.debug(dt);
				log.debug(jt);
				
//				if (tt.size() >= 3) {
//					int lowRun = 0, highRun = 0;
//					for (int i = 0; i < tt.size(); i++) {
//						if (tt.get(i) > tt.get(highRun))
//							highRun = i;
//						if (tt.get(i) < tt.get(lowRun))
//							lowRun = i;
//					}
//					
//					tt.remove(Math.max(lowRun, highRun));
//					tt.remove(Math.min(lowRun, highRun));
//					mt.remove(Math.max(lowRun, highRun));
//					mt.remove(Math.min(lowRun, highRun));
//					dt.remove(Math.max(lowRun, highRun));
//					dt.remove(Math.min(lowRun, highRun));
//					jt.remove(Math.max(lowRun, highRun));
//					jt.remove(Math.min(lowRun, highRun));
////					tt.remove(0);
////					tt.remove(0);
////					mt.remove(0);
////					mt.remove(0);
////					dt.remove(0);
////					dt.remove(0);
////					jt.remove(0);
////					jt.remove(0);
//				}
				
				double total_avg = 0, match_avg = 0, data_avg = 0, join_avg = 0;
				for (int i = 0; i < tt.size(); i++) {
					total_avg += tt.get(i);
					match_avg += mt.get(i);
					data_avg += dt.get(i);
					join_avg += jt.get(i);
				}
				
				total_avg /= tt.size();
				match_avg /= tt.size();
				data_avg /= tt.size();
				join_avg /= tt.size();
				
				log.info(q.getName() + " " + q2rs.get(q.getName()) + "\t" + total_avg + " (" + match_avg + ", " + data_avg + ", " + join_avg + ")");

				out.print(q.getName() + "_totalt\t");
				for (long i : runs.get(q.getName()))
					out.print(i + "\t");
				out.println();
				
				out.print(q.getName() + "_matcht\t");
				for (long i : q2match.get(q.getName()))
					out.print(i + "\t");
				out.println();
				
				out.print(q.getName() + "_datat\t");
				for (long i : q2data.get(q.getName()))
					out.print(i + "\t");
				out.println();
				
				out.print(q.getName() + "_joint\t");
				for (long i : q2join.get(q.getName()))
					out.print(i + "\t");
				out.println();
				out.println();
			}
//			out.println(runResults);
		}
	}
	
	private static Result run(IQueryEvaluator qe, List<Query> queries, int reps) throws StorageException, InterruptedException, ExecutionException, IOException {
		Result r = new Result();
		
		for (int i = 1; i <= reps; i++) {
			log.info("run #" + i);
			log.info("--------------------------------------------------");
			
			Collections.shuffle(queries);
			
			String s = "";
			for (Query q : queries) {
				s += q.getName() + " ";
				if (!r.runs.containsKey(q.getName()))
					r.runs.put(q.getName(), new ArrayList<Long>());
				if (!r.q2match.containsKey(q.getName()))
					r.q2match.put(q.getName(), new ArrayList<Long>());
				if (!r.q2data.containsKey(q.getName()))
					r.q2data.put(q.getName(), new ArrayList<Long>());
				if (!r.q2join.containsKey(q.getName()))
					r.q2join.put(q.getName(), new ArrayList<Long>());
			}
			log.info("query order: " + s);
			
			for (Query q : queries) {
				log.info("--------------------------------------------------");
				log.info("query: " + q.getName());
				
				Process p = Runtime.getRuntime().exec("./drop_caches.sh");
				p.waitFor();
				log.info("system caches dropped");

				qe.clearCaches();
				log.info("internal caches cleared, indices reopened and warmed up");

				System.gc();
				log.info("gc run");
				
//				BufferedReader in = new BufferedReader(new InputStreamReader(p.getErrorStream()));
//				String input;
//				while ((input = in.readLine()) != null)
//					log.debug(input);
				
				long start = System.currentTimeMillis();
				int res = qe.evaluate(q).size();
				long duration = System.currentTimeMillis() - start;
				
				r.runs.get(q.getName()).add(duration);
				if (!r.q2rs.containsKey(q.getName()))
					r.q2rs.put(q.getName(), res);
				
				long[] t = qe.getTimings();
				
				r.q2match.get(q.getName()).add(t[Timings.MATCH]);
				r.q2data.get(q.getName()).add(t[Timings.DATA]);
				r.q2join.get(q.getName()).add(t[Timings.JOIN]);
			}
		}
		
		return r;
	}
	
	private static Result run2(IQueryEvaluator qe, List<Query> queries, int reps, int runs) throws StorageException, InterruptedException, ExecutionException, IOException {
		Result r = new Result();
		
		for (int i = 1; i <= runs; i++) {
			log.info("run #" + i);
			log.info("--------------------------------------------------");
			
			Collections.shuffle(queries);
			
			String s = "";
			for (Query q : queries) {
				s += q.getName() + " ";
				if (!r.runs.containsKey(q.getName()))
					r.runs.put(q.getName(), new ArrayList<Long>());
				if (!r.q2match.containsKey(q.getName()))
					r.q2match.put(q.getName(), new ArrayList<Long>());
				if (!r.q2data.containsKey(q.getName()))
					r.q2data.put(q.getName(), new ArrayList<Long>());
				if (!r.q2join.containsKey(q.getName()))
					r.q2join.put(q.getName(), new ArrayList<Long>());
			}
			log.info("query order: " + s);

			for (Query q : queries) {
				log.info("--------------------------------------------------");
				log.info("query: " + q.getName());
				for (int j = 0; j < reps; j++) {
					log.info("-----------------------------");
					log.info("query run #" + j);
					
					Process p = Runtime.getRuntime().exec("/local/users/btc/gula/drop_caches.sh");
					p.waitFor();
					log.info("system caches dropped");
	
					qe.clearCaches();
					log.info("internal caches cleared, indices reopened and warmed up");
	
					System.gc();
					log.info("gc run");
					
					long start = System.currentTimeMillis();
					int res = qe.evaluate(q).size();
					long duration = System.currentTimeMillis() - start;
					
					r.runs.get(q.getName()).add(duration);
					if (!r.q2rs.containsKey(q.getName()))
						r.q2rs.put(q.getName(), res);
					
					long[] t = qe.getTimings();
					
					r.q2match.get(q.getName()).add(t[Timings.MATCH]);
					r.q2data.get(q.getName()).add(t[Timings.DATA]);
					r.q2join.get(q.getName()).add(t[Timings.JOIN]);
				}
			}
		}
		
		return r;
	}

	public static void main(String[] args) throws IOException, StorageException, InterruptedException, ExecutionException {
		OptionParser op = new OptionParser();
		op.accepts("vp").withRequiredArg().ofType(String.class);
		op.accepts("sp").withRequiredArg().ofType(String.class);
		op.accepts("qf").withRequiredArg().ofType(String.class);
		op.accepts("rf").withRequiredArg().ofType(String.class);
		op.accepts("r").withRequiredArg().ofType(Integer.class);
		op.accepts("runs").withRequiredArg().ofType(Integer.class);
		op.accepts("q").withRequiredArg().ofType(String.class);
		op.accepts("nodstes");
		op.accepts("nosrces");
		
		OptionSet os = op.parse(args);
		
		if ((!os.has("vp") && !os.has("sp")) || !os.has("qf") || !os.has("rf") || !os.has("r")) {
			op.printHelpOn(System.out);
			return;
		}
		
		boolean runVP = os.has("vp");
		boolean runSP = os.has("sp");
		
		boolean dstUnmappedES = !os.has("nodstes");
		boolean srcUnmappedES = !os.has("nosrces");
		
		String vpDir = (String)os.valueOf("vp");
		String spDir = (String)os.valueOf("sp");
		String queriesFile = (String)os.valueOf("qf");
		String resultsFile = (String)os.valueOf("rf");
		int reps = (Integer)os.valueOf("r");
		int runs = os.valueOf("runs") != null ? (Integer)os.valueOf("runs") : 3;
		String query = (String)os.valueOf("q");

		log.info("vp: " + runVP + " " + vpDir);
		log.info("sp: " + runSP + " " + spDir);
		log.info("queries: " + queriesFile);
		log.info("res file: " + resultsFile);
		log.info("repetitions: " + reps);
		log.info("dst unmapped ext setup: " + dstUnmappedES);
		log.info("src unmapped ext setup: " + srcUnmappedES);
		
		QueryLoader ql;
		
		List<Query> queries = null;
		
		Result vp, gi; 
		if (runVP) {
			LuceneStorage ls = new LuceneStorage(vpDir);
			ls.initialize(false, true);
			
			queries = loadQueries(queriesFile, query, new QueryLoader());
			if (queries.size() == 0) {
				log.info("no queries");
				ls.close();
				return;
			}
			
			IQueryEvaluator qeVP = new VPQueryEvaluator(ls);
			
			if (runVP)
				vp = run2(qeVP, queries, reps, runs);
			else 
				vp = null;
			ls.close();
			
			ls = null;
			qeVP = null;
			System.gc();
		}
		else
			vp = null;

		if (runSP) {
			StructureIndexReader index = new StructureIndexReader(spDir);
			index.setNumEvalThreads(2);
	//		index.getIndex().setTableCacheSize(1);
			index.getIndex().setDocumentCacheSize(10000);
			
			queries = loadQueries(queriesFile, query, index.getQueryLoader());
			if (queries.size() == 0) {
				log.info("no queries");
				index.close();
				return;
			}

			IQueryEvaluator qeGI = index.getQueryEvaluator();
//			((QueryEvaluator)qeGI).getMLV().setDstExtSetup(dstUnmappedES, srcUnmappedES);
			
			if (runSP)
				gi = run2(qeGI, queries, reps, runs);
			else
				gi = null;

			index.close();
		}
		else 
			gi = null;
		
		List<Query> ordered = new ArrayList<Query>(queries);

		PrintWriter pw = new PrintWriter(new FileWriter(resultsFile));
		
		if (vp != null) {
			log.info("================================================================");
			log.info("vp:");
			pw.println("vp");
			vp.print(ordered, pw);
		}
		
		if (gi != null) {
			log.info("----------------------------------------------------------------");
			log.info("gi:");
			pw.println("gi");
			gi.print(ordered, pw);
		}
		
		pw.close();
	}

	private static List<Query> loadQueries(String queriesFile, String query, QueryLoader ql) throws IOException {
		List<Query> queries = ql.loadQueryFile(queriesFile);
		log.info("loaded " + queries.size() + " queries");
		
		for (Iterator<Query> i = queries.iterator(); i.hasNext(); ) {
			boolean hasConstant = false;
			Query q = i.next();
			for (Literal l : q.getLiterals())
				if (!l.getObject().toString().startsWith("?") || !l.getSubject().toString().startsWith("?"))
					hasConstant = true;
			if (!hasConstant) {
				i.remove();
				log.debug("no constants: " + q.getName());
			}
		}
		
		if (query != null) {
			for (Iterator<Query> i = queries.iterator(); i.hasNext(); )
				if (!i.next().getName().equals(query))
					i.remove();
		}
		
		return queries;
	}
}
