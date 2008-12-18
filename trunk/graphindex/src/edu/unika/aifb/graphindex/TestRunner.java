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

import org.apache.log4j.Logger;

import edu.unika.aifb.graphindex.query.IQueryEvaluator;
import edu.unika.aifb.graphindex.query.QueryEvaluator;
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
				int res = qe.evaluate(q);
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
	
	private static Result run2(IQueryEvaluator qe, List<Query> queries, int reps) throws StorageException, InterruptedException, ExecutionException, IOException {
		Result r = new Result();
		
		for (int i = 1; i <= 3; i++) {
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
					
					Process p = Runtime.getRuntime().exec("./drop_caches.sh");
					p.waitFor();
					log.info("system caches dropped");
	
					qe.clearCaches();
					log.info("internal caches cleared, indices reopened and warmed up");
	
					System.gc();
					log.info("gc run");
					
					long start = System.currentTimeMillis();
					int res = qe.evaluate(q);
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
		if (args.length < 2) {
			log.info("Usage:");
			log.info(" TestRunner [vp|gi] <vpdir> <gidir> <queriesfile> <repetitions> [query]");
		}
		
		String type = args[0];
		String vpDir = args[1];
		String giDir = args[2];
		String queriesFile = args[3];
		int reps = Integer.parseInt(args[4]);
		String resultsFile = args[5];
		String query = args.length == 7 ? args[6] : null;
		
		log.info("type: " + type);
		log.info("queries: " + queriesFile);
		log.info("repetitions: " + reps);
		
		QueryLoader ql = new QueryLoader();
		List<Query> queries = ql.loadQueryFile(queriesFile);
		log.info("loaded " + queries.size() + " queries");
		
		if (query != null) {
			for (Iterator<Query> i = queries.iterator(); i.hasNext(); )
				if (!i.next().getName().equals(query))
					i.remove();
		}
		List<Query> ordered = new ArrayList<Query>(queries);
		
		LuceneStorage ls = new LuceneStorage(vpDir);
		ls.initialize(false, true);
		IQueryEvaluator qeVP = new VPQueryEvaluator(ls);
		Result vp = run2(qeVP, queries, reps);
		ls.close();
		
		ls = null;
		qeVP = null;
		System.gc();

		StructureIndexReader index = new StructureIndexReader(giDir);
		index.setNumEvalThreads(2);
//		index.getIndex().setTableCacheSize(1);
		index.getIndex().setDocumentCacheSize(10000);
		IQueryEvaluator qeGI = index.getQueryEvaluator();
		
		Result gi = run2(qeGI, queries, reps);
		
		PrintWriter pw = new PrintWriter(new FileWriter(resultsFile));
		
		log.info("================================================================");
		log.info("vp:");
		pw.println("vp");
		vp.print(ordered, pw);
		
		log.info("----------------------------------------------------------------");
		log.info("gi:");
		pw.println("gi");
		gi.print(ordered, pw);
		
		pw.close();
		index.close();
	}
}
