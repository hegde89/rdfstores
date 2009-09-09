package edu.unika.aifb.graphindex.evaluation;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import edu.unika.aifb.graphindex.data.Table;
import edu.unika.aifb.graphindex.index.DataIndex;
import edu.unika.aifb.graphindex.index.IndexReader;
import edu.unika.aifb.graphindex.index.StructureIndex;
import edu.unika.aifb.graphindex.query.PrunedQueryPart;
import edu.unika.aifb.graphindex.searcher.plan.QueryOperators;
import edu.unika.aifb.graphindex.storage.DataField;
import edu.unika.aifb.graphindex.storage.IndexDescription;
import edu.unika.aifb.graphindex.storage.IndexStorage;
import edu.unika.aifb.graphindex.storage.StorageException;

public class PathQuery {
	private IndexReader m_idxReader;
	private DataIndex m_dataIndex;
	private StructureIndex m_structureIndex;
	private Map<IndexDescription,IndexStorage> m_indexes;
	private QueryOperators m_op;

	public PathQuery(IndexReader reader) throws IOException, StorageException {
		m_idxReader = reader;
		m_dataIndex = m_idxReader.getDataIndex();
		m_structureIndex = m_idxReader.getStructureIndex();
		
		m_indexes = new HashMap<IndexDescription,IndexStorage>();
		m_indexes.put(IndexDescription.CPSO, m_dataIndex.getIndexStorage(IndexDescription.CPSO));
		m_indexes.put(IndexDescription.SCOP, m_dataIndex.getIndexStorage(IndexDescription.SCOP));
		m_indexes.put(IndexDescription.OCPS, m_dataIndex.getIndexStorage(IndexDescription.OCPS));
		m_indexes.put(IndexDescription.PSOC, m_dataIndex.getIndexStorage(IndexDescription.PSOC));
		m_indexes.put(IndexDescription.POCS, m_dataIndex.getIndexStorage(IndexDescription.POCS));
		m_indexes.put(IndexDescription.SOPC, m_dataIndex.getIndexStorage(IndexDescription.SOPC));

		m_indexes.put(IndexDescription.SES, m_structureIndex.getSPIndexStorage());
		
		m_op = new QueryOperators(m_idxReader, m_indexes);
	}
	
	private String[] names(String... names) {
		return names;
	}
	
	private DataField[] SO = new DataField[] { DataField.SUBJECT, DataField.OBJECT };
	public void query_q1_spc_2() throws StorageException {
		Table<String> t1 = m_op.load(IndexDescription.POCS, SO, names("?x1", "UndergraduateStudent75@Department8.University0.edu"), DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#emailAddress", DataField.OBJECT, "UndergraduateStudent75@Department8.University0.edu");

		Table<String> t2 = m_op.load(IndexDescription.POCS, SO, names("?x2", "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#Course"), DataField.PROPERTY, "http://www.w3.org/1999/02/22-rdf-syntax-ns#type", DataField.OBJECT, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#Course");

		Table<String> t3_a = m_op.load(IndexDescription.POCS, SO, names("?x2", "Course5"), DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#name", DataField.OBJECT, "Course5");
		Table<String> t3 = m_op.mergeJoin(t3_a, t2, "?x2");
		t3 = m_op.compact(t3, Arrays.asList("?x2"));

		Table<String> t4_a = m_op.indexJoin(t1, "?x1", IndexDescription.PSOC, SO, names("?x1", "?x2"), DataField.SUBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#takesCourse");
		Table<String> t4 = m_op.mergeJoin(t4_a, t3, "?x2");

		}
		public void query_q2_spc_2() throws StorageException {
		PrunedQueryPart p1 = new PrunedQueryPart("p", null);
		p1.addEdge("?x1", "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#undergraduateDegreeFrom", "?x2");
		Table<String> t1 = m_op.load(IndexDescription.POCS, SO, names("?x1", "GraduateStudent144@Department11.University0.edu"), DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#emailAddress", DataField.OBJECT, "GraduateStudent144@Department11.University0.edu");
		t1 = m_op.refineWithPrunedPart(p1, "?x1", t1);

		Table<String> t2 = m_op.load(IndexDescription.POCS, SO, names("?x3", "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#AssociateProfessor"), DataField.PROPERTY, "http://www.w3.org/1999/02/22-rdf-syntax-ns#type", DataField.OBJECT, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#AssociateProfessor");

		Table<String> t3_a = m_op.indexJoin(t1, "?x1", IndexDescription.PSOC, SO, names("?x1", "?x3"), DataField.SUBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#advisor");
		Table<String> t3 = m_op.mergeJoin(t3_a, t2, "?x3");

		}
		public void query_q3_spc_2() throws StorageException {
		PrunedQueryPart p1 = new PrunedQueryPart("p", null);
		p1.addEdge("?x1", "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#worksFor", "?x2");
		Table<String> t1 = m_op.load(IndexDescription.POCS, SO, names("?x1", "AssistantProfessor5"), DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#name", DataField.OBJECT, "AssistantProfessor5");
		t1 = m_op.refineWithPrunedPart(p1, "?x1", t1);

		Table<String> t2 = m_op.load(IndexDescription.POCS, SO, names("?x3", "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#University"), DataField.PROPERTY, "http://www.w3.org/1999/02/22-rdf-syntax-ns#type", DataField.OBJECT, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#University");

		Table<String> t3_a = m_op.indexJoin(t1, "?x1", IndexDescription.PSOC, SO, names("?x1", "?x3"), DataField.SUBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#doctoralDegreeFrom");
		Table<String> t3 = m_op.mergeJoin(t3_a, t2, "?x3");

		}
		public void query_q4_spc_2() throws StorageException {
		PrunedQueryPart p1 = new PrunedQueryPart("p", null);
		p1.addEdge("?x2", "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#subOrganizationOf", "?x6");
		Table<String> t1 = m_op.load(IndexDescription.POCS, SO, names("?x1", "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#UndergraduateStudent"), DataField.PROPERTY, "http://www.w3.org/1999/02/22-rdf-syntax-ns#type", DataField.OBJECT, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#UndergraduateStudent");

		Table<String> t2 = m_op.load(IndexDescription.POCS, SO, names("?x4", "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#Course"), DataField.PROPERTY, "http://www.w3.org/1999/02/22-rdf-syntax-ns#type", DataField.OBJECT, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#Course");

		Table<String> t3_a = m_op.load(IndexDescription.POCS, SO, names("?x1", "UndergraduateStudent200@Department5.University0.edu"), DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#emailAddress", DataField.OBJECT, "UndergraduateStudent200@Department5.University0.edu");
		Table<String> t3 = m_op.mergeJoin(t3_a, t1, "?x1");
		t3 = m_op.compact(t3, Arrays.asList("?x1"));

		Table<String> t4 = m_op.load(IndexDescription.POCS, SO, names("?x2", "Department5"), DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#name", DataField.OBJECT, "Department5");
		t4 = m_op.refineWithPrunedPart(p1, "?x2", t4);

		Table<String> t5_a = m_op.load(IndexDescription.POCS, SO, names("?x4", "Course30"), DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#name", DataField.OBJECT, "Course30");
		Table<String> t5 = m_op.mergeJoin(t5_a, t2, "?x4");
		t5 = m_op.compact(t5, Arrays.asList("?x4"));

		Table<String> t6_a = m_op.indexJoin(t3, "?x1", IndexDescription.PSOC, SO, names("?x1", "?x2"), DataField.SUBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#memberOf");
		Table<String> t6 = m_op.mergeJoin(t6_a, t4, "?x2");

		Table<String> t7 = m_op.indexJoin(t5, "?x4", IndexDescription.POCS, SO, names("?x5", "?x4"), DataField.OBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#takesCourse");

		Table<String> t8 = m_op.indexJoin(t6, "?x2", IndexDescription.POCS, SO, names("?x3", "?x2"), DataField.OBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#memberOf");

		Table<String> t9_a = m_op.indexJoin(t7, "?x5", IndexDescription.PSOC, SO, names("?x5", "?x2"), DataField.SUBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#memberOf");
		Table<String> t9 = m_op.mergeJoin(t9_a, t8, "?x2");
		t9 = m_op.compact(t9, Arrays.asList("?x3", "?x4", "?x1", "?x2"));

		Table<String> t10 = m_op.indexJoin(t9, "?x3", "?x4", IndexDescription.PSOC, SO, names("?x3", "?x4"), DataField.SUBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#teachingAssistantOf");
		t10 = m_op.compact(t10, Arrays.asList("?x4", "?x1", "?x2"));

		}
		public void query_q5_spc_2() throws StorageException {
		PrunedQueryPart p1 = new PrunedQueryPart("p", null);
		p1.addEdge("?x7", "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#publicationAuthor", "?x6");
		p1.addEdge("?x3", "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#advisor", "?x6");
		Table<String> t1 = m_op.load(IndexDescription.POCS, SO, names("?x1", "GraduateStudent70"), DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#name", DataField.OBJECT, "GraduateStudent70");

		Table<String> t2 = m_op.load(IndexDescription.POCS, SO, names("?x4", "Publication1"), DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#name", DataField.OBJECT, "Publication1");

		Table<String> t3 = m_op.load(IndexDescription.POCS, SO, names("?x2", "FullProfessor6"), DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#name", DataField.OBJECT, "FullProfessor6");

		Table<String> t4_a = m_op.indexJoin(t2, "?x4", IndexDescription.PSOC, SO, names("?x4", "?x1"), DataField.SUBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#publicationAuthor");
		Table<String> t4 = m_op.mergeJoin(t4_a, t1, "?x1");

		Table<String> t5_a = m_op.indexJoin(t4, "?x4", IndexDescription.PSOC, SO, names("?x4", "?x2"), DataField.SUBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#publicationAuthor");
		Table<String> t5 = m_op.mergeJoin(t5_a, t3, "?x2");

		Table<String> t6 = m_op.indexJoin(t5, "?x2", IndexDescription.POCS, SO, names("?x5", "?x2"), DataField.OBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#publicationAuthor");

		Table<String> t7 = m_op.indexJoin(t6, "?x5", IndexDescription.PSOC, SO, names("?x5", "?x3"), DataField.SUBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#publicationAuthor");
		t7 = m_op.refineWithPrunedPart(p1, "?x3", t7);
		t7 = m_op.compact(t7, Arrays.asList("?x4", "?x1", "?x2"));

		}
		public void query_q6_spc_2() throws StorageException {
		PrunedQueryPart p1 = new PrunedQueryPart("p", null);
		p1.addEdge("?x7", "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#takesCourse", "?x6");
		p1.addEdge("?x9", "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#teacherOf", "?x6");
		PrunedQueryPart p2 = new PrunedQueryPart("p", null);
		p2.addEdge("?x8", "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#undergraduateDegreeFrom", "?x10");
		Table<String> t1 = m_op.load(IndexDescription.POCS, SO, names("?x1", "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#Publication"), DataField.PROPERTY, "http://www.w3.org/1999/02/22-rdf-syntax-ns#type", DataField.OBJECT, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#Publication");

		Table<String> t2 = m_op.load(IndexDescription.POCS, SO, names("?x8", "GraduateStudent75"), DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#name", DataField.OBJECT, "GraduateStudent75");
		t2 = m_op.refineWithPrunedPart(p2, "?x8", t2);

		Table<String> t3_a = m_op.load(IndexDescription.POCS, SO, names("?x1", "Publication1"), DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#name", DataField.OBJECT, "Publication1");
		Table<String> t3 = m_op.mergeJoin(t3_a, t1, "?x1");
		t3 = m_op.compact(t3, Arrays.asList("?x1"));

		Table<String> t4 = m_op.load(IndexDescription.POCS, SO, names("?x4", "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#AssociateProfessor"), DataField.PROPERTY, "http://www.w3.org/1999/02/22-rdf-syntax-ns#type", DataField.OBJECT, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#AssociateProfessor");

		Table<String> t5 = m_op.load(IndexDescription.POCS, SO, names("?x3", "FullProfessor3"), DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#name", DataField.OBJECT, "FullProfessor3");

		Table<String> t6_a = m_op.indexJoin(t3, "?x1", IndexDescription.PSOC, SO, names("?x1", "?x4"), DataField.SUBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#publicationAuthor");
		Table<String> t6 = m_op.mergeJoin(t6_a, t4, "?x4");

		Table<String> t7_a = m_op.indexJoin(t6, "?x1", IndexDescription.PSOC, SO, names("?x1", "?x8"), DataField.SUBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#publicationAuthor");
		Table<String> t7 = m_op.mergeJoin(t7_a, t2, "?x8");

		Table<String> t8 = m_op.indexJoin(t7, "?x4", IndexDescription.PSOC, SO, names("?x4", "?x2"), DataField.SUBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#undergraduateDegreeFrom");

		Table<String> t9 = m_op.indexJoin(t5, "?x3", IndexDescription.POCS, SO, names("?x5", "?x3"), DataField.OBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#publicationAuthor");

		Table<String> t10_a = m_op.indexJoin(t9, "?x3", IndexDescription.PSOC, SO, names("?x3", "?x2"), DataField.SUBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#doctoralDegreeFrom");
		Table<String> t10 = m_op.mergeJoin(t10_a, t8, "?x2");
		t10 = m_op.compact(t10, Arrays.asList("?x3", "?x4", "?x1", "?x8", "?x5"));

		Table<String> t11 = m_op.indexJoin(t10, "?x5", IndexDescription.PSOC, SO, names("?x5", "?x7"), DataField.SUBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#publicationAuthor");
		t11 = m_op.refineWithPrunedPart(p1, "?x7", t11);
		t11 = m_op.compact(t11, Arrays.asList("?x3", "?x4", "?x1", "?x8"));

		}
		public void query_q7_spc_2() throws StorageException {
		PrunedQueryPart p1 = new PrunedQueryPart("p", null);
		p1.addEdge("?x6", "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#subOrganizationOf", "?x4");
		p1.addEdge("?x7", "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#subOrganizationOf", "?x4");
		PrunedQueryPart p2 = new PrunedQueryPart("p", null);
		p2.addEdge("?x8", "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#teacherOf", "?x5");
		p2.addEdge("?x3", "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#takesCourse", "?x5");
		Table<String> t1 = m_op.load(IndexDescription.POCS, SO, names("?x1", "Research28"), DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#researchInterest", DataField.OBJECT, "Research28");

		Table<String> t2 = m_op.load(IndexDescription.POCS, SO, names("?x6", "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#Department"), DataField.PROPERTY, "http://www.w3.org/1999/02/22-rdf-syntax-ns#type", DataField.OBJECT, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#Department");
		t2 = m_op.refineWithPrunedPart(p1, "?x6", t2);

		Table<String> t3_a = m_op.indexJoin(t1, "?x1", IndexDescription.PSOC, SO, names("?x1", "?x6"), DataField.SUBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#worksFor");
		Table<String> t3 = m_op.mergeJoin(t3_a, t2, "?x6");

		Table<String> t4 = m_op.indexJoin(t3, "?x1", IndexDescription.POCS, SO, names("?x2", "?x1"), DataField.OBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#publicationAuthor");

		Table<String> t5 = m_op.indexJoin(t4, "?x2", IndexDescription.PSOC, SO, names("?x2", "?x3"), DataField.SUBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#publicationAuthor");
		t5 = m_op.refineWithPrunedPart(p2, "?x3", t5);
		t5 = m_op.compact(t5, Arrays.asList("?x1", "?x6"));

		}
		public void query_q8_spc_2() throws StorageException {
		PrunedQueryPart p1 = new PrunedQueryPart("p", null);
		p1.addEdge("?x5", "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#subOrganizationOf", "?x10");
		Table<String> t1 = m_op.load(IndexDescription.POCS, SO, names("?x1", "GraduateStudent98@Department3.University0.edu"), DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#emailAddress", DataField.OBJECT, "GraduateStudent98@Department3.University0.edu");

		Table<String> t2 = m_op.load(IndexDescription.POCS, SO, names("?x5", "Department3"), DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#name", DataField.OBJECT, "Department3");

		Table<String> t3 = m_op.load(IndexDescription.POCS, SO, names("?x3", "Publication14"), DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#name", DataField.OBJECT, "Publication14");

		Table<String> t4_a = m_op.indexJoin(t1, "?x1", IndexDescription.PSOC, SO, names("?x1", "?x5"), DataField.SUBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#memberOf");
		Table<String> t4 = m_op.mergeJoin(t4_a, t2, "?x5");

		Table<String> t5 = m_op.indexJoin(t3, "?x3", IndexDescription.PSOC, SO, names("?x3", "?x9"), DataField.SUBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#publicationAuthor");

		Table<String> t6 = m_op.indexJoin(t4, "?x5", IndexDescription.POCS, SO, names("?x7", "?x5"), DataField.OBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#memberOf");

		Table<String> t7_a = m_op.indexJoin(t5, "?x9", IndexDescription.PSOC, SO, names("?x9", "?x5"), DataField.SUBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#worksFor");
		Table<String> t7 = m_op.mergeJoin(t7_a, t6, "?x5");
		t7 = m_op.compact(t7, Arrays.asList("?x3", "?x1", "?x7", "?x5"));

		Table<String> t8 = m_op.indexJoin(t7, "?x1", IndexDescription.PSOC, SO, names("?x1", "?x4"), DataField.SUBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#advisor");

		Table<String> t9 = m_op.indexJoin(t8, "?x4", IndexDescription.POCS, SO, names("?x6", "?x4"), DataField.OBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#advisor");
		t9 = m_op.compact(t9, Arrays.asList("?x3", "?x1", "?x7", "?x5", "?x6"));

		Table<String> t10 = m_op.indexJoin(t9, "?x7", IndexDescription.PSOC, SO, names("?x7", "?x2"), DataField.SUBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#advisor");
		t10 = m_op.compact(t10, Arrays.asList("?x3", "?x1", "?x2", "?x5", "?x6"));

		Table<String> t11 = m_op.indexJoin(t10, "?x6", IndexDescription.POCS, SO, names("?x8", "?x6"), DataField.OBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#publicationAuthor");
		t11 = m_op.compact(t11, Arrays.asList("?x3", "?x1", "?x2", "?x8", "?x5"));

		Table<String> t12 = m_op.indexJoin(t11, "?x8", "?x2", IndexDescription.PSOC, SO, names("?x8", "?x2"), DataField.SUBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#publicationAuthor");
		t12 = m_op.compact(t12, Arrays.asList("?x3", "?x1", "?x5"));
		t12 = m_op.refineWithPrunedPart(p1, "?x5", t12);

		}
		public void query_q9_spc_2() throws StorageException {
		PrunedQueryPart p1 = new PrunedQueryPart("p", null);
		p1.addEdge("?x10", "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#takesCourse", "?x4");
		p1.addEdge("?x10", "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#undergraduateDegreeFrom", "?x13");
		Table<String> t1 = m_op.load(IndexDescription.POCS, SO, names("?x1", "GraduateStudent84@Department14.University0.edu"), DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#emailAddress", DataField.OBJECT, "GraduateStudent84@Department14.University0.edu");

		Table<String> t2 = m_op.load(IndexDescription.POCS, SO, names("?x11", "GraduateStudent31@Department3.University0.edu"), DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#emailAddress", DataField.OBJECT, "GraduateStudent31@Department3.University0.edu");

		Table<String> t3 = m_op.load(IndexDescription.POCS, SO, names("?x5", "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#Department"), DataField.PROPERTY, "http://www.w3.org/1999/02/22-rdf-syntax-ns#type", DataField.OBJECT, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#Department");

		Table<String> t4 = m_op.load(IndexDescription.POCS, SO, names("?x9", "Publication13"), DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#name", DataField.OBJECT, "Publication13");

		Table<String> t5_a = m_op.indexJoin(t4, "?x9", IndexDescription.PSOC, SO, names("?x9", "?x11"), DataField.SUBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#publicationAuthor");
		Table<String> t5 = m_op.mergeJoin(t5_a, t2, "?x11");

		Table<String> t6_a = m_op.indexJoin(t1, "?x1", IndexDescription.PSOC, SO, names("?x1", "?x5"), DataField.SUBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#memberOf");
		Table<String> t6 = m_op.mergeJoin(t6_a, t3, "?x5");

		Table<String> t7 = m_op.indexJoin(t5, "?x11", IndexDescription.PSOC, SO, names("?x11", "?x12"), DataField.SUBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#undergraduateDegreeFrom");

		Table<String> t8 = m_op.indexJoin(t6, "?x5", IndexDescription.PSOC, SO, names("?x5", "?x2"), DataField.SUBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#subOrganizationOf");

		Table<String> t9_a = m_op.indexJoin(t8, "?x1", IndexDescription.PSOC, SO, names("?x1", "?x12"), DataField.SUBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#undergraduateDegreeFrom");
		Table<String> t9 = m_op.mergeJoin(t9_a, t7, "?x12");
		t9 = m_op.compact(t9, Arrays.asList("?x1", "?x2", "?x5", "?x11", "?x9"));

		Table<String> t10 = m_op.indexJoin(t9, "?x2", IndexDescription.POCS, SO, names("?x7", "?x2"), DataField.OBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#mastersDegreeFrom");
		t10 = m_op.compact(t10, Arrays.asList("?x1", "?x7", "?x5", "?x11", "?x9"));

		Table<String> t11 = m_op.indexJoin(t10, "?x7", IndexDescription.POCS, SO, names("?x8", "?x7"), DataField.OBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#publicationAuthor");
		t11 = m_op.compact(t11, Arrays.asList("?x1", "?x8", "?x5", "?x11", "?x9"));

		Table<String> t12 = m_op.indexJoin(t11, "?x8", IndexDescription.PSOC, SO, names("?x8", "?x6"), DataField.SUBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#publicationAuthor");
		t12 = m_op.compact(t12, Arrays.asList("?x1", "?x5", "?x6", "?x11", "?x9"));

		Table<String> t13 = m_op.indexJoin(t12, "?x6", IndexDescription.PSOC, SO, names("?x6", "?x3"), DataField.SUBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#advisor");
		t13 = m_op.compact(t13, Arrays.asList("?x3", "?x1", "?x5", "?x11", "?x9"));

		Table<String> t14 = m_op.indexJoin(t13, "?x3", IndexDescription.PSOC, SO, names("?x3", "?x4"), DataField.SUBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#teacherOf");
		t14 = m_op.refineWithPrunedPart(p1, "?x4", t14);
		t14 = m_op.compact(t14, Arrays.asList("?x1", "?x5", "?x11", "?x9"));

		}
		public void query_q10_spc_2() throws StorageException {
		PrunedQueryPart p1 = new PrunedQueryPart("p", null);
		p1.addEdge("?x3", "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#undergraduateDegreeFrom", "?x6");
		p1.addEdge("?x12", "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#publicationAuthor", "?x3");
		Table<String> t1 = m_op.load(IndexDescription.POCS, SO, names("?x1", "UndergraduateStudent180"), DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#name", DataField.OBJECT, "UndergraduateStudent180");

		Table<String> t2 = m_op.load(IndexDescription.POCS, SO, names("?x10", "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#Department"), DataField.PROPERTY, "http://www.w3.org/1999/02/22-rdf-syntax-ns#type", DataField.OBJECT, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#Department");

		Table<String> t3 = m_op.load(IndexDescription.POCS, SO, names("?x5", "AssociateProfessor6"), DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#name", DataField.OBJECT, "AssociateProfessor6");

		Table<String> t4_a = m_op.load(IndexDescription.POCS, SO, names("?x10", "Department9"), DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#name", DataField.OBJECT, "Department9");
		Table<String> t4 = m_op.mergeJoin(t4_a, t2, "?x10");
		t4 = m_op.compact(t4, Arrays.asList("?x10"));

		Table<String> t5_a = m_op.indexJoin(t3, "?x5", IndexDescription.PSOC, SO, names("?x5", "?x10"), DataField.SUBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#worksFor");
		Table<String> t5 = m_op.mergeJoin(t5_a, t4, "?x10");

		Table<String> t6_a = m_op.indexJoin(t1, "?x1", IndexDescription.PSOC, SO, names("?x1", "?x10"), DataField.SUBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#memberOf");
		Table<String> t6 = m_op.mergeJoin(t6_a, t5, "?x10");

		Table<String> t7 = m_op.indexJoin(t6, "?x5", IndexDescription.POCS, SO, names("?x2", "?x5"), DataField.OBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#publicationAuthor");

		Table<String> t8 = m_op.indexJoin(t7, "?x2", IndexDescription.PSOC, SO, names("?x2", "?x7"), DataField.SUBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#publicationAuthor");
		t8 = m_op.compact(t8, Arrays.asList("?x1", "?x7", "?x5", "?x10"));

		Table<String> t9 = m_op.indexJoin(t8, "?x7", IndexDescription.PSOC, SO, names("?x7", "?x11"), DataField.SUBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#takesCourse");
		t9 = m_op.compact(t9, Arrays.asList("?x1", "?x5", "?x10", "?x11"));

		Table<String> t10 = m_op.indexJoin(t9, "?x11", IndexDescription.POCS, SO, names("?x4", "?x11"), DataField.OBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#takesCourse");
		t10 = m_op.compact(t10, Arrays.asList("?x4", "?x1", "?x5", "?x10"));

		Table<String> t11 = m_op.indexJoin(t10, "?x4", IndexDescription.PSOC, SO, names("?x4", "?x9"), DataField.SUBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#takesCourse");
		t11 = m_op.compact(t11, Arrays.asList("?x1", "?x5", "?x10", "?x9"));

		Table<String> t12 = m_op.indexJoin(t11, "?x9", IndexDescription.POCS, SO, names("?x8", "?x9"), DataField.OBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#teacherOf");
		t12 = m_op.compact(t12, Arrays.asList("?x1", "?x8", "?x5", "?x10"));

		Table<String> t13 = m_op.indexJoin(t12, "?x8", IndexDescription.PSOC, SO, names("?x8", "?x6"), DataField.SUBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#doctoralDegreeFrom");
		t13 = m_op.refineWithPrunedPart(p1, "?x6", t13);
		t13 = m_op.compact(t13, Arrays.asList("?x1", "?x5", "?x10"));

		}
		public void query_q1_vp() throws StorageException {
			Table<String> t1 = m_op.load(IndexDescription.POCS, SO, names("?x1", "UndergraduateStudent75@Department8.University0.edu"), DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#emailAddress", DataField.OBJECT, "UndergraduateStudent75@Department8.University0.edu");

			Table<String> t2 = m_op.load(IndexDescription.POCS, SO, names("?x2", "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#Course"), DataField.PROPERTY, "http://www.w3.org/1999/02/22-rdf-syntax-ns#type", DataField.OBJECT, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#Course");

			Table<String> t3_a = m_op.load(IndexDescription.POCS, SO, names("?x2", "Course5"), DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#name", DataField.OBJECT, "Course5");
			Table<String> t3 = m_op.mergeJoin(t3_a, t2, "?x2");
			t3 = m_op.compact(t3, Arrays.asList("?x2"));

			Table<String> t4_a = m_op.indexJoin(t1, "?x1", IndexDescription.PSOC, SO, names("?x1", "?x2"), DataField.SUBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#takesCourse");
			Table<String> t4 = m_op.mergeJoin(t4_a, t3, "?x2");

			}
			public void query_q2_vp() throws StorageException {
			Table<String> t1 = m_op.load(IndexDescription.POCS, SO, names("?x1", "GraduateStudent144@Department11.University0.edu"), DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#emailAddress", DataField.OBJECT, "GraduateStudent144@Department11.University0.edu");

			Table<String> t2 = m_op.load(IndexDescription.POCS, SO, names("?x3", "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#AssociateProfessor"), DataField.PROPERTY, "http://www.w3.org/1999/02/22-rdf-syntax-ns#type", DataField.OBJECT, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#AssociateProfessor");

			Table<String> t3_a = m_op.indexJoin(t1, "?x1", IndexDescription.PSOC, SO, names("?x1", "?x3"), DataField.SUBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#advisor");
			Table<String> t3 = m_op.mergeJoin(t3_a, t2, "?x3");

			Table<String> t4 = m_op.indexJoin(t3, "?x1", IndexDescription.PSOC, SO, names("?x1", "?x2"), DataField.SUBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#undergraduateDegreeFrom");
			t4 = m_op.compact(t4, Arrays.asList("?x3", "?x1"));

			}
			public void query_q3_vp() throws StorageException {
			Table<String> t1 = m_op.load(IndexDescription.POCS, SO, names("?x1", "AssistantProfessor5"), DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#name", DataField.OBJECT, "AssistantProfessor5");

			Table<String> t2 = m_op.load(IndexDescription.POCS, SO, names("?x3", "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#University"), DataField.PROPERTY, "http://www.w3.org/1999/02/22-rdf-syntax-ns#type", DataField.OBJECT, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#University");

			Table<String> t3_a = m_op.indexJoin(t1, "?x1", IndexDescription.PSOC, SO, names("?x1", "?x3"), DataField.SUBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#doctoralDegreeFrom");
			Table<String> t3 = m_op.mergeJoin(t3_a, t2, "?x3");

			Table<String> t4 = m_op.indexJoin(t3, "?x1", IndexDescription.PSOC, SO, names("?x1", "?x2"), DataField.SUBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#worksFor");
			t4 = m_op.compact(t4, Arrays.asList("?x3", "?x1"));

			}
			public void query_q4_vp() throws StorageException {
			Table<String> t1 = m_op.load(IndexDescription.POCS, SO, names("?x1", "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#UndergraduateStudent"), DataField.PROPERTY, "http://www.w3.org/1999/02/22-rdf-syntax-ns#type", DataField.OBJECT, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#UndergraduateStudent");

			Table<String> t2 = m_op.load(IndexDescription.POCS, SO, names("?x4", "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#Course"), DataField.PROPERTY, "http://www.w3.org/1999/02/22-rdf-syntax-ns#type", DataField.OBJECT, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#Course");

			Table<String> t3_a = m_op.load(IndexDescription.POCS, SO, names("?x1", "UndergraduateStudent200@Department5.University0.edu"), DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#emailAddress", DataField.OBJECT, "UndergraduateStudent200@Department5.University0.edu");
			Table<String> t3 = m_op.mergeJoin(t3_a, t1, "?x1");
			t3 = m_op.compact(t3, Arrays.asList("?x1"));

			Table<String> t4 = m_op.load(IndexDescription.POCS, SO, names("?x2", "Department5"), DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#name", DataField.OBJECT, "Department5");

			Table<String> t5_a = m_op.load(IndexDescription.POCS, SO, names("?x4", "Course30"), DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#name", DataField.OBJECT, "Course30");
			Table<String> t5 = m_op.mergeJoin(t5_a, t2, "?x4");
			t5 = m_op.compact(t5, Arrays.asList("?x4"));

			Table<String> t6_a = m_op.indexJoin(t3, "?x1", IndexDescription.PSOC, SO, names("?x1", "?x2"), DataField.SUBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#memberOf");
			Table<String> t6 = m_op.mergeJoin(t6_a, t4, "?x2");

			Table<String> t7 = m_op.indexJoin(t5, "?x4", IndexDescription.POCS, SO, names("?x5", "?x4"), DataField.OBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#takesCourse");

			Table<String> t8 = m_op.indexJoin(t6, "?x2", IndexDescription.PSOC, SO, names("?x2", "?x6"), DataField.SUBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#subOrganizationOf");
			t8 = m_op.compact(t8, Arrays.asList("?x1", "?x2"));

			Table<String> t9 = m_op.indexJoin(t7, "?x4", IndexDescription.POCS, SO, names("?x3", "?x4"), DataField.OBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#teachingAssistantOf");

			Table<String> t10_a = m_op.indexJoin(t9, "?x5", IndexDescription.PSOC, SO, names("?x5", "?x2"), DataField.SUBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#memberOf");
			Table<String> t10 = m_op.mergeJoin(t10_a, t8, "?x2");
			t10 = m_op.compact(t10, Arrays.asList("?x3", "?x4", "?x1", "?x2"));

			Table<String> t11 = m_op.indexJoin(t10, "?x3", "?x2", IndexDescription.PSOC, SO, names("?x3", "?x2"), DataField.SUBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#memberOf");
			t11 = m_op.compact(t11, Arrays.asList("?x4", "?x1", "?x2"));

			}
			public void query_q5_vp() throws StorageException {
			Table<String> t1 = m_op.load(IndexDescription.POCS, SO, names("?x1", "GraduateStudent70"), DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#name", DataField.OBJECT, "GraduateStudent70");

			Table<String> t2 = m_op.load(IndexDescription.POCS, SO, names("?x4", "Publication1"), DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#name", DataField.OBJECT, "Publication1");

			Table<String> t3 = m_op.load(IndexDescription.POCS, SO, names("?x2", "FullProfessor6"), DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#name", DataField.OBJECT, "FullProfessor6");

			Table<String> t4_a = m_op.indexJoin(t2, "?x4", IndexDescription.PSOC, SO, names("?x4", "?x1"), DataField.SUBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#publicationAuthor");
			Table<String> t4 = m_op.mergeJoin(t4_a, t1, "?x1");

			Table<String> t5_a = m_op.indexJoin(t4, "?x4", IndexDescription.PSOC, SO, names("?x4", "?x2"), DataField.SUBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#publicationAuthor");
			Table<String> t5 = m_op.mergeJoin(t5_a, t3, "?x2");

			Table<String> t6 = m_op.indexJoin(t5, "?x2", IndexDescription.POCS, SO, names("?x5", "?x2"), DataField.OBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#publicationAuthor");

			Table<String> t7 = m_op.indexJoin(t6, "?x5", IndexDescription.PSOC, SO, names("?x5", "?x3"), DataField.SUBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#publicationAuthor");
			t7 = m_op.compact(t7, Arrays.asList("?x3", "?x4", "?x1", "?x2"));

			Table<String> t8 = m_op.indexJoin(t7, "?x3", IndexDescription.PSOC, SO, names("?x3", "?x6"), DataField.SUBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#advisor");
			t8 = m_op.compact(t8, Arrays.asList("?x4", "?x1", "?x2", "?x6"));

			Table<String> t9 = m_op.indexJoin(t8, "?x6", IndexDescription.POCS, SO, names("?x7", "?x6"), DataField.OBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#publicationAuthor");
			t9 = m_op.compact(t9, Arrays.asList("?x4", "?x1", "?x2"));

			}
			public void query_q6_vp() throws StorageException {
			Table<String> t1 = m_op.load(IndexDescription.POCS, SO, names("?x1", "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#Publication"), DataField.PROPERTY, "http://www.w3.org/1999/02/22-rdf-syntax-ns#type", DataField.OBJECT, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#Publication");

			Table<String> t2 = m_op.load(IndexDescription.POCS, SO, names("?x8", "GraduateStudent75"), DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#name", DataField.OBJECT, "GraduateStudent75");

			Table<String> t3_a = m_op.load(IndexDescription.POCS, SO, names("?x1", "Publication1"), DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#name", DataField.OBJECT, "Publication1");
			Table<String> t3 = m_op.mergeJoin(t3_a, t1, "?x1");
			t3 = m_op.compact(t3, Arrays.asList("?x1"));

			Table<String> t4 = m_op.load(IndexDescription.POCS, SO, names("?x4", "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#AssociateProfessor"), DataField.PROPERTY, "http://www.w3.org/1999/02/22-rdf-syntax-ns#type", DataField.OBJECT, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#AssociateProfessor");

			Table<String> t5 = m_op.load(IndexDescription.POCS, SO, names("?x3", "FullProfessor3"), DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#name", DataField.OBJECT, "FullProfessor3");

			Table<String> t6_a = m_op.indexJoin(t3, "?x1", IndexDescription.PSOC, SO, names("?x1", "?x4"), DataField.SUBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#publicationAuthor");
			Table<String> t6 = m_op.mergeJoin(t6_a, t4, "?x4");

			Table<String> t7_a = m_op.indexJoin(t6, "?x1", IndexDescription.PSOC, SO, names("?x1", "?x8"), DataField.SUBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#publicationAuthor");
			Table<String> t7 = m_op.mergeJoin(t7_a, t2, "?x8");

			Table<String> t8 = m_op.indexJoin(t7, "?x4", IndexDescription.PSOC, SO, names("?x4", "?x2"), DataField.SUBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#undergraduateDegreeFrom");

			Table<String> t9 = m_op.indexJoin(t8, "?x8", IndexDescription.PSOC, SO, names("?x8", "?x10"), DataField.SUBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#undergraduateDegreeFrom");
			t9 = m_op.compact(t9, Arrays.asList("?x4", "?x1", "?x2", "?x8"));

			Table<String> t10 = m_op.indexJoin(t5, "?x3", IndexDescription.POCS, SO, names("?x5", "?x3"), DataField.OBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#publicationAuthor");

			Table<String> t11_a = m_op.indexJoin(t10, "?x3", IndexDescription.PSOC, SO, names("?x3", "?x2"), DataField.SUBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#doctoralDegreeFrom");
			Table<String> t11 = m_op.mergeJoin(t11_a, t9, "?x2");
			t11 = m_op.compact(t11, Arrays.asList("?x3", "?x4", "?x1", "?x8", "?x5"));

			Table<String> t12 = m_op.indexJoin(t11, "?x5", IndexDescription.PSOC, SO, names("?x5", "?x7"), DataField.SUBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#publicationAuthor");
			t12 = m_op.compact(t12, Arrays.asList("?x3", "?x4", "?x1", "?x7", "?x8"));

			Table<String> t13 = m_op.indexJoin(t12, "?x7", IndexDescription.PSOC, SO, names("?x7", "?x6"), DataField.SUBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#takesCourse");
			t13 = m_op.compact(t13, Arrays.asList("?x3", "?x4", "?x1", "?x8", "?x6"));

			Table<String> t14 = m_op.indexJoin(t13, "?x6", IndexDescription.POCS, SO, names("?x9", "?x6"), DataField.OBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#teacherOf");
			t14 = m_op.compact(t14, Arrays.asList("?x3", "?x4", "?x1", "?x8"));

			}
			public void query_q7_vp() throws StorageException {
			Table<String> t1 = m_op.load(IndexDescription.POCS, SO, names("?x1", "Research28"), DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#researchInterest", DataField.OBJECT, "Research28");

			Table<String> t2 = m_op.load(IndexDescription.POCS, SO, names("?x6", "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#Department"), DataField.PROPERTY, "http://www.w3.org/1999/02/22-rdf-syntax-ns#type", DataField.OBJECT, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#Department");

			Table<String> t3_a = m_op.indexJoin(t1, "?x1", IndexDescription.PSOC, SO, names("?x1", "?x6"), DataField.SUBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#worksFor");
			Table<String> t3 = m_op.mergeJoin(t3_a, t2, "?x6");

			Table<String> t4 = m_op.indexJoin(t3, "?x6", IndexDescription.PSOC, SO, names("?x6", "?x4"), DataField.SUBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#subOrganizationOf");

			Table<String> t5 = m_op.indexJoin(t4, "?x1", IndexDescription.POCS, SO, names("?x2", "?x1"), DataField.OBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#publicationAuthor");

			Table<String> t6 = m_op.indexJoin(t5, "?x4", IndexDescription.POCS, SO, names("?x7", "?x4"), DataField.OBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#subOrganizationOf");
			t6 = m_op.compact(t6, Arrays.asList("?x1", "?x2", "?x6"));

			Table<String> t7 = m_op.indexJoin(t6, "?x2", IndexDescription.PSOC, SO, names("?x2", "?x3"), DataField.SUBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#publicationAuthor");
			t7 = m_op.compact(t7, Arrays.asList("?x3", "?x1", "?x6"));

			Table<String> t8 = m_op.indexJoin(t7, "?x3", IndexDescription.PSOC, SO, names("?x3", "?x5"), DataField.SUBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#takesCourse");
			t8 = m_op.compact(t8, Arrays.asList("?x1", "?x5", "?x6"));

			Table<String> t9 = m_op.indexJoin(t8, "?x5", IndexDescription.POCS, SO, names("?x8", "?x5"), DataField.OBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#teacherOf");
			t9 = m_op.compact(t9, Arrays.asList("?x1", "?x6"));

			}
			public void query_q8_vp() throws StorageException {
			Table<String> t1 = m_op.load(IndexDescription.POCS, SO, names("?x1", "GraduateStudent98@Department3.University0.edu"), DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#emailAddress", DataField.OBJECT, "GraduateStudent98@Department3.University0.edu");

			Table<String> t2 = m_op.load(IndexDescription.POCS, SO, names("?x5", "Department3"), DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#name", DataField.OBJECT, "Department3");

			Table<String> t3 = m_op.load(IndexDescription.POCS, SO, names("?x3", "Publication14"), DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#name", DataField.OBJECT, "Publication14");

			Table<String> t4_a = m_op.indexJoin(t1, "?x1", IndexDescription.PSOC, SO, names("?x1", "?x5"), DataField.SUBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#memberOf");
			Table<String> t4 = m_op.mergeJoin(t4_a, t2, "?x5");

			Table<String> t5 = m_op.indexJoin(t3, "?x3", IndexDescription.PSOC, SO, names("?x3", "?x9"), DataField.SUBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#publicationAuthor");

			Table<String> t6 = m_op.indexJoin(t4, "?x5", IndexDescription.POCS, SO, names("?x7", "?x5"), DataField.OBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#memberOf");

			Table<String> t7_a = m_op.indexJoin(t5, "?x9", IndexDescription.PSOC, SO, names("?x9", "?x5"), DataField.SUBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#worksFor");
			Table<String> t7 = m_op.mergeJoin(t7_a, t6, "?x5");
			t7 = m_op.compact(t7, Arrays.asList("?x3", "?x1", "?x7", "?x5"));

			Table<String> t8 = m_op.indexJoin(t7, "?x1", IndexDescription.PSOC, SO, names("?x1", "?x4"), DataField.SUBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#advisor");

			Table<String> t9 = m_op.indexJoin(t8, "?x5", IndexDescription.PSOC, SO, names("?x5", "?x10"), DataField.SUBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#subOrganizationOf");
			t9 = m_op.compact(t9, Arrays.asList("?x3", "?x4", "?x1", "?x7", "?x5"));

			Table<String> t10 = m_op.indexJoin(t9, "?x4", IndexDescription.POCS, SO, names("?x6", "?x4"), DataField.OBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#advisor");
			t10 = m_op.compact(t10, Arrays.asList("?x3", "?x1", "?x7", "?x5", "?x6"));

			Table<String> t11 = m_op.indexJoin(t10, "?x7", IndexDescription.PSOC, SO, names("?x7", "?x2"), DataField.SUBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#advisor");
			t11 = m_op.compact(t11, Arrays.asList("?x3", "?x1", "?x2", "?x5", "?x6"));

			Table<String> t12 = m_op.indexJoin(t11, "?x6", IndexDescription.POCS, SO, names("?x8", "?x6"), DataField.OBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#publicationAuthor");
			t12 = m_op.compact(t12, Arrays.asList("?x3", "?x1", "?x2", "?x8", "?x5"));

			Table<String> t13 = m_op.indexJoin(t12, "?x8", "?x2", IndexDescription.PSOC, SO, names("?x8", "?x2"), DataField.SUBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#publicationAuthor");
			t13 = m_op.compact(t13, Arrays.asList("?x3", "?x1", "?x5"));

			}
			public void query_q9_vp() throws StorageException {
			Table<String> t1 = m_op.load(IndexDescription.POCS, SO, names("?x1", "GraduateStudent84@Department14.University0.edu"), DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#emailAddress", DataField.OBJECT, "GraduateStudent84@Department14.University0.edu");

			Table<String> t2 = m_op.load(IndexDescription.POCS, SO, names("?x11", "GraduateStudent31@Department3.University0.edu"), DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#emailAddress", DataField.OBJECT, "GraduateStudent31@Department3.University0.edu");

			Table<String> t3 = m_op.load(IndexDescription.POCS, SO, names("?x5", "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#Department"), DataField.PROPERTY, "http://www.w3.org/1999/02/22-rdf-syntax-ns#type", DataField.OBJECT, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#Department");

			Table<String> t4 = m_op.load(IndexDescription.POCS, SO, names("?x9", "Publication13"), DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#name", DataField.OBJECT, "Publication13");

			Table<String> t5_a = m_op.indexJoin(t4, "?x9", IndexDescription.PSOC, SO, names("?x9", "?x11"), DataField.SUBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#publicationAuthor");
			Table<String> t5 = m_op.mergeJoin(t5_a, t2, "?x11");

			Table<String> t6_a = m_op.indexJoin(t1, "?x1", IndexDescription.PSOC, SO, names("?x1", "?x5"), DataField.SUBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#memberOf");
			Table<String> t6 = m_op.mergeJoin(t6_a, t3, "?x5");

			Table<String> t7 = m_op.indexJoin(t5, "?x11", IndexDescription.PSOC, SO, names("?x11", "?x12"), DataField.SUBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#undergraduateDegreeFrom");

			Table<String> t8 = m_op.indexJoin(t6, "?x5", IndexDescription.PSOC, SO, names("?x5", "?x2"), DataField.SUBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#subOrganizationOf");

			Table<String> t9_a = m_op.indexJoin(t8, "?x1", IndexDescription.PSOC, SO, names("?x1", "?x12"), DataField.SUBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#undergraduateDegreeFrom");
			Table<String> t9 = m_op.mergeJoin(t9_a, t7, "?x12");
			t9 = m_op.compact(t9, Arrays.asList("?x1", "?x2", "?x5", "?x11", "?x9"));

			Table<String> t10 = m_op.indexJoin(t9, "?x2", IndexDescription.POCS, SO, names("?x7", "?x2"), DataField.OBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#mastersDegreeFrom");
			t10 = m_op.compact(t10, Arrays.asList("?x1", "?x7", "?x5", "?x11", "?x9"));

			Table<String> t11 = m_op.indexJoin(t10, "?x7", IndexDescription.POCS, SO, names("?x8", "?x7"), DataField.OBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#publicationAuthor");
			t11 = m_op.compact(t11, Arrays.asList("?x1", "?x8", "?x5", "?x11", "?x9"));

			Table<String> t12 = m_op.indexJoin(t11, "?x8", IndexDescription.PSOC, SO, names("?x8", "?x6"), DataField.SUBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#publicationAuthor");
			t12 = m_op.compact(t12, Arrays.asList("?x1", "?x5", "?x6", "?x11", "?x9"));

			Table<String> t13 = m_op.indexJoin(t12, "?x6", IndexDescription.PSOC, SO, names("?x6", "?x3"), DataField.SUBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#advisor");
			t13 = m_op.compact(t13, Arrays.asList("?x3", "?x1", "?x5", "?x11", "?x9"));

			Table<String> t14 = m_op.indexJoin(t13, "?x3", IndexDescription.PSOC, SO, names("?x3", "?x4"), DataField.SUBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#teacherOf");
			t14 = m_op.compact(t14, Arrays.asList("?x4", "?x1", "?x5", "?x11", "?x9"));

			Table<String> t15 = m_op.indexJoin(t14, "?x4", IndexDescription.POCS, SO, names("?x10", "?x4"), DataField.OBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#takesCourse");
			t15 = m_op.compact(t15, Arrays.asList("?x1", "?x5", "?x10", "?x11", "?x9"));

			Table<String> t16 = m_op.indexJoin(t15, "?x10", IndexDescription.PSOC, SO, names("?x10", "?x13"), DataField.SUBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#undergraduateDegreeFrom");
			t16 = m_op.compact(t16, Arrays.asList("?x1", "?x5", "?x11", "?x9"));

			}
			public void query_q10_vp() throws StorageException {
			Table<String> t1 = m_op.load(IndexDescription.POCS, SO, names("?x1", "UndergraduateStudent180"), DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#name", DataField.OBJECT, "UndergraduateStudent180");

			Table<String> t2 = m_op.load(IndexDescription.POCS, SO, names("?x10", "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#Department"), DataField.PROPERTY, "http://www.w3.org/1999/02/22-rdf-syntax-ns#type", DataField.OBJECT, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#Department");

			Table<String> t3 = m_op.load(IndexDescription.POCS, SO, names("?x5", "AssociateProfessor6"), DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#name", DataField.OBJECT, "AssociateProfessor6");

			Table<String> t4_a = m_op.load(IndexDescription.POCS, SO, names("?x10", "Department9"), DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#name", DataField.OBJECT, "Department9");
			Table<String> t4 = m_op.mergeJoin(t4_a, t2, "?x10");
			t4 = m_op.compact(t4, Arrays.asList("?x10"));

			Table<String> t5_a = m_op.indexJoin(t3, "?x5", IndexDescription.PSOC, SO, names("?x5", "?x10"), DataField.SUBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#worksFor");
			Table<String> t5 = m_op.mergeJoin(t5_a, t4, "?x10");

			Table<String> t6_a = m_op.indexJoin(t1, "?x1", IndexDescription.PSOC, SO, names("?x1", "?x10"), DataField.SUBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#memberOf");
			Table<String> t6 = m_op.mergeJoin(t6_a, t5, "?x10");

			Table<String> t7 = m_op.indexJoin(t6, "?x5", IndexDescription.POCS, SO, names("?x2", "?x5"), DataField.OBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#publicationAuthor");

			Table<String> t8 = m_op.indexJoin(t7, "?x2", IndexDescription.PSOC, SO, names("?x2", "?x7"), DataField.SUBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#publicationAuthor");
			t8 = m_op.compact(t8, Arrays.asList("?x1", "?x7", "?x5", "?x10"));

			Table<String> t9 = m_op.indexJoin(t8, "?x7", IndexDescription.PSOC, SO, names("?x7", "?x11"), DataField.SUBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#takesCourse");
			t9 = m_op.compact(t9, Arrays.asList("?x1", "?x5", "?x10", "?x11"));

			Table<String> t10 = m_op.indexJoin(t9, "?x11", IndexDescription.POCS, SO, names("?x4", "?x11"), DataField.OBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#takesCourse");
			t10 = m_op.compact(t10, Arrays.asList("?x4", "?x1", "?x5", "?x10"));

			Table<String> t11 = m_op.indexJoin(t10, "?x4", IndexDescription.PSOC, SO, names("?x4", "?x9"), DataField.SUBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#takesCourse");
			t11 = m_op.compact(t11, Arrays.asList("?x1", "?x5", "?x10", "?x9"));

			Table<String> t12 = m_op.indexJoin(t11, "?x9", IndexDescription.POCS, SO, names("?x8", "?x9"), DataField.OBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#teacherOf");
			t12 = m_op.compact(t12, Arrays.asList("?x1", "?x8", "?x5", "?x10"));

			Table<String> t13 = m_op.indexJoin(t12, "?x8", IndexDescription.PSOC, SO, names("?x8", "?x6"), DataField.SUBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#doctoralDegreeFrom");
			t13 = m_op.compact(t13, Arrays.asList("?x1", "?x5", "?x6", "?x10"));

			Table<String> t14 = m_op.indexJoin(t13, "?x6", IndexDescription.POCS, SO, names("?x3", "?x6"), DataField.OBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#undergraduateDegreeFrom");
			t14 = m_op.compact(t14, Arrays.asList("?x3", "?x1", "?x5", "?x10"));

			Table<String> t15 = m_op.indexJoin(t14, "?x3", IndexDescription.POCS, SO, names("?x12", "?x3"), DataField.OBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#publicationAuthor");
			t15 = m_op.compact(t15, Arrays.asList("?x1", "?x5", "?x10"));

			}

			public void query_q1_spc_1() throws StorageException {
				Table<String> t1 = m_op.load(IndexDescription.POCS, SO, names("?x1", "UndergraduateStudent75@Department8.University0.edu"), DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#emailAddress", DataField.OBJECT, "UndergraduateStudent75@Department8.University0.edu");

				Table<String> t2 = m_op.load(IndexDescription.POCS, SO, names("?x2", "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#Course"), DataField.PROPERTY, "http://www.w3.org/1999/02/22-rdf-syntax-ns#type", DataField.OBJECT, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#Course");

				Table<String> t3_a = m_op.load(IndexDescription.POCS, SO, names("?x2", "Course5"), DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#name", DataField.OBJECT, "Course5");
				Table<String> t3 = m_op.mergeJoin(t3_a, t2, "?x2");
				t3 = m_op.compact(t3, Arrays.asList("?x2"));

				Table<String> t4_a = m_op.indexJoin(t1, "?x1", IndexDescription.PSOC, SO, names("?x1", "?x2"), DataField.SUBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#takesCourse");
				Table<String> t4 = m_op.mergeJoin(t4_a, t3, "?x2");

				}
				public void query_q2_spc_1() throws StorageException {
				PrunedQueryPart p1 = new PrunedQueryPart("p", null);
				p1.addEdge("?x1", "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#undergraduateDegreeFrom", "?x2");
				Table<String> t1 = m_op.load(IndexDescription.POCS, SO, names("?x1", "GraduateStudent144@Department11.University0.edu"), DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#emailAddress", DataField.OBJECT, "GraduateStudent144@Department11.University0.edu");
				t1 = m_op.refineWithPrunedPart(p1, "?x1", t1);

				Table<String> t2 = m_op.load(IndexDescription.POCS, SO, names("?x3", "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#AssociateProfessor"), DataField.PROPERTY, "http://www.w3.org/1999/02/22-rdf-syntax-ns#type", DataField.OBJECT, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#AssociateProfessor");

				Table<String> t3_a = m_op.indexJoin(t1, "?x1", IndexDescription.PSOC, SO, names("?x1", "?x3"), DataField.SUBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#advisor");
				Table<String> t3 = m_op.mergeJoin(t3_a, t2, "?x3");

				}
				public void query_q3_spc_1() throws StorageException {
				PrunedQueryPart p1 = new PrunedQueryPart("p", null);
				p1.addEdge("?x1", "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#worksFor", "?x2");
				Table<String> t1 = m_op.load(IndexDescription.POCS, SO, names("?x1", "AssistantProfessor5"), DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#name", DataField.OBJECT, "AssistantProfessor5");
				t1 = m_op.refineWithPrunedPart(p1, "?x1", t1);

				Table<String> t2 = m_op.load(IndexDescription.POCS, SO, names("?x3", "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#University"), DataField.PROPERTY, "http://www.w3.org/1999/02/22-rdf-syntax-ns#type", DataField.OBJECT, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#University");

				Table<String> t3_a = m_op.indexJoin(t1, "?x1", IndexDescription.PSOC, SO, names("?x1", "?x3"), DataField.SUBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#doctoralDegreeFrom");
				Table<String> t3 = m_op.mergeJoin(t3_a, t2, "?x3");

				}
				public void query_q4_spc_1() throws StorageException {
				PrunedQueryPart p1 = new PrunedQueryPart("p", null);
				p1.addEdge("?x2", "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#subOrganizationOf", "?x6");
				Table<String> t1 = m_op.load(IndexDescription.POCS, SO, names("?x1", "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#UndergraduateStudent"), DataField.PROPERTY, "http://www.w3.org/1999/02/22-rdf-syntax-ns#type", DataField.OBJECT, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#UndergraduateStudent");

				Table<String> t2 = m_op.load(IndexDescription.POCS, SO, names("?x4", "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#Course"), DataField.PROPERTY, "http://www.w3.org/1999/02/22-rdf-syntax-ns#type", DataField.OBJECT, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#Course");

				Table<String> t3_a = m_op.load(IndexDescription.POCS, SO, names("?x1", "UndergraduateStudent200@Department5.University0.edu"), DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#emailAddress", DataField.OBJECT, "UndergraduateStudent200@Department5.University0.edu");
				Table<String> t3 = m_op.mergeJoin(t3_a, t1, "?x1");
				t3 = m_op.compact(t3, Arrays.asList("?x1"));

				Table<String> t4 = m_op.load(IndexDescription.POCS, SO, names("?x2", "Department5"), DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#name", DataField.OBJECT, "Department5");
				t4 = m_op.refineWithPrunedPart(p1, "?x2", t4);

				Table<String> t5_a = m_op.load(IndexDescription.POCS, SO, names("?x4", "Course30"), DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#name", DataField.OBJECT, "Course30");
				Table<String> t5 = m_op.mergeJoin(t5_a, t2, "?x4");
				t5 = m_op.compact(t5, Arrays.asList("?x4"));

				Table<String> t6_a = m_op.indexJoin(t3, "?x1", IndexDescription.PSOC, SO, names("?x1", "?x2"), DataField.SUBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#memberOf");
				Table<String> t6 = m_op.mergeJoin(t6_a, t4, "?x2");

				Table<String> t7 = m_op.indexJoin(t5, "?x4", IndexDescription.POCS, SO, names("?x5", "?x4"), DataField.OBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#takesCourse");

				Table<String> t8 = m_op.indexJoin(t6, "?x2", IndexDescription.POCS, SO, names("?x3", "?x2"), DataField.OBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#memberOf");

				Table<String> t9_a = m_op.indexJoin(t7, "?x5", IndexDescription.PSOC, SO, names("?x5", "?x2"), DataField.SUBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#memberOf");
				Table<String> t9 = m_op.mergeJoin(t9_a, t8, "?x2");
				t9 = m_op.compact(t9, Arrays.asList("?x3", "?x4", "?x1", "?x2"));

				Table<String> t10 = m_op.indexJoin(t9, "?x3", "?x4", IndexDescription.PSOC, SO, names("?x3", "?x4"), DataField.SUBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#teachingAssistantOf");
				t10 = m_op.compact(t10, Arrays.asList("?x4", "?x1", "?x2"));

				}
				public void query_q5_spc_1() throws StorageException {
				PrunedQueryPart p1 = new PrunedQueryPart("p", null);
				p1.addEdge("?x7", "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#publicationAuthor", "?x6");
				Table<String> t1 = m_op.load(IndexDescription.POCS, SO, names("?x1", "GraduateStudent70"), DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#name", DataField.OBJECT, "GraduateStudent70");

				Table<String> t2 = m_op.load(IndexDescription.POCS, SO, names("?x4", "Publication1"), DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#name", DataField.OBJECT, "Publication1");

				Table<String> t3 = m_op.load(IndexDescription.POCS, SO, names("?x2", "FullProfessor6"), DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#name", DataField.OBJECT, "FullProfessor6");

				Table<String> t4_a = m_op.indexJoin(t2, "?x4", IndexDescription.PSOC, SO, names("?x4", "?x1"), DataField.SUBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#publicationAuthor");
				Table<String> t4 = m_op.mergeJoin(t4_a, t1, "?x1");

				Table<String> t5_a = m_op.indexJoin(t4, "?x4", IndexDescription.PSOC, SO, names("?x4", "?x2"), DataField.SUBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#publicationAuthor");
				Table<String> t5 = m_op.mergeJoin(t5_a, t3, "?x2");

				Table<String> t6 = m_op.indexJoin(t5, "?x2", IndexDescription.POCS, SO, names("?x5", "?x2"), DataField.OBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#publicationAuthor");

				Table<String> t7 = m_op.indexJoin(t6, "?x5", IndexDescription.PSOC, SO, names("?x5", "?x3"), DataField.SUBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#publicationAuthor");
				t7 = m_op.compact(t7, Arrays.asList("?x3", "?x4", "?x1", "?x2"));

				Table<String> t8 = m_op.indexJoin(t7, "?x3", IndexDescription.PSOC, SO, names("?x3", "?x6"), DataField.SUBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#advisor");
				t8 = m_op.refineWithPrunedPart(p1, "?x6", t8);
				t8 = m_op.compact(t8, Arrays.asList("?x4", "?x1", "?x2"));

				}
				public void query_q6_spc_1() throws StorageException {
				PrunedQueryPart p1 = new PrunedQueryPart("p", null);
				p1.addEdge("?x9", "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#teacherOf", "?x6");
				PrunedQueryPart p2 = new PrunedQueryPart("p", null);
				p2.addEdge("?x8", "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#undergraduateDegreeFrom", "?x10");
				Table<String> t1 = m_op.load(IndexDescription.POCS, SO, names("?x1", "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#Publication"), DataField.PROPERTY, "http://www.w3.org/1999/02/22-rdf-syntax-ns#type", DataField.OBJECT, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#Publication");

				Table<String> t2 = m_op.load(IndexDescription.POCS, SO, names("?x8", "GraduateStudent75"), DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#name", DataField.OBJECT, "GraduateStudent75");
				t2 = m_op.refineWithPrunedPart(p2, "?x8", t2);

				Table<String> t3_a = m_op.load(IndexDescription.POCS, SO, names("?x1", "Publication1"), DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#name", DataField.OBJECT, "Publication1");
				Table<String> t3 = m_op.mergeJoin(t3_a, t1, "?x1");
				t3 = m_op.compact(t3, Arrays.asList("?x1"));

				Table<String> t4 = m_op.load(IndexDescription.POCS, SO, names("?x4", "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#AssociateProfessor"), DataField.PROPERTY, "http://www.w3.org/1999/02/22-rdf-syntax-ns#type", DataField.OBJECT, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#AssociateProfessor");

				Table<String> t5 = m_op.load(IndexDescription.POCS, SO, names("?x3", "FullProfessor3"), DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#name", DataField.OBJECT, "FullProfessor3");

				Table<String> t6_a = m_op.indexJoin(t3, "?x1", IndexDescription.PSOC, SO, names("?x1", "?x4"), DataField.SUBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#publicationAuthor");
				Table<String> t6 = m_op.mergeJoin(t6_a, t4, "?x4");

				Table<String> t7_a = m_op.indexJoin(t6, "?x1", IndexDescription.PSOC, SO, names("?x1", "?x8"), DataField.SUBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#publicationAuthor");
				Table<String> t7 = m_op.mergeJoin(t7_a, t2, "?x8");

				Table<String> t8 = m_op.indexJoin(t7, "?x4", IndexDescription.PSOC, SO, names("?x4", "?x2"), DataField.SUBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#undergraduateDegreeFrom");

				Table<String> t9_a = m_op.indexJoin(t5, "?x3", IndexDescription.PSOC, SO, names("?x3", "?x2"), DataField.SUBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#doctoralDegreeFrom");
				Table<String> t9 = m_op.mergeJoin(t9_a, t8, "?x2");
				t9 = m_op.compact(t9, Arrays.asList("?x3", "?x4", "?x1", "?x8"));

				Table<String> t10 = m_op.indexJoin(t9, "?x3", IndexDescription.POCS, SO, names("?x5", "?x3"), DataField.OBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#publicationAuthor");

				Table<String> t11 = m_op.indexJoin(t10, "?x5", IndexDescription.PSOC, SO, names("?x5", "?x7"), DataField.SUBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#publicationAuthor");
				t11 = m_op.compact(t11, Arrays.asList("?x3", "?x4", "?x1", "?x7", "?x8"));

				Table<String> t12 = m_op.indexJoin(t11, "?x7", IndexDescription.PSOC, SO, names("?x7", "?x6"), DataField.SUBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#takesCourse");
				t12 = m_op.refineWithPrunedPart(p1, "?x6", t12);
				t12 = m_op.compact(t12, Arrays.asList("?x3", "?x4", "?x1", "?x8"));

				}
				public void query_q7_spc_1() throws StorageException {
				PrunedQueryPart p1 = new PrunedQueryPart("p", null);
				p1.addEdge("?x8", "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#teacherOf", "?x5");
				PrunedQueryPart p2 = new PrunedQueryPart("p", null);
				p2.addEdge("?x7", "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#subOrganizationOf", "?x4");
				Table<String> t1 = m_op.load(IndexDescription.POCS, SO, names("?x1", "Research28"), DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#researchInterest", DataField.OBJECT, "Research28");

				Table<String> t2 = m_op.load(IndexDescription.POCS, SO, names("?x6", "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#Department"), DataField.PROPERTY, "http://www.w3.org/1999/02/22-rdf-syntax-ns#type", DataField.OBJECT, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#Department");

				Table<String> t3_a = m_op.indexJoin(t1, "?x1", IndexDescription.PSOC, SO, names("?x1", "?x6"), DataField.SUBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#worksFor");
				Table<String> t3 = m_op.mergeJoin(t3_a, t2, "?x6");

				Table<String> t4 = m_op.indexJoin(t3, "?x6", IndexDescription.PSOC, SO, names("?x6", "?x4"), DataField.SUBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#subOrganizationOf");
				t4 = m_op.refineWithPrunedPart(p2, "?x4", t4);
				t4 = m_op.compact(t4, Arrays.asList("?x1", "?x6"));

				Table<String> t5 = m_op.indexJoin(t4, "?x1", IndexDescription.POCS, SO, names("?x2", "?x1"), DataField.OBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#publicationAuthor");

				Table<String> t6 = m_op.indexJoin(t5, "?x2", IndexDescription.PSOC, SO, names("?x2", "?x3"), DataField.SUBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#publicationAuthor");
				t6 = m_op.compact(t6, Arrays.asList("?x3", "?x1", "?x6"));

				Table<String> t7 = m_op.indexJoin(t6, "?x3", IndexDescription.PSOC, SO, names("?x3", "?x5"), DataField.SUBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#takesCourse");
				t7 = m_op.refineWithPrunedPart(p1, "?x5", t7);
				t7 = m_op.compact(t7, Arrays.asList("?x1", "?x6"));

				}
				public void query_q8_spc_1() throws StorageException {
				PrunedQueryPart p1 = new PrunedQueryPart("p", null);
				p1.addEdge("?x5", "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#subOrganizationOf", "?x10");
				Table<String> t1 = m_op.load(IndexDescription.POCS, SO, names("?x1", "GraduateStudent98@Department3.University0.edu"), DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#emailAddress", DataField.OBJECT, "GraduateStudent98@Department3.University0.edu");

				Table<String> t2 = m_op.load(IndexDescription.POCS, SO, names("?x5", "Department3"), DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#name", DataField.OBJECT, "Department3");
				t2 = m_op.refineWithPrunedPart(p1, "?x5", t2);

				Table<String> t3 = m_op.load(IndexDescription.POCS, SO, names("?x3", "Publication14"), DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#name", DataField.OBJECT, "Publication14");

				Table<String> t4_a = m_op.indexJoin(t1, "?x1", IndexDescription.PSOC, SO, names("?x1", "?x5"), DataField.SUBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#memberOf");
				Table<String> t4 = m_op.mergeJoin(t4_a, t2, "?x5");

				Table<String> t5 = m_op.indexJoin(t3, "?x3", IndexDescription.PSOC, SO, names("?x3", "?x9"), DataField.SUBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#publicationAuthor");

				Table<String> t6 = m_op.indexJoin(t4, "?x5", IndexDescription.POCS, SO, names("?x7", "?x5"), DataField.OBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#memberOf");

				Table<String> t7_a = m_op.indexJoin(t5, "?x9", IndexDescription.PSOC, SO, names("?x9", "?x5"), DataField.SUBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#worksFor");
				Table<String> t7 = m_op.mergeJoin(t7_a, t6, "?x5");
				t7 = m_op.compact(t7, Arrays.asList("?x3", "?x1", "?x7", "?x5"));

				Table<String> t8 = m_op.indexJoin(t7, "?x1", IndexDescription.PSOC, SO, names("?x1", "?x4"), DataField.SUBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#advisor");

				Table<String> t9 = m_op.indexJoin(t8, "?x4", IndexDescription.POCS, SO, names("?x6", "?x4"), DataField.OBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#advisor");
				t9 = m_op.compact(t9, Arrays.asList("?x3", "?x1", "?x7", "?x5", "?x6"));

				Table<String> t10 = m_op.indexJoin(t9, "?x7", IndexDescription.PSOC, SO, names("?x7", "?x2"), DataField.SUBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#advisor");
				t10 = m_op.compact(t10, Arrays.asList("?x3", "?x1", "?x2", "?x5", "?x6"));

				Table<String> t11 = m_op.indexJoin(t10, "?x6", IndexDescription.POCS, SO, names("?x8", "?x6"), DataField.OBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#publicationAuthor");
				t11 = m_op.compact(t11, Arrays.asList("?x3", "?x1", "?x2", "?x8", "?x5"));

				Table<String> t12 = m_op.indexJoin(t11, "?x8", "?x2", IndexDescription.PSOC, SO, names("?x8", "?x2"), DataField.SUBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#publicationAuthor");
				t12 = m_op.compact(t12, Arrays.asList("?x3", "?x1", "?x5"));

				}
				public void query_q9_spc_1() throws StorageException {
				PrunedQueryPart p1 = new PrunedQueryPart("p", null);
				p1.addEdge("?x10", "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#undergraduateDegreeFrom", "?x13");
				Table<String> t1 = m_op.load(IndexDescription.POCS, SO, names("?x1", "GraduateStudent84@Department14.University0.edu"), DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#emailAddress", DataField.OBJECT, "GraduateStudent84@Department14.University0.edu");

				Table<String> t2 = m_op.load(IndexDescription.POCS, SO, names("?x11", "GraduateStudent31@Department3.University0.edu"), DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#emailAddress", DataField.OBJECT, "GraduateStudent31@Department3.University0.edu");

				Table<String> t3 = m_op.load(IndexDescription.POCS, SO, names("?x5", "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#Department"), DataField.PROPERTY, "http://www.w3.org/1999/02/22-rdf-syntax-ns#type", DataField.OBJECT, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#Department");

				Table<String> t4 = m_op.load(IndexDescription.POCS, SO, names("?x9", "Publication13"), DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#name", DataField.OBJECT, "Publication13");

				Table<String> t5_a = m_op.indexJoin(t4, "?x9", IndexDescription.PSOC, SO, names("?x9", "?x11"), DataField.SUBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#publicationAuthor");
				Table<String> t5 = m_op.mergeJoin(t5_a, t2, "?x11");

				Table<String> t6_a = m_op.indexJoin(t1, "?x1", IndexDescription.PSOC, SO, names("?x1", "?x5"), DataField.SUBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#memberOf");
				Table<String> t6 = m_op.mergeJoin(t6_a, t3, "?x5");

				Table<String> t7 = m_op.indexJoin(t5, "?x11", IndexDescription.PSOC, SO, names("?x11", "?x12"), DataField.SUBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#undergraduateDegreeFrom");

				Table<String> t8 = m_op.indexJoin(t6, "?x5", IndexDescription.PSOC, SO, names("?x5", "?x2"), DataField.SUBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#subOrganizationOf");

				Table<String> t9_a = m_op.indexJoin(t8, "?x1", IndexDescription.PSOC, SO, names("?x1", "?x12"), DataField.SUBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#undergraduateDegreeFrom");
				Table<String> t9 = m_op.mergeJoin(t9_a, t7, "?x12");
				t9 = m_op.compact(t9, Arrays.asList("?x1", "?x2", "?x5", "?x11", "?x9"));

				Table<String> t10 = m_op.indexJoin(t9, "?x2", IndexDescription.POCS, SO, names("?x7", "?x2"), DataField.OBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#mastersDegreeFrom");
				t10 = m_op.compact(t10, Arrays.asList("?x1", "?x7", "?x5", "?x11", "?x9"));

				Table<String> t11 = m_op.indexJoin(t10, "?x7", IndexDescription.POCS, SO, names("?x8", "?x7"), DataField.OBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#publicationAuthor");
				t11 = m_op.compact(t11, Arrays.asList("?x1", "?x8", "?x5", "?x11", "?x9"));

				Table<String> t12 = m_op.indexJoin(t11, "?x8", IndexDescription.PSOC, SO, names("?x8", "?x6"), DataField.SUBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#publicationAuthor");
				t12 = m_op.compact(t12, Arrays.asList("?x1", "?x5", "?x6", "?x11", "?x9"));

				Table<String> t13 = m_op.indexJoin(t12, "?x6", IndexDescription.PSOC, SO, names("?x6", "?x3"), DataField.SUBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#advisor");
				t13 = m_op.compact(t13, Arrays.asList("?x3", "?x1", "?x5", "?x11", "?x9"));

				Table<String> t14 = m_op.indexJoin(t13, "?x3", IndexDescription.PSOC, SO, names("?x3", "?x4"), DataField.SUBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#teacherOf");
				t14 = m_op.compact(t14, Arrays.asList("?x4", "?x1", "?x5", "?x11", "?x9"));

				Table<String> t15 = m_op.indexJoin(t14, "?x4", IndexDescription.POCS, SO, names("?x10", "?x4"), DataField.OBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#takesCourse");
				t15 = m_op.refineWithPrunedPart(p1, "?x10", t15);
				t15 = m_op.compact(t15, Arrays.asList("?x1", "?x5", "?x11", "?x9"));

				}
				public void query_q10_spc_1() throws StorageException {
				PrunedQueryPart p1 = new PrunedQueryPart("p", null);
				p1.addEdge("?x12", "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#publicationAuthor", "?x3");
				Table<String> t1 = m_op.load(IndexDescription.POCS, SO, names("?x1", "UndergraduateStudent180"), DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#name", DataField.OBJECT, "UndergraduateStudent180");

				Table<String> t2 = m_op.load(IndexDescription.POCS, SO, names("?x10", "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#Department"), DataField.PROPERTY, "http://www.w3.org/1999/02/22-rdf-syntax-ns#type", DataField.OBJECT, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#Department");

				Table<String> t3 = m_op.load(IndexDescription.POCS, SO, names("?x5", "AssociateProfessor6"), DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#name", DataField.OBJECT, "AssociateProfessor6");

				Table<String> t4_a = m_op.load(IndexDescription.POCS, SO, names("?x10", "Department9"), DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#name", DataField.OBJECT, "Department9");
				Table<String> t4 = m_op.mergeJoin(t4_a, t2, "?x10");
				t4 = m_op.compact(t4, Arrays.asList("?x10"));

				Table<String> t5_a = m_op.indexJoin(t3, "?x5", IndexDescription.PSOC, SO, names("?x5", "?x10"), DataField.SUBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#worksFor");
				Table<String> t5 = m_op.mergeJoin(t5_a, t4, "?x10");

				Table<String> t6_a = m_op.indexJoin(t1, "?x1", IndexDescription.PSOC, SO, names("?x1", "?x10"), DataField.SUBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#memberOf");
				Table<String> t6 = m_op.mergeJoin(t6_a, t5, "?x10");

				Table<String> t7 = m_op.indexJoin(t6, "?x5", IndexDescription.POCS, SO, names("?x2", "?x5"), DataField.OBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#publicationAuthor");

				Table<String> t8 = m_op.indexJoin(t7, "?x2", IndexDescription.PSOC, SO, names("?x2", "?x7"), DataField.SUBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#publicationAuthor");
				t8 = m_op.compact(t8, Arrays.asList("?x1", "?x7", "?x5", "?x10"));

				Table<String> t9 = m_op.indexJoin(t8, "?x7", IndexDescription.PSOC, SO, names("?x7", "?x11"), DataField.SUBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#takesCourse");
				t9 = m_op.compact(t9, Arrays.asList("?x1", "?x5", "?x10", "?x11"));

				Table<String> t10 = m_op.indexJoin(t9, "?x11", IndexDescription.POCS, SO, names("?x4", "?x11"), DataField.OBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#takesCourse");
				t10 = m_op.compact(t10, Arrays.asList("?x4", "?x1", "?x5", "?x10"));

				Table<String> t11 = m_op.indexJoin(t10, "?x4", IndexDescription.PSOC, SO, names("?x4", "?x9"), DataField.SUBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#takesCourse");
				t11 = m_op.compact(t11, Arrays.asList("?x1", "?x5", "?x10", "?x9"));

				Table<String> t12 = m_op.indexJoin(t11, "?x9", IndexDescription.POCS, SO, names("?x8", "?x9"), DataField.OBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#teacherOf");
				t12 = m_op.compact(t12, Arrays.asList("?x1", "?x8", "?x5", "?x10"));

				Table<String> t13 = m_op.indexJoin(t12, "?x8", IndexDescription.PSOC, SO, names("?x8", "?x6"), DataField.SUBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#doctoralDegreeFrom");
				t13 = m_op.compact(t13, Arrays.asList("?x1", "?x5", "?x6", "?x10"));

				Table<String> t14 = m_op.indexJoin(t13, "?x6", IndexDescription.POCS, SO, names("?x3", "?x6"), DataField.OBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#undergraduateDegreeFrom");
				t14 = m_op.refineWithPrunedPart(p1, "?x3", t14);
				t14 = m_op.compact(t14, Arrays.asList("?x1", "?x5", "?x10"));

				}
				}
