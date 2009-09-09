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

public class StarQuery {
	private IndexReader m_idxReader;
	private DataIndex m_dataIndex;
	private StructureIndex m_structureIndex;
	private Map<IndexDescription,IndexStorage> m_indexes;
	private QueryOperators m_op;

	public StarQuery(IndexReader reader) throws IOException, StorageException {
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
	public void query_q1_vp() throws StorageException {
		Table<String> t1 = m_op.load(IndexDescription.POCS, SO, names("?x1", "Research23"), DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#researchInterest", DataField.OBJECT, "Research23");

		Table<String> t2 = m_op.load(IndexDescription.POCS, SO, names("?x7", "GraduateStudent82"), DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#name", DataField.OBJECT, "GraduateStudent82");

		Table<String> t3 = m_op.load(IndexDescription.POCS, SO, names("?x8", "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#GraduateStudent"), DataField.PROPERTY, "http://www.w3.org/1999/02/22-rdf-syntax-ns#type", DataField.OBJECT, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#GraduateStudent");

		Table<String> t4_a = m_op.load(IndexDescription.POCS, SO, names("?x8", "GraduateStudent65"), DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#name", DataField.OBJECT, "GraduateStudent65");
		Table<String> t4 = m_op.mergeJoin(t4_a, t3, "?x8");
		t4 = m_op.compact(t4, Arrays.asList("?x8"));

		Table<String> t5 = m_op.load(IndexDescription.POCS, SO, names("?x2", "FullProfessor0"), DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#name", DataField.OBJECT, "FullProfessor0");

		Table<String> t6 = m_op.load(IndexDescription.POCS, SO, names("?x4", "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#Course"), DataField.PROPERTY, "http://www.w3.org/1999/02/22-rdf-syntax-ns#type", DataField.OBJECT, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#Course");

		Table<String> t7_a = m_op.indexJoin(t4, "?x8", IndexDescription.PSOC, SO, names("?x8", "?x1"), DataField.SUBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#advisor");
		Table<String> t7 = m_op.mergeJoin(t7_a, t1, "?x1");

		Table<String> t8_a = m_op.indexJoin(t2, "?x7", IndexDescription.PSOC, SO, names("?x7", "?x2"), DataField.SUBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#advisor");
		Table<String> t8 = m_op.mergeJoin(t8_a, t5, "?x2");

		Table<String> t9_a = m_op.indexJoin(t7, "?x1", IndexDescription.PSOC, SO, names("?x1", "?x4"), DataField.SUBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#teacherOf");
		Table<String> t9 = m_op.mergeJoin(t9_a, t6, "?x4");

		Table<String> t10 = m_op.indexJoin(t9, "?x1", IndexDescription.PSOC, SO, names("?x1", "?x6"), DataField.SUBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#worksFor");

		Table<String> t11 = m_op.indexJoin(t10, "?x1", IndexDescription.PSOC, SO, names("?x1", "?x3"), DataField.SUBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#undergraduateDegreeFrom");
		t11 = m_op.compact(t11, Arrays.asList("?x4", "?x1", "?x8", "?x6"));

		Table<String> t12_a = m_op.indexJoin(t8, "?x2", IndexDescription.PSOC, SO, names("?x2", "?x6"), DataField.SUBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#headOf");
		Table<String> t12 = m_op.mergeJoin(t12_a, t11, "?x6");
		t12 = m_op.compact(t12, Arrays.asList("?x4", "?x1", "?x2", "?x7", "?x8"));

		Table<String> t13 = m_op.indexJoin(t12, "?x1", IndexDescription.POCS, SO, names("?x5", "?x1"), DataField.OBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#publicationAuthor");
		t13 = m_op.compact(t13, Arrays.asList("?x4", "?x1", "?x2", "?x7", "?x8"));

		}
		public void query_q2_vp() throws StorageException {
		Table<String> t1 = m_op.load(IndexDescription.POCS, SO, names("?x1", "Lecturer3@Department5.University0.edu"), DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#emailAddress", DataField.OBJECT, "Lecturer3@Department5.University0.edu");

		Table<String> t2 = m_op.load(IndexDescription.POCS, SO, names("?x4", "Publication2"), DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#name", DataField.OBJECT, "Publication2");

		Table<String> t3 = m_op.load(IndexDescription.POCS, SO, names("?x2", "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#GraduateCourse"), DataField.PROPERTY, "http://www.w3.org/1999/02/22-rdf-syntax-ns#type", DataField.OBJECT, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#GraduateCourse");

		Table<String> t4 = m_op.load(IndexDescription.POCS, SO, names("?x5", "Publication1"), DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#name", DataField.OBJECT, "Publication1");

		Table<String> t5_a = m_op.indexJoin(t4, "?x5", IndexDescription.PSOC, SO, names("?x5", "?x1"), DataField.SUBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#publicationAuthor");
		Table<String> t5 = m_op.mergeJoin(t5_a, t1, "?x1");

		Table<String> t6_a = m_op.indexJoin(t5, "?x1", IndexDescription.PSOC, SO, names("?x1", "?x2"), DataField.SUBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#teacherOf");
		Table<String> t6 = m_op.mergeJoin(t6_a, t3, "?x2");

		Table<String> t7_a = m_op.indexJoin(t2, "?x4", IndexDescription.PSOC, SO, names("?x4", "?x1"), DataField.SUBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#publicationAuthor");
		Table<String> t7 = m_op.mergeJoin(t7_a, t6, "?x1");

		Table<String> t8 = m_op.indexJoin(t7, "?x1", IndexDescription.PSOC, SO, names("?x1", "?x3"), DataField.SUBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#doctoralDegreeFrom");
		t8 = m_op.compact(t8, Arrays.asList("?x4", "?x1", "?x2", "?x5"));

		}
		public void query_q3_vp() throws StorageException {
		Table<String> t1 = m_op.load(IndexDescription.POCS, SO, names("?x1", "GraduateCourse28"), DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#name", DataField.OBJECT, "GraduateCourse28");

		Table<String> t2 = m_op.load(IndexDescription.POCS, SO, names("?x6", "Research8"), DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#researchInterest", DataField.OBJECT, "Research8");

		Table<String> t3 = m_op.load(IndexDescription.POCS, SO, names("?x4", "AssistantProfessor8@Department8.University0.edu"), DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#emailAddress", DataField.OBJECT, "AssistantProfessor8@Department8.University0.edu");

		Table<String> t4 = m_op.load(IndexDescription.POCS, SO, names("?x5", "Publication4"), DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#name", DataField.OBJECT, "Publication4");

		Table<String> t5_a = m_op.indexJoin(t4, "?x5", IndexDescription.PSOC, SO, names("?x5", "?x4"), DataField.SUBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#publicationAuthor");
		Table<String> t5 = m_op.mergeJoin(t5_a, t3, "?x4");

		Table<String> t6_a = m_op.indexJoin(t2, "?x6", IndexDescription.PSOC, SO, names("?x6", "?x1"), DataField.SUBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#teacherOf");
		Table<String> t6 = m_op.mergeJoin(t6_a, t1, "?x1");

		Table<String> t7 = m_op.indexJoin(t5, "?x5", IndexDescription.PSOC, SO, names("?x5", "?x3"), DataField.SUBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#publicationAuthor");

		Table<String> t8 = m_op.indexJoin(t7, "?x4", IndexDescription.POCS, SO, names("?x2", "?x4"), DataField.OBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#advisor");

		Table<String> t9_a = m_op.indexJoin(t8, "?x2", IndexDescription.PSOC, SO, names("?x2", "?x1"), DataField.SUBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#takesCourse");
		Table<String> t9 = m_op.mergeJoin(t9_a, t6, "?x1");
		t9 = m_op.compact(t9, Arrays.asList("?x3", "?x4", "?x1", "?x5", "?x6"));

		Table<String> t10 = m_op.indexJoin(t9, "?x3", IndexDescription.POCS, SO, names("?x7", "?x3"), DataField.OBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#publicationAuthor");
		t10 = m_op.compact(t10, Arrays.asList("?x4", "?x1", "?x5", "?x6"));

		}
		public void query_q4_vp() throws StorageException {
		Table<String> t1 = m_op.load(IndexDescription.POCS, SO, names("?x1", "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#AssociateProfessor"), DataField.PROPERTY, "http://www.w3.org/1999/02/22-rdf-syntax-ns#type", DataField.OBJECT, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#AssociateProfessor");

		Table<String> t2_a = m_op.load(IndexDescription.POCS, SO, names("?x1", "AssociateProfessor0@Department2.University0.edu"), DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#emailAddress", DataField.OBJECT, "AssociateProfessor0@Department2.University0.edu");
		Table<String> t2 = m_op.mergeJoin(t2_a, t1, "?x1");
		t2 = m_op.compact(t2, Arrays.asList("?x1"));

		Table<String> t3 = m_op.load(IndexDescription.POCS, SO, names("?x6", "Publication8"), DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#name", DataField.OBJECT, "Publication8");

		Table<String> t4 = m_op.load(IndexDescription.POCS, SO, names("?x2", "Publication6"), DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#name", DataField.OBJECT, "Publication6");

		Table<String> t5_a = m_op.indexJoin(t4, "?x2", IndexDescription.PSOC, SO, names("?x2", "?x1"), DataField.SUBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#publicationAuthor");
		Table<String> t5 = m_op.mergeJoin(t5_a, t2, "?x1");

		Table<String> t6_a = m_op.indexJoin(t3, "?x6", IndexDescription.PSOC, SO, names("?x6", "?x1"), DataField.SUBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#publicationAuthor");
		Table<String> t6 = m_op.mergeJoin(t6_a, t5, "?x1");

		Table<String> t7 = m_op.indexJoin(t6, "?x1", IndexDescription.POCS, SO, names("?x5", "?x1"), DataField.OBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#advisor");
		t7 = m_op.compact(t7, Arrays.asList("?x1", "?x2", "?x6"));

		Table<String> t8 = m_op.indexJoin(t7, "?x1", IndexDescription.PSOC, SO, names("?x1", "?x3"), DataField.SUBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#mastersDegreeFrom");
		t8 = m_op.compact(t8, Arrays.asList("?x1", "?x2", "?x6"));

		Table<String> t9 = m_op.indexJoin(t8, "?x2", IndexDescription.PSOC, SO, names("?x2", "?x4"), DataField.SUBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#publicationAuthor");
		t9 = m_op.compact(t9, Arrays.asList("?x1", "?x2", "?x6"));

		}
		public void query_q5_vp() throws StorageException {
		Table<String> t1 = m_op.load(IndexDescription.POCS, SO, names("?x1", "GraduateStudent15"), DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#name", DataField.OBJECT, "GraduateStudent15");

		Table<String> t2 = m_op.load(IndexDescription.POCS, SO, names("?x6", "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#Publication"), DataField.PROPERTY, "http://www.w3.org/1999/02/22-rdf-syntax-ns#type", DataField.OBJECT, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#Publication");

		Table<String> t3 = m_op.load(IndexDescription.POCS, SO, names("?x4", "Department3"), DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#name", DataField.OBJECT, "Department3");

		Table<String> t4_a = m_op.load(IndexDescription.POCS, SO, names("?x4", "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#Department"), DataField.PROPERTY, "http://www.w3.org/1999/02/22-rdf-syntax-ns#type", DataField.OBJECT, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#Department");
		Table<String> t4 = m_op.mergeJoin(t4_a, t3, "?x4");
		t4 = m_op.compact(t4, Arrays.asList("?x4"));

		Table<String> t5 = m_op.load(IndexDescription.POCS, SO, names("?x7", "AssociateProfessor9"), DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#name", DataField.OBJECT, "AssociateProfessor9");

		Table<String> t6 = m_op.load(IndexDescription.POCS, SO, names("?x3", "Publication15"), DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#name", DataField.OBJECT, "Publication15");

		Table<String> t7 = m_op.load(IndexDescription.POCS, SO, names("?x5", "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#GraduateStudent"), DataField.PROPERTY, "http://www.w3.org/1999/02/22-rdf-syntax-ns#type", DataField.OBJECT, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#GraduateStudent");

		Table<String> t8 = m_op.load(IndexDescription.POCS, SO, names("?x9", "AssistantProfessor4"), DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#name", DataField.OBJECT, "AssistantProfessor4");

		Table<String> t9_a = m_op.indexJoin(t6, "?x3", IndexDescription.PSOC, SO, names("?x3", "?x1"), DataField.SUBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#publicationAuthor");
		Table<String> t9 = m_op.mergeJoin(t9_a, t1, "?x1");

		Table<String> t10_a = m_op.indexJoin(t7, "?x5", IndexDescription.PSOC, SO, names("?x5", "?x4"), DataField.SUBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#memberOf");
		Table<String> t10 = m_op.mergeJoin(t10_a, t4, "?x4");

		Table<String> t11_a = m_op.indexJoin(t5, "?x7", IndexDescription.PSOC, SO, names("?x7", "?x4"), DataField.SUBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#worksFor");
		Table<String> t11 = m_op.mergeJoin(t11_a, t10, "?x4");

		Table<String> t12_a = m_op.indexJoin(t9, "?x1", IndexDescription.PSOC, SO, names("?x1", "?x4"), DataField.SUBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#memberOf");
		Table<String> t12 = m_op.mergeJoin(t12_a, t11, "?x4");

		Table<String> t13_a = m_op.indexJoin(t2, "?x6", IndexDescription.PSOC, SO, names("?x6", "?x5"), DataField.SUBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#publicationAuthor");
		Table<String> t13 = m_op.mergeJoin(t13_a, t12, "?x5");

		Table<String> t14 = m_op.indexJoin(t13, "?x6", "?x1", IndexDescription.PSOC, SO, names("?x6", "?x1"), DataField.SUBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#publicationAuthor");

		Table<String> t15 = m_op.indexJoin(t8, "?x9", IndexDescription.PSOC, SO, names("?x9", "?x8"), DataField.SUBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#teacherOf");

		Table<String> t16 = m_op.indexJoin(t14, "?x4", IndexDescription.POCS, SO, names("?x12", "?x4"), DataField.OBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#subOrganizationOf");
		t16 = m_op.compact(t16, Arrays.asList("?x3", "?x4", "?x1", "?x7", "?x5", "?x6"));

		Table<String> t17 = m_op.indexJoin(t16, "?x7", IndexDescription.PSOC, SO, names("?x7", "?x2"), DataField.SUBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#undergraduateDegreeFrom");

		Table<String> t18_a = m_op.indexJoin(t17, "?x1", IndexDescription.PSOC, SO, names("?x1", "?x8"), DataField.SUBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#takesCourse");
		Table<String> t18 = m_op.mergeJoin(t18_a, t15, "?x8");
		t18 = m_op.compact(t18, Arrays.asList("?x3", "?x4", "?x1", "?x2", "?x7", "?x5", "?x6", "?x9"));

		Table<String> t19 = m_op.indexJoin(t18, "?x9", IndexDescription.POCS, SO, names("?x10", "?x9"), DataField.OBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#publicationAuthor");
		t19 = m_op.compact(t19, Arrays.asList("?x3", "?x4", "?x1", "?x2", "?x7", "?x5", "?x6", "?x9"));

		Table<String> t20 = m_op.indexJoin(t19, "?x2", IndexDescription.POCS, SO, names("?x11", "?x2"), DataField.OBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#undergraduateDegreeFrom");
		t20 = m_op.compact(t20, Arrays.asList("?x3", "?x4", "?x1", "?x7", "?x5", "?x6", "?x9"));

		}
		public void query_q6_vp() throws StorageException {
		Table<String> t1 = m_op.load(IndexDescription.POCS, SO, names("?x1", "GraduateStudent118@Department8.University0.edu"), DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#emailAddress", DataField.OBJECT, "GraduateStudent118@Department8.University0.edu");

		Table<String> t2 = m_op.load(IndexDescription.POCS, SO, names("?x6", "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#GraduateCourse"), DataField.PROPERTY, "http://www.w3.org/1999/02/22-rdf-syntax-ns#type", DataField.OBJECT, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#GraduateCourse");

		Table<String> t3_a = m_op.load(IndexDescription.POCS, SO, names("?x6", "GraduateCourse5"), DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#name", DataField.OBJECT, "GraduateCourse5");
		Table<String> t3 = m_op.mergeJoin(t3_a, t2, "?x6");
		t3 = m_op.compact(t3, Arrays.asList("?x6"));

		Table<String> t4_a = m_op.indexJoin(t1, "?x1", IndexDescription.PSOC, SO, names("?x1", "?x6"), DataField.SUBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#takesCourse");
		Table<String> t4 = m_op.mergeJoin(t4_a, t3, "?x6");

		Table<String> t5 = m_op.indexJoin(t4, "?x1", IndexDescription.PSOC, SO, names("?x1", "?x7"), DataField.SUBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#takesCourse");
		t5 = m_op.compact(t5, Arrays.asList("?x1", "?x6"));

		Table<String> t6 = m_op.indexJoin(t5, "?x1", IndexDescription.POCS, SO, names("?x4", "?x1"), DataField.OBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#publicationAuthor");
		t6 = m_op.compact(t6, Arrays.asList("?x1", "?x6"));

		Table<String> t7 = m_op.indexJoin(t6, "?x1", IndexDescription.PSOC, SO, names("?x1", "?x3"), DataField.SUBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#undergraduateDegreeFrom");
		t7 = m_op.compact(t7, Arrays.asList("?x1", "?x6"));

		Table<String> t8 = m_op.indexJoin(t7, "?x1", IndexDescription.PSOC, SO, names("?x1", "?x2"), DataField.SUBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#memberOf");
		t8 = m_op.compact(t8, Arrays.asList("?x1", "?x6"));

		Table<String> t9 = m_op.indexJoin(t8, "?x1", IndexDescription.PSOC, SO, names("?x1", "?x5"), DataField.SUBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#advisor");
		t9 = m_op.compact(t9, Arrays.asList("?x1", "?x6"));

		}
		public void query_q7_vp() throws StorageException {
		Table<String> t1 = m_op.load(IndexDescription.POCS, SO, names("?x1", "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#GraduateStudent"), DataField.PROPERTY, "http://www.w3.org/1999/02/22-rdf-syntax-ns#type", DataField.OBJECT, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#GraduateStudent");

		Table<String> t2_a = m_op.load(IndexDescription.POCS, SO, names("?x1", "GraduateStudent42@Department8.University0.edu"), DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#emailAddress", DataField.OBJECT, "GraduateStudent42@Department8.University0.edu");
		Table<String> t2 = m_op.mergeJoin(t2_a, t1, "?x1");
		t2 = m_op.compact(t2, Arrays.asList("?x1"));

		Table<String> t3 = m_op.load(IndexDescription.POCS, SO, names("?x4", "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#Course"), DataField.PROPERTY, "http://www.w3.org/1999/02/22-rdf-syntax-ns#type", DataField.OBJECT, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#Course");

		Table<String> t4_a = m_op.load(IndexDescription.POCS, SO, names("?x4", "Course42"), DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#name", DataField.OBJECT, "Course42");
		Table<String> t4 = m_op.mergeJoin(t4_a, t3, "?x4");
		t4 = m_op.compact(t4, Arrays.asList("?x4"));

		Table<String> t5_a = m_op.indexJoin(t2, "?x1", IndexDescription.PSOC, SO, names("?x1", "?x4"), DataField.SUBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#teachingAssistantOf");
		Table<String> t5 = m_op.mergeJoin(t5_a, t4, "?x4");

		Table<String> t6 = m_op.indexJoin(t5, "?x1", IndexDescription.PSOC, SO, names("?x1", "?x5"), DataField.SUBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#advisor");
		t6 = m_op.compact(t6, Arrays.asList("?x4", "?x1"));

		Table<String> t7 = m_op.indexJoin(t6, "?x1", IndexDescription.POCS, SO, names("?x2", "?x1"), DataField.OBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#publicationAuthor");

		Table<String> t8 = m_op.indexJoin(t7, "?x1", IndexDescription.POCS, SO, names("?x7", "?x1"), DataField.OBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#publicationAuthor");
		t8 = m_op.compact(t8, Arrays.asList("?x4", "?x1", "?x2"));

		Table<String> t9 = m_op.indexJoin(t8, "?x1", IndexDescription.PSOC, SO, names("?x1", "?x3"), DataField.SUBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#memberOf");
		t9 = m_op.compact(t9, Arrays.asList("?x4", "?x1", "?x2"));

		Table<String> t10 = m_op.indexJoin(t9, "?x1", IndexDescription.POCS, SO, names("?x6", "?x1"), DataField.OBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#publicationAuthor");
		t10 = m_op.compact(t10, Arrays.asList("?x4", "?x1", "?x2"));

		Table<String> t11 = m_op.indexJoin(t10, "?x2", IndexDescription.PSOC, SO, names("?x2", "?x8"), DataField.SUBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#publicationAuthor");
		t11 = m_op.compact(t11, Arrays.asList("?x4", "?x1"));

		}
		public void query_q8_vp() throws StorageException {
		Table<String> t1 = m_op.load(IndexDescription.POCS, SO, names("?x1", "GraduateStudent15@Department6.University0.edu"), DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#emailAddress", DataField.OBJECT, "GraduateStudent15@Department6.University0.edu");

		Table<String> t2 = m_op.load(IndexDescription.POCS, SO, names("?x2", "Publication11"), DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#name", DataField.OBJECT, "Publication11");

		Table<String> t3 = m_op.load(IndexDescription.POCS, SO, names("?x4", "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#FullProfessor"), DataField.PROPERTY, "http://www.w3.org/1999/02/22-rdf-syntax-ns#type", DataField.OBJECT, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#FullProfessor");

		Table<String> t4_a = m_op.indexJoin(t2, "?x2", IndexDescription.PSOC, SO, names("?x2", "?x1"), DataField.SUBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#publicationAuthor");
		Table<String> t4 = m_op.mergeJoin(t4_a, t1, "?x1");

		Table<String> t5_a = m_op.indexJoin(t4, "?x1", IndexDescription.PSOC, SO, names("?x1", "?x4"), DataField.SUBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#advisor");
		Table<String> t5 = m_op.mergeJoin(t5_a, t3, "?x4");

		Table<String> t6 = m_op.indexJoin(t5, "?x4", IndexDescription.PSOC, SO, names("?x4", "?x5"), DataField.SUBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#teacherOf");
		t6 = m_op.compact(t6, Arrays.asList("?x4", "?x1", "?x2"));

		Table<String> t7 = m_op.indexJoin(t6, "?x2", IndexDescription.PSOC, SO, names("?x2", "?x6"), DataField.SUBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#publicationAuthor");
		t7 = m_op.compact(t7, Arrays.asList("?x4", "?x1", "?x2"));

		Table<String> t8 = m_op.indexJoin(t7, "?x1", IndexDescription.PSOC, SO, names("?x1", "?x3"), DataField.SUBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#memberOf");

		Table<String> t9 = m_op.indexJoin(t8, "?x1", IndexDescription.PSOC, SO, names("?x1", "?x8"), DataField.SUBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#takesCourse");
		t9 = m_op.compact(t9, Arrays.asList("?x3", "?x4", "?x1", "?x2"));

		Table<String> t10 = m_op.indexJoin(t9, "?x1", IndexDescription.POCS, SO, names("?x7", "?x1"), DataField.OBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#publicationAuthor");
		t10 = m_op.compact(t10, Arrays.asList("?x3", "?x4", "?x1", "?x2"));

		Table<String> t11 = m_op.indexJoin(t10, "?x1", IndexDescription.PSOC, SO, names("?x1", "?x9"), DataField.SUBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#takesCourse");
		t11 = m_op.compact(t11, Arrays.asList("?x3", "?x4", "?x1", "?x2"));

		Table<String> t12 = m_op.indexJoin(t11, "?x3", IndexDescription.POCS, SO, names("?x10", "?x3"), DataField.OBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#headOf");
		t12 = m_op.compact(t12, Arrays.asList("?x4", "?x1", "?x2"));

		}
		public void query_q9_vp() throws StorageException {
		Table<String> t1 = m_op.load(IndexDescription.POCS, SO, names("?x1", "Lecturer5"), DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#name", DataField.OBJECT, "Lecturer5");

		Table<String> t2 = m_op.load(IndexDescription.POCS, SO, names("?x3", "GraduateCourse47"), DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#name", DataField.OBJECT, "GraduateCourse47");

		Table<String> t3 = m_op.load(IndexDescription.POCS, SO, names("?x10", "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#Publication"), DataField.PROPERTY, "http://www.w3.org/1999/02/22-rdf-syntax-ns#type", DataField.OBJECT, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#Publication");

		Table<String> t4 = m_op.load(IndexDescription.POCS, SO, names("?x7", "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#University"), DataField.PROPERTY, "http://www.w3.org/1999/02/22-rdf-syntax-ns#type", DataField.OBJECT, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#University");

		Table<String> t5 = m_op.load(IndexDescription.POCS, SO, names("?x2", "GraduateStudent29"), DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#name", DataField.OBJECT, "GraduateStudent29");

		Table<String> t6 = m_op.load(IndexDescription.POCS, SO, names("?x12", "AssistantProfessor4"), DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#name", DataField.OBJECT, "AssistantProfessor4");

		Table<String> t7_a = m_op.indexJoin(t1, "?x1", IndexDescription.PSOC, SO, names("?x1", "?x7"), DataField.SUBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#doctoralDegreeFrom");
		Table<String> t7 = m_op.mergeJoin(t7_a, t4, "?x7");

		Table<String> t8_a = m_op.indexJoin(t3, "?x10", IndexDescription.PSOC, SO, names("?x10", "?x2"), DataField.SUBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#publicationAuthor");
		Table<String> t8 = m_op.mergeJoin(t8_a, t5, "?x2");

		Table<String> t9_a = m_op.indexJoin(t7, "?x1", IndexDescription.PSOC, SO, names("?x1", "?x3"), DataField.SUBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#teacherOf");
		Table<String> t9 = m_op.mergeJoin(t9_a, t2, "?x3");

		Table<String> t10_a = m_op.indexJoin(t8, "?x2", IndexDescription.PSOC, SO, names("?x2", "?x7"), DataField.SUBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#undergraduateDegreeFrom");
		Table<String> t10 = m_op.mergeJoin(t10_a, t9, "?x7");

		Table<String> t11 = m_op.indexJoin(t10, "?x3", IndexDescription.POCS, SO, names("?x6", "?x3"), DataField.OBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#takesCourse");

		Table<String> t12 = m_op.indexJoin(t6, "?x12", IndexDescription.POCS, SO, names("?x5", "?x12"), DataField.OBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#advisor");

		Table<String> t13 = m_op.indexJoin(t11, "?x1", IndexDescription.PSOC, SO, names("?x1", "?x9"), DataField.SUBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#mastersDegreeFrom");

		Table<String> t14_a = m_op.indexJoin(t12, "?x12", IndexDescription.PSOC, SO, names("?x12", "?x9"), DataField.SUBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#mastersDegreeFrom");
		Table<String> t14 = m_op.mergeJoin(t14_a, t13, "?x9");
		t14 = m_op.compact(t14, Arrays.asList("?x12", "?x10", "?x3", "?x1", "?x2", "?x7", "?x5", "?x6"));

		Table<String> t15 = m_op.indexJoin(t14, "?x6", IndexDescription.POCS, SO, names("?x8", "?x6"), DataField.OBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#publicationAuthor");
		t15 = m_op.compact(t15, Arrays.asList("?x12", "?x10", "?x3", "?x1", "?x2", "?x7", "?x8", "?x5"));

		Table<String> t16 = m_op.indexJoin(t15, "?x5", IndexDescription.POCS, SO, names("?x15", "?x5"), DataField.OBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#publicationAuthor");
		t16 = m_op.compact(t16, Arrays.asList("?x3", "?x1", "?x2", "?x7", "?x8", "?x12", "?x10"));

		Table<String> t17 = m_op.indexJoin(t16, "?x8", IndexDescription.PSOC, SO, names("?x8", "?x4"), DataField.SUBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#publicationAuthor");
		t17 = m_op.compact(t17, Arrays.asList("?x3", "?x4", "?x1", "?x2", "?x7", "?x12", "?x10"));

		Table<String> t18 = m_op.indexJoin(t17, "?x4", IndexDescription.PSOC, SO, names("?x4", "?x13"), DataField.SUBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#worksFor");
		t18 = m_op.compact(t18, Arrays.asList("?x3", "?x1", "?x2", "?x7", "?x12", "?x13", "?x10"));

		Table<String> t19 = m_op.indexJoin(t18, "?x13", IndexDescription.POCS, SO, names("?x11", "?x13"), DataField.OBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#worksFor");
		t19 = m_op.compact(t19, Arrays.asList("?x3", "?x1", "?x2", "?x7", "?x12", "?x10", "?x11"));

		Table<String> t20 = m_op.indexJoin(t19, "?x11", IndexDescription.POCS, SO, names("?x14", "?x11"), DataField.OBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#publicationAuthor");
		t20 = m_op.compact(t20, Arrays.asList("?x3", "?x1", "?x2", "?x7", "?x12", "?x10"));

		}
		public void query_q10_vp() throws StorageException {
		Table<String> t1 = m_op.load(IndexDescription.POCS, SO, names("?x1", "GraduateStudent11@Department7.University0.edu"), DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#emailAddress", DataField.OBJECT, "GraduateStudent11@Department7.University0.edu");

		Table<String> t2 = m_op.load(IndexDescription.POCS, SO, names("?x7", "AssociateProfessor6"), DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#name", DataField.OBJECT, "AssociateProfessor6");

		Table<String> t3 = m_op.load(IndexDescription.POCS, SO, names("?x13", "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#AssistantProfessor"), DataField.PROPERTY, "http://www.w3.org/1999/02/22-rdf-syntax-ns#type", DataField.OBJECT, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#AssistantProfessor");

		Table<String> t4 = m_op.load(IndexDescription.POCS, SO, names("?x5", "Department7"), DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#name", DataField.OBJECT, "Department7");

		Table<String> t5_a = m_op.indexJoin(t1, "?x1", IndexDescription.PSOC, SO, names("?x1", "?x5"), DataField.SUBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#memberOf");
		Table<String> t5 = m_op.mergeJoin(t5_a, t4, "?x5");

		Table<String> t6 = m_op.indexJoin(t3, "?x13", IndexDescription.PSOC, SO, names("?x13", "?x11"), DataField.SUBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#teacherOf");

		Table<String> t7 = m_op.indexJoin(t5, "?x5", IndexDescription.PSOC, SO, names("?x5", "?x2"), DataField.SUBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#subOrganizationOf");

		Table<String> t8 = m_op.indexJoin(t2, "?x7", IndexDescription.POCS, SO, names("?x8", "?x7"), DataField.OBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#advisor");

		Table<String> t9 = m_op.indexJoin(t7, "?x1", IndexDescription.POCS, SO, names("?x12", "?x1"), DataField.OBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#publicationAuthor");

		Table<String> t10_a = m_op.indexJoin(t9, "?x1", IndexDescription.PSOC, SO, names("?x1", "?x11"), DataField.SUBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#teachingAssistantOf");
		Table<String> t10 = m_op.mergeJoin(t10_a, t6, "?x11");
		t10 = m_op.compact(t10, Arrays.asList("?x12", "?x13", "?x1", "?x2", "?x5"));

		Table<String> t11 = m_op.indexJoin(t8, "?x7", IndexDescription.POCS, SO, names("?x3", "?x7"), DataField.OBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#publicationAuthor");

		Table<String> t12_a = m_op.indexJoin(t11, "?x3", IndexDescription.PSOC, SO, names("?x3", "?x1"), DataField.SUBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#publicationAuthor");
		Table<String> t12 = m_op.mergeJoin(t12_a, t10, "?x1");
		t12 = m_op.compact(t12, Arrays.asList("?x12", "?x13", "?x1", "?x2", "?x7", "?x8", "?x5"));

		Table<String> t13 = m_op.indexJoin(t12, "?x5", IndexDescription.POCS, SO, names("?x10", "?x5"), DataField.OBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#worksFor");

		Table<String> t14 = m_op.indexJoin(t13, "?x1", "?x10", IndexDescription.PSOC, SO, names("?x1", "?x10"), DataField.SUBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#advisor");
		t14 = m_op.compact(t14, Arrays.asList("?x12", "?x13", "?x1", "?x2", "?x7", "?x8", "?x5"));

		Table<String> t15 = m_op.indexJoin(t14, "?x2", IndexDescription.POCS, SO, names("?x9", "?x2"), DataField.OBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#mastersDegreeFrom");
		t15 = m_op.compact(t15, Arrays.asList("?x1", "?x7", "?x8", "?x5", "?x12", "?x13", "?x9"));

		Table<String> t16 = m_op.indexJoin(t15, "?x12", IndexDescription.PSOC, SO, names("?x12", "?x4"), DataField.SUBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#publicationAuthor");
		t16 = m_op.compact(t16, Arrays.asList("?x4", "?x1", "?x7", "?x8", "?x5", "?x13", "?x9"));

		Table<String> t17 = m_op.indexJoin(t16, "?x8", IndexDescription.PSOC, SO, names("?x8", "?x15"), DataField.SUBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#takesCourse");
		t17 = m_op.compact(t17, Arrays.asList("?x4", "?x1", "?x7", "?x5", "?x13", "?x9"));

		Table<String> t18 = m_op.indexJoin(t17, "?x9", IndexDescription.POCS, SO, names("?x14", "?x9"), DataField.OBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#publicationAuthor");
		t18 = m_op.compact(t18, Arrays.asList("?x4", "?x1", "?x7", "?x5", "?x13"));

		Table<String> t19 = m_op.indexJoin(t18, "?x4", IndexDescription.PSOC, SO, names("?x4", "?x6"), DataField.SUBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#advisor");
		t19 = m_op.compact(t19, Arrays.asList("?x1", "?x7", "?x5", "?x6", "?x13"));

		Table<String> t20 = m_op.indexJoin(t19, "?x6", IndexDescription.PSOC, SO, names("?x6", "?x16"), DataField.SUBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#doctoralDegreeFrom");
		t20 = m_op.compact(t20, Arrays.asList("?x1", "?x7", "?x5", "?x13"));

		}
		public void query_q1_spc_2() throws StorageException {
			PrunedQueryPart p1 = new PrunedQueryPart("p", null);
			p1.addEdge("?x5", "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#publicationAuthor", "?x1");
			p1.addEdge("?x1", "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#undergraduateDegreeFrom", "?x3");
			Table<String> t1 = m_op.load(IndexDescription.POCS, SO, names("?x1", "Research23"), DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#researchInterest", DataField.OBJECT, "Research23");
			t1 = m_op.refineWithPrunedPart(p1, "?x1", t1);

			Table<String> t2 = m_op.load(IndexDescription.POCS, SO, names("?x8", "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#GraduateStudent"), DataField.PROPERTY, "http://www.w3.org/1999/02/22-rdf-syntax-ns#type", DataField.OBJECT, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#GraduateStudent");

			Table<String> t3 = m_op.load(IndexDescription.POCS, SO, names("?x4", "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#Course"), DataField.PROPERTY, "http://www.w3.org/1999/02/22-rdf-syntax-ns#type", DataField.OBJECT, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#Course");

			Table<String> t4_a = m_op.load(IndexDescription.POCS, SO, names("?x8", "GraduateStudent65"), DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#name", DataField.OBJECT, "GraduateStudent65");
			Table<String> t4 = m_op.mergeJoin(t4_a, t2, "?x8");
			t4 = m_op.compact(t4, Arrays.asList("?x8"));

			Table<String> t5 = m_op.load(IndexDescription.POCS, SO, names("?x7", "GraduateStudent82"), DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#name", DataField.OBJECT, "GraduateStudent82");

			Table<String> t6 = m_op.load(IndexDescription.POCS, SO, names("?x2", "FullProfessor0"), DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#name", DataField.OBJECT, "FullProfessor0");

			Table<String> t7_a = m_op.indexJoin(t5, "?x7", IndexDescription.PSOC, SO, names("?x7", "?x2"), DataField.SUBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#advisor");
			Table<String> t7 = m_op.mergeJoin(t7_a, t6, "?x2");

			Table<String> t8_a = m_op.indexJoin(t1, "?x1", IndexDescription.PSOC, SO, names("?x1", "?x4"), DataField.SUBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#teacherOf");
			Table<String> t8 = m_op.mergeJoin(t8_a, t3, "?x4");

			Table<String> t9_a = m_op.indexJoin(t4, "?x8", IndexDescription.PSOC, SO, names("?x8", "?x1"), DataField.SUBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#advisor");
			Table<String> t9 = m_op.mergeJoin(t9_a, t8, "?x1");

			Table<String> t10 = m_op.indexJoin(t7, "?x2", IndexDescription.PSOC, SO, names("?x2", "?x6"), DataField.SUBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#headOf");

			Table<String> t11_a = m_op.indexJoin(t9, "?x1", IndexDescription.PSOC, SO, names("?x1", "?x6"), DataField.SUBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#worksFor");
			Table<String> t11 = m_op.mergeJoin(t11_a, t10, "?x6");
			t11 = m_op.compact(t11, Arrays.asList("?x4", "?x1", "?x2", "?x7", "?x8"));

			}
			public void query_q2_spc_2() throws StorageException {
			PrunedQueryPart p1 = new PrunedQueryPart("p", null);
			p1.addEdge("?x1", "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#doctoralDegreeFrom", "?x3");
			Table<String> t1 = m_op.load(IndexDescription.POCS, SO, names("?x1", "Lecturer3@Department5.University0.edu"), DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#emailAddress", DataField.OBJECT, "Lecturer3@Department5.University0.edu");
			t1 = m_op.refineWithPrunedPart(p1, "?x1", t1);

			Table<String> t2 = m_op.load(IndexDescription.POCS, SO, names("?x4", "Publication2"), DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#name", DataField.OBJECT, "Publication2");

			Table<String> t3 = m_op.load(IndexDescription.POCS, SO, names("?x5", "Publication1"), DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#name", DataField.OBJECT, "Publication1");

			Table<String> t4 = m_op.load(IndexDescription.POCS, SO, names("?x2", "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#GraduateCourse"), DataField.PROPERTY, "http://www.w3.org/1999/02/22-rdf-syntax-ns#type", DataField.OBJECT, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#GraduateCourse");

			Table<String> t5_a = m_op.indexJoin(t3, "?x5", IndexDescription.PSOC, SO, names("?x5", "?x1"), DataField.SUBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#publicationAuthor");
			Table<String> t5 = m_op.mergeJoin(t5_a, t1, "?x1");

			Table<String> t6_a = m_op.indexJoin(t5, "?x1", IndexDescription.PSOC, SO, names("?x1", "?x2"), DataField.SUBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#teacherOf");
			Table<String> t6 = m_op.mergeJoin(t6_a, t4, "?x2");

			Table<String> t7_a = m_op.indexJoin(t2, "?x4", IndexDescription.PSOC, SO, names("?x4", "?x1"), DataField.SUBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#publicationAuthor");
			Table<String> t7 = m_op.mergeJoin(t7_a, t6, "?x1");

			}
			public void query_q3_spc_2() throws StorageException {
			PrunedQueryPart p1 = new PrunedQueryPart("p", null);
			p1.addEdge("?x5", "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#publicationAuthor", "?x3");
			p1.addEdge("?x7", "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#publicationAuthor", "?x3");
			Table<String> t1 = m_op.load(IndexDescription.POCS, SO, names("?x1", "GraduateCourse28"), DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#name", DataField.OBJECT, "GraduateCourse28");

			Table<String> t2 = m_op.load(IndexDescription.POCS, SO, names("?x6", "Research8"), DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#researchInterest", DataField.OBJECT, "Research8");

			Table<String> t3 = m_op.load(IndexDescription.POCS, SO, names("?x4", "AssistantProfessor8@Department8.University0.edu"), DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#emailAddress", DataField.OBJECT, "AssistantProfessor8@Department8.University0.edu");

			Table<String> t4 = m_op.load(IndexDescription.POCS, SO, names("?x5", "Publication4"), DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#name", DataField.OBJECT, "Publication4");
			t4 = m_op.refineWithPrunedPart(p1, "?x5", t4);

			Table<String> t5_a = m_op.indexJoin(t4, "?x5", IndexDescription.PSOC, SO, names("?x5", "?x4"), DataField.SUBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#publicationAuthor");
			Table<String> t5 = m_op.mergeJoin(t5_a, t3, "?x4");

			Table<String> t6_a = m_op.indexJoin(t2, "?x6", IndexDescription.PSOC, SO, names("?x6", "?x1"), DataField.SUBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#teacherOf");
			Table<String> t6 = m_op.mergeJoin(t6_a, t1, "?x1");

			Table<String> t7 = m_op.indexJoin(t6, "?x1", IndexDescription.POCS, SO, names("?x2", "?x1"), DataField.OBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#takesCourse");

			Table<String> t8_a = m_op.indexJoin(t7, "?x2", IndexDescription.PSOC, SO, names("?x2", "?x4"), DataField.SUBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#advisor");
			Table<String> t8 = m_op.mergeJoin(t8_a, t5, "?x4");
			t8 = m_op.compact(t8, Arrays.asList("?x4", "?x1", "?x5", "?x6"));

			}
			public void query_q4_spc_2() throws StorageException {
			PrunedQueryPart p1 = new PrunedQueryPart("p", null);
			p1.addEdge("?x1", "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#mastersDegreeFrom", "?x3");
			p1.addEdge("?x5", "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#advisor", "?x1");
			PrunedQueryPart p2 = new PrunedQueryPart("p", null);
			p2.addEdge("?x2", "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#publicationAuthor", "?x4");
			Table<String> t1 = m_op.load(IndexDescription.POCS, SO, names("?x1", "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#AssociateProfessor"), DataField.PROPERTY, "http://www.w3.org/1999/02/22-rdf-syntax-ns#type", DataField.OBJECT, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#AssociateProfessor");
			t1 = m_op.refineWithPrunedPart(p1, "?x1", t1);

			Table<String> t2 = m_op.load(IndexDescription.POCS, SO, names("?x2", "Publication6"), DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#name", DataField.OBJECT, "Publication6");
			t2 = m_op.refineWithPrunedPart(p2, "?x2", t2);

			Table<String> t3_a = m_op.load(IndexDescription.POCS, SO, names("?x1", "AssociateProfessor0@Department2.University0.edu"), DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#emailAddress", DataField.OBJECT, "AssociateProfessor0@Department2.University0.edu");
			Table<String> t3 = m_op.mergeJoin(t3_a, t1, "?x1");
			t3 = m_op.compact(t3, Arrays.asList("?x1"));

			Table<String> t4 = m_op.load(IndexDescription.POCS, SO, names("?x6", "Publication8"), DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#name", DataField.OBJECT, "Publication8");

			Table<String> t5_a = m_op.indexJoin(t2, "?x2", IndexDescription.PSOC, SO, names("?x2", "?x1"), DataField.SUBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#publicationAuthor");
			Table<String> t5 = m_op.mergeJoin(t5_a, t3, "?x1");

			Table<String> t6_a = m_op.indexJoin(t4, "?x6", IndexDescription.PSOC, SO, names("?x6", "?x1"), DataField.SUBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#publicationAuthor");
			Table<String> t6 = m_op.mergeJoin(t6_a, t5, "?x1");

			}
			public void query_q5_spc_2() throws StorageException {
			PrunedQueryPart p1 = new PrunedQueryPart("p", null);
			p1.addEdge("?x7", "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#undergraduateDegreeFrom", "?x2");
			p1.addEdge("?x11", "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#undergraduateDegreeFrom", "?x2");
			PrunedQueryPart p2 = new PrunedQueryPart("p", null);
			p2.addEdge("?x12", "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#subOrganizationOf", "?x4");
			PrunedQueryPart p3 = new PrunedQueryPart("p", null);
			p3.addEdge("?x10", "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#publicationAuthor", "?x9");
			Table<String> t1 = m_op.load(IndexDescription.POCS, SO, names("?x1", "GraduateStudent15"), DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#name", DataField.OBJECT, "GraduateStudent15");

			Table<String> t2 = m_op.load(IndexDescription.POCS, SO, names("?x6", "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#Publication"), DataField.PROPERTY, "http://www.w3.org/1999/02/22-rdf-syntax-ns#type", DataField.OBJECT, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#Publication");

			Table<String> t3 = m_op.load(IndexDescription.POCS, SO, names("?x4", "Department3"), DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#name", DataField.OBJECT, "Department3");
			t3 = m_op.refineWithPrunedPart(p2, "?x4", t3);

			Table<String> t4_a = m_op.load(IndexDescription.POCS, SO, names("?x4", "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#Department"), DataField.PROPERTY, "http://www.w3.org/1999/02/22-rdf-syntax-ns#type", DataField.OBJECT, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#Department");
			Table<String> t4 = m_op.mergeJoin(t4_a, t3, "?x4");
			t4 = m_op.compact(t4, Arrays.asList("?x4"));

			Table<String> t5 = m_op.load(IndexDescription.POCS, SO, names("?x7", "AssociateProfessor9"), DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#name", DataField.OBJECT, "AssociateProfessor9");
			t5 = m_op.refineWithPrunedPart(p1, "?x7", t5);

			Table<String> t6 = m_op.load(IndexDescription.POCS, SO, names("?x3", "Publication15"), DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#name", DataField.OBJECT, "Publication15");

			Table<String> t7 = m_op.load(IndexDescription.POCS, SO, names("?x5", "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#GraduateStudent"), DataField.PROPERTY, "http://www.w3.org/1999/02/22-rdf-syntax-ns#type", DataField.OBJECT, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#GraduateStudent");

			Table<String> t8 = m_op.load(IndexDescription.POCS, SO, names("?x9", "AssistantProfessor4"), DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#name", DataField.OBJECT, "AssistantProfessor4");
			t8 = m_op.refineWithPrunedPart(p3, "?x9", t8);

			Table<String> t9_a = m_op.indexJoin(t6, "?x3", IndexDescription.PSOC, SO, names("?x3", "?x1"), DataField.SUBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#publicationAuthor");
			Table<String> t9 = m_op.mergeJoin(t9_a, t1, "?x1");

			Table<String> t10_a = m_op.indexJoin(t7, "?x5", IndexDescription.PSOC, SO, names("?x5", "?x4"), DataField.SUBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#memberOf");
			Table<String> t10 = m_op.mergeJoin(t10_a, t4, "?x4");

			Table<String> t11_a = m_op.indexJoin(t2, "?x6", IndexDescription.PSOC, SO, names("?x6", "?x1"), DataField.SUBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#publicationAuthor");
			Table<String> t11 = m_op.mergeJoin(t11_a, t9, "?x1");

			Table<String> t12_a = m_op.indexJoin(t5, "?x7", IndexDescription.PSOC, SO, names("?x7", "?x4"), DataField.SUBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#worksFor");
			Table<String> t12 = m_op.mergeJoin(t12_a, t10, "?x4");

			Table<String> t13_a = m_op.indexJoin(t11, "?x1", IndexDescription.PSOC, SO, names("?x1", "?x4"), DataField.SUBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#memberOf");
			Table<String> t13 = m_op.mergeJoin(t13_a, t12, "?x4");

			Table<String> t14 = m_op.indexJoin(t13, "?x6", "?x5", IndexDescription.PSOC, SO, names("?x6", "?x5"), DataField.SUBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#publicationAuthor");

			Table<String> t15 = m_op.indexJoin(t14, "?x1", IndexDescription.PSOC, SO, names("?x1", "?x8"), DataField.SUBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#takesCourse");

			Table<String> t16_a = m_op.indexJoin(t8, "?x9", IndexDescription.PSOC, SO, names("?x9", "?x8"), DataField.SUBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#teacherOf");
			Table<String> t16 = m_op.mergeJoin(t16_a, t15, "?x8");
			t16 = m_op.compact(t16, Arrays.asList("?x3", "?x4", "?x1", "?x7", "?x5", "?x6", "?x9"));

			}
			public void query_q6_spc_2() throws StorageException {
			PrunedQueryPart p1 = new PrunedQueryPart("p", null);
			p1.addEdge("?x1", "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#undergraduateDegreeFrom", "?x3");
			p1.addEdge("?x4", "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#publicationAuthor", "?x1");
			p1.addEdge("?x1", "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#advisor", "?x5");
			p1.addEdge("?x1", "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#memberOf", "?x2");
			p1.addEdge("?x1", "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#takesCourse", "?x7");
			Table<String> t1 = m_op.load(IndexDescription.POCS, SO, names("?x1", "GraduateStudent118@Department8.University0.edu"), DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#emailAddress", DataField.OBJECT, "GraduateStudent118@Department8.University0.edu");
			t1 = m_op.refineWithPrunedPart(p1, "?x1", t1);

			Table<String> t2 = m_op.load(IndexDescription.POCS, SO, names("?x6", "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#GraduateCourse"), DataField.PROPERTY, "http://www.w3.org/1999/02/22-rdf-syntax-ns#type", DataField.OBJECT, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#GraduateCourse");

			Table<String> t3_a = m_op.load(IndexDescription.POCS, SO, names("?x6", "GraduateCourse5"), DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#name", DataField.OBJECT, "GraduateCourse5");
			Table<String> t3 = m_op.mergeJoin(t3_a, t2, "?x6");
			t3 = m_op.compact(t3, Arrays.asList("?x6"));

			Table<String> t4_a = m_op.indexJoin(t1, "?x1", IndexDescription.PSOC, SO, names("?x1", "?x6"), DataField.SUBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#takesCourse");
			Table<String> t4 = m_op.mergeJoin(t4_a, t3, "?x6");

			}
			public void query_q7_spc_2() throws StorageException {
			PrunedQueryPart p1 = new PrunedQueryPart("p", null);
			p1.addEdge("?x1", "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#advisor", "?x5");
			p1.addEdge("?x6", "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#publicationAuthor", "?x1");
			p1.addEdge("?x1", "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#memberOf", "?x3");
			p1.addEdge("?x2", "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#publicationAuthor", "?x1");
			p1.addEdge("?x7", "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#publicationAuthor", "?x1");
			p1.addEdge("?x2", "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#publicationAuthor", "?x8");
			Table<String> t1 = m_op.load(IndexDescription.POCS, SO, names("?x1", "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#GraduateStudent"), DataField.PROPERTY, "http://www.w3.org/1999/02/22-rdf-syntax-ns#type", DataField.OBJECT, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#GraduateStudent");
			t1 = m_op.refineWithPrunedPart(p1, "?x1", t1);

			Table<String> t2_a = m_op.load(IndexDescription.POCS, SO, names("?x1", "GraduateStudent42@Department8.University0.edu"), DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#emailAddress", DataField.OBJECT, "GraduateStudent42@Department8.University0.edu");
			Table<String> t2 = m_op.mergeJoin(t2_a, t1, "?x1");
			t2 = m_op.compact(t2, Arrays.asList("?x1"));

			Table<String> t3 = m_op.load(IndexDescription.POCS, SO, names("?x4", "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#Course"), DataField.PROPERTY, "http://www.w3.org/1999/02/22-rdf-syntax-ns#type", DataField.OBJECT, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#Course");

			Table<String> t4_a = m_op.load(IndexDescription.POCS, SO, names("?x4", "Course42"), DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#name", DataField.OBJECT, "Course42");
			Table<String> t4 = m_op.mergeJoin(t4_a, t3, "?x4");
			t4 = m_op.compact(t4, Arrays.asList("?x4"));

			Table<String> t5_a = m_op.indexJoin(t2, "?x1", IndexDescription.PSOC, SO, names("?x1", "?x4"), DataField.SUBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#teachingAssistantOf");
			Table<String> t5 = m_op.mergeJoin(t5_a, t4, "?x4");

			}
			public void query_q8_spc_2() throws StorageException {
			PrunedQueryPart p1 = new PrunedQueryPart("p", null);
			p1.addEdge("?x1", "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#takesCourse", "?x9");
			p1.addEdge("?x1", "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#memberOf", "?x3");
			p1.addEdge("?x1", "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#takesCourse", "?x8");
			p1.addEdge("?x7", "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#publicationAuthor", "?x1");
			p1.addEdge("?x10", "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#headOf", "?x3");
			PrunedQueryPart p2 = new PrunedQueryPart("p", null);
			p2.addEdge("?x4", "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#teacherOf", "?x5");
			PrunedQueryPart p3 = new PrunedQueryPart("p", null);
			p3.addEdge("?x2", "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#publicationAuthor", "?x6");
			Table<String> t1 = m_op.load(IndexDescription.POCS, SO, names("?x1", "GraduateStudent15@Department6.University0.edu"), DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#emailAddress", DataField.OBJECT, "GraduateStudent15@Department6.University0.edu");
			t1 = m_op.refineWithPrunedPart(p1, "?x1", t1);

			Table<String> t2 = m_op.load(IndexDescription.POCS, SO, names("?x2", "Publication11"), DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#name", DataField.OBJECT, "Publication11");
			t2 = m_op.refineWithPrunedPart(p3, "?x2", t2);

			Table<String> t3 = m_op.load(IndexDescription.POCS, SO, names("?x4", "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#FullProfessor"), DataField.PROPERTY, "http://www.w3.org/1999/02/22-rdf-syntax-ns#type", DataField.OBJECT, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#FullProfessor");
			t3 = m_op.refineWithPrunedPart(p2, "?x4", t3);

			Table<String> t4_a = m_op.indexJoin(t1, "?x1", IndexDescription.PSOC, SO, names("?x1", "?x4"), DataField.SUBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#advisor");
			Table<String> t4 = m_op.mergeJoin(t4_a, t3, "?x4");

			Table<String> t5_a = m_op.indexJoin(t2, "?x2", IndexDescription.PSOC, SO, names("?x2", "?x1"), DataField.SUBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#publicationAuthor");
			Table<String> t5 = m_op.mergeJoin(t5_a, t4, "?x1");

			}
			public void query_q9_spc_2() throws StorageException {
			PrunedQueryPart p1 = new PrunedQueryPart("p", null);
			p1.addEdge("?x5", "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#advisor", "?x12");
			p1.addEdge("?x15", "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#publicationAuthor", "?x5");
			PrunedQueryPart p2 = new PrunedQueryPart("p", null);
			p2.addEdge("?x14", "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#publicationAuthor", "?x11");
			p2.addEdge("?x11", "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#worksFor", "?x13");
			Table<String> t1 = m_op.load(IndexDescription.POCS, SO, names("?x1", "Lecturer5"), DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#name", DataField.OBJECT, "Lecturer5");

			Table<String> t2 = m_op.load(IndexDescription.POCS, SO, names("?x3", "GraduateCourse47"), DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#name", DataField.OBJECT, "GraduateCourse47");

			Table<String> t3 = m_op.load(IndexDescription.POCS, SO, names("?x10", "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#Publication"), DataField.PROPERTY, "http://www.w3.org/1999/02/22-rdf-syntax-ns#type", DataField.OBJECT, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#Publication");

			Table<String> t4 = m_op.load(IndexDescription.POCS, SO, names("?x7", "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#University"), DataField.PROPERTY, "http://www.w3.org/1999/02/22-rdf-syntax-ns#type", DataField.OBJECT, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#University");

			Table<String> t5 = m_op.load(IndexDescription.POCS, SO, names("?x2", "GraduateStudent29"), DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#name", DataField.OBJECT, "GraduateStudent29");

			Table<String> t6 = m_op.load(IndexDescription.POCS, SO, names("?x12", "AssistantProfessor4"), DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#name", DataField.OBJECT, "AssistantProfessor4");
			t6 = m_op.refineWithPrunedPart(p1, "?x12", t6);

			Table<String> t7_a = m_op.indexJoin(t1, "?x1", IndexDescription.PSOC, SO, names("?x1", "?x7"), DataField.SUBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#doctoralDegreeFrom");
			Table<String> t7 = m_op.mergeJoin(t7_a, t4, "?x7");

			Table<String> t8_a = m_op.indexJoin(t3, "?x10", IndexDescription.PSOC, SO, names("?x10", "?x2"), DataField.SUBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#publicationAuthor");
			Table<String> t8 = m_op.mergeJoin(t8_a, t5, "?x2");

			Table<String> t9_a = m_op.indexJoin(t7, "?x1", IndexDescription.PSOC, SO, names("?x1", "?x3"), DataField.SUBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#teacherOf");
			Table<String> t9 = m_op.mergeJoin(t9_a, t2, "?x3");

			Table<String> t10_a = m_op.indexJoin(t8, "?x2", IndexDescription.PSOC, SO, names("?x2", "?x7"), DataField.SUBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#undergraduateDegreeFrom");
			Table<String> t10 = m_op.mergeJoin(t10_a, t9, "?x7");

			Table<String> t11 = m_op.indexJoin(t6, "?x12", IndexDescription.PSOC, SO, names("?x12", "?x9"), DataField.SUBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#mastersDegreeFrom");

			Table<String> t12 = m_op.indexJoin(t10, "?x3", IndexDescription.POCS, SO, names("?x6", "?x3"), DataField.OBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#takesCourse");

			Table<String> t13_a = m_op.indexJoin(t12, "?x1", IndexDescription.PSOC, SO, names("?x1", "?x9"), DataField.SUBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#mastersDegreeFrom");
			Table<String> t13 = m_op.mergeJoin(t13_a, t11, "?x9");
			t13 = m_op.compact(t13, Arrays.asList("?x3", "?x1", "?x2", "?x7", "?x6", "?x12", "?x10"));

			Table<String> t14 = m_op.indexJoin(t13, "?x6", IndexDescription.POCS, SO, names("?x8", "?x6"), DataField.OBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#publicationAuthor");
			t14 = m_op.compact(t14, Arrays.asList("?x3", "?x1", "?x2", "?x7", "?x8", "?x12", "?x10"));

			Table<String> t15 = m_op.indexJoin(t14, "?x8", IndexDescription.PSOC, SO, names("?x8", "?x4"), DataField.SUBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#publicationAuthor");
			t15 = m_op.compact(t15, Arrays.asList("?x3", "?x4", "?x1", "?x2", "?x7", "?x12", "?x10"));

			Table<String> t16 = m_op.indexJoin(t15, "?x4", IndexDescription.PSOC, SO, names("?x4", "?x13"), DataField.SUBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#worksFor");
			t16 = m_op.refineWithPrunedPart(p2, "?x13", t16);
			t16 = m_op.compact(t16, Arrays.asList("?x3", "?x1", "?x2", "?x7", "?x12", "?x10"));

			}
			public void query_q10_spc_2() throws StorageException {
			PrunedQueryPart p1 = new PrunedQueryPart("p", null);
			p1.addEdge("?x9", "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#mastersDegreeFrom", "?x2");
			p1.addEdge("?x14", "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#publicationAuthor", "?x9");
			PrunedQueryPart p2 = new PrunedQueryPart("p", null);
			p2.addEdge("?x8", "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#advisor", "?x7");
			p2.addEdge("?x8", "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#takesCourse", "?x15");
			PrunedQueryPart p3 = new PrunedQueryPart("p", null);
			p3.addEdge("?x6", "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#doctoralDegreeFrom", "?x16");
			p3.addEdge("?x4", "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#advisor", "?x6");
			Table<String> t1 = m_op.load(IndexDescription.POCS, SO, names("?x1", "GraduateStudent11@Department7.University0.edu"), DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#emailAddress", DataField.OBJECT, "GraduateStudent11@Department7.University0.edu");

			Table<String> t2 = m_op.load(IndexDescription.POCS, SO, names("?x13", "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#AssistantProfessor"), DataField.PROPERTY, "http://www.w3.org/1999/02/22-rdf-syntax-ns#type", DataField.OBJECT, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#AssistantProfessor");

			Table<String> t3 = m_op.load(IndexDescription.POCS, SO, names("?x5", "Department7"), DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#name", DataField.OBJECT, "Department7");

			Table<String> t4 = m_op.load(IndexDescription.POCS, SO, names("?x7", "AssociateProfessor6"), DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#name", DataField.OBJECT, "AssociateProfessor6");
			t4 = m_op.refineWithPrunedPart(p2, "?x7", t4);

			Table<String> t5_a = m_op.indexJoin(t1, "?x1", IndexDescription.PSOC, SO, names("?x1", "?x5"), DataField.SUBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#memberOf");
			Table<String> t5 = m_op.mergeJoin(t5_a, t3, "?x5");

			Table<String> t6 = m_op.indexJoin(t5, "?x1", IndexDescription.PSOC, SO, names("?x1", "?x10"), DataField.SUBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#advisor");

			Table<String> t7 = m_op.indexJoin(t2, "?x13", IndexDescription.PSOC, SO, names("?x13", "?x11"), DataField.SUBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#teacherOf");

			Table<String> t8 = m_op.indexJoin(t6, "?x1", IndexDescription.POCS, SO, names("?x12", "?x1"), DataField.OBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#publicationAuthor");

			Table<String> t9 = m_op.indexJoin(t8, "?x5", IndexDescription.PSOC, SO, names("?x5", "?x2"), DataField.SUBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#subOrganizationOf");
			t9 = m_op.refineWithPrunedPart(p1, "?x2", t9);
			t9 = m_op.compact(t9, Arrays.asList("?x1", "?x5", "?x12", "?x10"));

			Table<String> t10 = m_op.indexJoin(t9, "?x10", "?x5", IndexDescription.PSOC, SO, names("?x10", "?x5"), DataField.SUBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#worksFor");
			t10 = m_op.compact(t10, Arrays.asList("?x1", "?x5", "?x12"));

			Table<String> t11_a = m_op.indexJoin(t10, "?x1", IndexDescription.PSOC, SO, names("?x1", "?x11"), DataField.SUBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#teachingAssistantOf");
			Table<String> t11 = m_op.mergeJoin(t11_a, t7, "?x11");
			t11 = m_op.compact(t11, Arrays.asList("?x1", "?x5", "?x12", "?x13"));

			Table<String> t12 = m_op.indexJoin(t11, "?x1", IndexDescription.POCS, SO, names("?x3", "?x1"), DataField.OBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#publicationAuthor");

			Table<String> t13_a = m_op.indexJoin(t12, "?x3", IndexDescription.PSOC, SO, names("?x3", "?x7"), DataField.SUBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#publicationAuthor");
			Table<String> t13 = m_op.mergeJoin(t13_a, t4, "?x7");
			t13 = m_op.compact(t13, Arrays.asList("?x1", "?x7", "?x5", "?x12", "?x13"));

			Table<String> t14 = m_op.indexJoin(t13, "?x12", IndexDescription.PSOC, SO, names("?x12", "?x4"), DataField.SUBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#publicationAuthor");
			t14 = m_op.refineWithPrunedPart(p3, "?x4", t14);
			t14 = m_op.compact(t14, Arrays.asList("?x1", "?x7", "?x5", "?x13"));

			}

			public void query_q1_spc_1() throws StorageException {
				PrunedQueryPart p1 = new PrunedQueryPart("p", null);
				p1.addEdge("?x5", "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#publicationAuthor", "?x1");
				p1.addEdge("?x1", "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#undergraduateDegreeFrom", "?x3");
				Table<String> t1 = m_op.load(IndexDescription.POCS, SO, names("?x1", "Research23"), DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#researchInterest", DataField.OBJECT, "Research23");
				t1 = m_op.refineWithPrunedPart(p1, "?x1", t1);

				Table<String> t2 = m_op.load(IndexDescription.POCS, SO, names("?x8", "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#GraduateStudent"), DataField.PROPERTY, "http://www.w3.org/1999/02/22-rdf-syntax-ns#type", DataField.OBJECT, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#GraduateStudent");

				Table<String> t3 = m_op.load(IndexDescription.POCS, SO, names("?x4", "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#Course"), DataField.PROPERTY, "http://www.w3.org/1999/02/22-rdf-syntax-ns#type", DataField.OBJECT, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#Course");

				Table<String> t4_a = m_op.load(IndexDescription.POCS, SO, names("?x8", "GraduateStudent65"), DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#name", DataField.OBJECT, "GraduateStudent65");
				Table<String> t4 = m_op.mergeJoin(t4_a, t2, "?x8");
				t4 = m_op.compact(t4, Arrays.asList("?x8"));

				Table<String> t5 = m_op.load(IndexDescription.POCS, SO, names("?x7", "GraduateStudent82"), DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#name", DataField.OBJECT, "GraduateStudent82");

				Table<String> t6 = m_op.load(IndexDescription.POCS, SO, names("?x2", "FullProfessor0"), DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#name", DataField.OBJECT, "FullProfessor0");

				Table<String> t7_a = m_op.indexJoin(t5, "?x7", IndexDescription.PSOC, SO, names("?x7", "?x2"), DataField.SUBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#advisor");
				Table<String> t7 = m_op.mergeJoin(t7_a, t6, "?x2");

				Table<String> t8_a = m_op.indexJoin(t1, "?x1", IndexDescription.PSOC, SO, names("?x1", "?x4"), DataField.SUBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#teacherOf");
				Table<String> t8 = m_op.mergeJoin(t8_a, t3, "?x4");

				Table<String> t9_a = m_op.indexJoin(t4, "?x8", IndexDescription.PSOC, SO, names("?x8", "?x1"), DataField.SUBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#advisor");
				Table<String> t9 = m_op.mergeJoin(t9_a, t8, "?x1");

				Table<String> t10 = m_op.indexJoin(t7, "?x2", IndexDescription.PSOC, SO, names("?x2", "?x6"), DataField.SUBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#headOf");

				Table<String> t11_a = m_op.indexJoin(t9, "?x1", IndexDescription.PSOC, SO, names("?x1", "?x6"), DataField.SUBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#worksFor");
				Table<String> t11 = m_op.mergeJoin(t11_a, t10, "?x6");
				t11 = m_op.compact(t11, Arrays.asList("?x4", "?x1", "?x2", "?x7", "?x8"));

				}
				public void query_q2_spc_1() throws StorageException {
				PrunedQueryPart p1 = new PrunedQueryPart("p", null);
				p1.addEdge("?x1", "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#doctoralDegreeFrom", "?x3");
				Table<String> t1 = m_op.load(IndexDescription.POCS, SO, names("?x1", "Lecturer3@Department5.University0.edu"), DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#emailAddress", DataField.OBJECT, "Lecturer3@Department5.University0.edu");
				t1 = m_op.refineWithPrunedPart(p1, "?x1", t1);

				Table<String> t2 = m_op.load(IndexDescription.POCS, SO, names("?x4", "Publication2"), DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#name", DataField.OBJECT, "Publication2");

				Table<String> t3 = m_op.load(IndexDescription.POCS, SO, names("?x5", "Publication1"), DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#name", DataField.OBJECT, "Publication1");

				Table<String> t4 = m_op.load(IndexDescription.POCS, SO, names("?x2", "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#GraduateCourse"), DataField.PROPERTY, "http://www.w3.org/1999/02/22-rdf-syntax-ns#type", DataField.OBJECT, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#GraduateCourse");

				Table<String> t5_a = m_op.indexJoin(t3, "?x5", IndexDescription.PSOC, SO, names("?x5", "?x1"), DataField.SUBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#publicationAuthor");
				Table<String> t5 = m_op.mergeJoin(t5_a, t1, "?x1");

				Table<String> t6_a = m_op.indexJoin(t5, "?x1", IndexDescription.PSOC, SO, names("?x1", "?x2"), DataField.SUBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#teacherOf");
				Table<String> t6 = m_op.mergeJoin(t6_a, t4, "?x2");

				Table<String> t7_a = m_op.indexJoin(t2, "?x4", IndexDescription.PSOC, SO, names("?x4", "?x1"), DataField.SUBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#publicationAuthor");
				Table<String> t7 = m_op.mergeJoin(t7_a, t6, "?x1");

				}
				public void query_q3_spc_1() throws StorageException {
				PrunedQueryPart p1 = new PrunedQueryPart("p", null);
				p1.addEdge("?x7", "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#publicationAuthor", "?x3");
				Table<String> t1 = m_op.load(IndexDescription.POCS, SO, names("?x1", "GraduateCourse28"), DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#name", DataField.OBJECT, "GraduateCourse28");

				Table<String> t2 = m_op.load(IndexDescription.POCS, SO, names("?x6", "Research8"), DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#researchInterest", DataField.OBJECT, "Research8");

				Table<String> t3 = m_op.load(IndexDescription.POCS, SO, names("?x4", "AssistantProfessor8@Department8.University0.edu"), DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#emailAddress", DataField.OBJECT, "AssistantProfessor8@Department8.University0.edu");

				Table<String> t4 = m_op.load(IndexDescription.POCS, SO, names("?x5", "Publication4"), DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#name", DataField.OBJECT, "Publication4");

				Table<String> t5_a = m_op.indexJoin(t4, "?x5", IndexDescription.PSOC, SO, names("?x5", "?x4"), DataField.SUBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#publicationAuthor");
				Table<String> t5 = m_op.mergeJoin(t5_a, t3, "?x4");

				Table<String> t6_a = m_op.indexJoin(t2, "?x6", IndexDescription.PSOC, SO, names("?x6", "?x1"), DataField.SUBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#teacherOf");
				Table<String> t6 = m_op.mergeJoin(t6_a, t1, "?x1");

				Table<String> t7 = m_op.indexJoin(t5, "?x5", IndexDescription.PSOC, SO, names("?x5", "?x3"), DataField.SUBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#publicationAuthor");
				t7 = m_op.refineWithPrunedPart(p1, "?x3", t7);
				t7 = m_op.compact(t7, Arrays.asList("?x4", "?x5"));

				Table<String> t8 = m_op.indexJoin(t6, "?x1", IndexDescription.POCS, SO, names("?x2", "?x1"), DataField.OBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#takesCourse");

				Table<String> t9_a = m_op.indexJoin(t8, "?x2", IndexDescription.PSOC, SO, names("?x2", "?x4"), DataField.SUBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#advisor");
				Table<String> t9 = m_op.mergeJoin(t9_a, t7, "?x4");
				t9 = m_op.compact(t9, Arrays.asList("?x4", "?x1", "?x5", "?x6"));

				}
				public void query_q4_spc_1() throws StorageException {
				PrunedQueryPart p1 = new PrunedQueryPart("p", null);
				p1.addEdge("?x2", "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#publicationAuthor", "?x4");
				PrunedQueryPart p2 = new PrunedQueryPart("p", null);
				p2.addEdge("?x1", "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#mastersDegreeFrom", "?x3");
				p2.addEdge("?x5", "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#advisor", "?x1");
				Table<String> t1 = m_op.load(IndexDescription.POCS, SO, names("?x1", "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#AssociateProfessor"), DataField.PROPERTY, "http://www.w3.org/1999/02/22-rdf-syntax-ns#type", DataField.OBJECT, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#AssociateProfessor");
				t1 = m_op.refineWithPrunedPart(p2, "?x1", t1);

				Table<String> t2 = m_op.load(IndexDescription.POCS, SO, names("?x2", "Publication6"), DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#name", DataField.OBJECT, "Publication6");
				t2 = m_op.refineWithPrunedPart(p1, "?x2", t2);

				Table<String> t3_a = m_op.load(IndexDescription.POCS, SO, names("?x1", "AssociateProfessor0@Department2.University0.edu"), DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#emailAddress", DataField.OBJECT, "AssociateProfessor0@Department2.University0.edu");
				Table<String> t3 = m_op.mergeJoin(t3_a, t1, "?x1");
				t3 = m_op.compact(t3, Arrays.asList("?x1"));

				Table<String> t4 = m_op.load(IndexDescription.POCS, SO, names("?x6", "Publication8"), DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#name", DataField.OBJECT, "Publication8");

				Table<String> t5_a = m_op.indexJoin(t2, "?x2", IndexDescription.PSOC, SO, names("?x2", "?x1"), DataField.SUBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#publicationAuthor");
				Table<String> t5 = m_op.mergeJoin(t5_a, t3, "?x1");

				Table<String> t6_a = m_op.indexJoin(t4, "?x6", IndexDescription.PSOC, SO, names("?x6", "?x1"), DataField.SUBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#publicationAuthor");
				Table<String> t6 = m_op.mergeJoin(t6_a, t5, "?x1");

				}
				public void query_q5_spc_1() throws StorageException {
				PrunedQueryPart p1 = new PrunedQueryPart("p", null);
				p1.addEdge("?x10", "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#publicationAuthor", "?x9");
				PrunedQueryPart p2 = new PrunedQueryPart("p", null);
				p2.addEdge("?x11", "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#undergraduateDegreeFrom", "?x2");
				PrunedQueryPart p3 = new PrunedQueryPart("p", null);
				p3.addEdge("?x12", "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#subOrganizationOf", "?x4");
				Table<String> t1 = m_op.load(IndexDescription.POCS, SO, names("?x1", "GraduateStudent15"), DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#name", DataField.OBJECT, "GraduateStudent15");

				Table<String> t2 = m_op.load(IndexDescription.POCS, SO, names("?x6", "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#Publication"), DataField.PROPERTY, "http://www.w3.org/1999/02/22-rdf-syntax-ns#type", DataField.OBJECT, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#Publication");

				Table<String> t3 = m_op.load(IndexDescription.POCS, SO, names("?x4", "Department3"), DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#name", DataField.OBJECT, "Department3");
				t3 = m_op.refineWithPrunedPart(p3, "?x4", t3);

				Table<String> t4_a = m_op.load(IndexDescription.POCS, SO, names("?x4", "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#Department"), DataField.PROPERTY, "http://www.w3.org/1999/02/22-rdf-syntax-ns#type", DataField.OBJECT, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#Department");
				Table<String> t4 = m_op.mergeJoin(t4_a, t3, "?x4");
				t4 = m_op.compact(t4, Arrays.asList("?x4"));

				Table<String> t5 = m_op.load(IndexDescription.POCS, SO, names("?x7", "AssociateProfessor9"), DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#name", DataField.OBJECT, "AssociateProfessor9");

				Table<String> t6 = m_op.load(IndexDescription.POCS, SO, names("?x3", "Publication15"), DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#name", DataField.OBJECT, "Publication15");

				Table<String> t7 = m_op.load(IndexDescription.POCS, SO, names("?x5", "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#GraduateStudent"), DataField.PROPERTY, "http://www.w3.org/1999/02/22-rdf-syntax-ns#type", DataField.OBJECT, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#GraduateStudent");

				Table<String> t8 = m_op.load(IndexDescription.POCS, SO, names("?x9", "AssistantProfessor4"), DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#name", DataField.OBJECT, "AssistantProfessor4");
				t8 = m_op.refineWithPrunedPart(p1, "?x9", t8);

				Table<String> t9_a = m_op.indexJoin(t6, "?x3", IndexDescription.PSOC, SO, names("?x3", "?x1"), DataField.SUBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#publicationAuthor");
				Table<String> t9 = m_op.mergeJoin(t9_a, t1, "?x1");

				Table<String> t10_a = m_op.indexJoin(t7, "?x5", IndexDescription.PSOC, SO, names("?x5", "?x4"), DataField.SUBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#memberOf");
				Table<String> t10 = m_op.mergeJoin(t10_a, t4, "?x4");

				Table<String> t11_a = m_op.indexJoin(t5, "?x7", IndexDescription.PSOC, SO, names("?x7", "?x4"), DataField.SUBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#worksFor");
				Table<String> t11 = m_op.mergeJoin(t11_a, t10, "?x4");

				Table<String> t12_a = m_op.indexJoin(t9, "?x1", IndexDescription.PSOC, SO, names("?x1", "?x4"), DataField.SUBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#memberOf");
				Table<String> t12 = m_op.mergeJoin(t12_a, t11, "?x4");

				Table<String> t13_a = m_op.indexJoin(t2, "?x6", IndexDescription.PSOC, SO, names("?x6", "?x1"), DataField.SUBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#publicationAuthor");
				Table<String> t13 = m_op.mergeJoin(t13_a, t12, "?x1");

				Table<String> t14 = m_op.indexJoin(t13, "?x6", "?x5", IndexDescription.PSOC, SO, names("?x6", "?x5"), DataField.SUBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#publicationAuthor");

				Table<String> t15 = m_op.indexJoin(t14, "?x7", IndexDescription.PSOC, SO, names("?x7", "?x2"), DataField.SUBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#undergraduateDegreeFrom");
				t15 = m_op.refineWithPrunedPart(p2, "?x2", t15);
				t15 = m_op.compact(t15, Arrays.asList("?x3", "?x4", "?x1", "?x7", "?x5", "?x6"));

				Table<String> t16 = m_op.indexJoin(t8, "?x9", IndexDescription.PSOC, SO, names("?x9", "?x8"), DataField.SUBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#teacherOf");

				Table<String> t17_a = m_op.indexJoin(t15, "?x1", IndexDescription.PSOC, SO, names("?x1", "?x8"), DataField.SUBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#takesCourse");
				Table<String> t17 = m_op.mergeJoin(t17_a, t16, "?x8");
				t17 = m_op.compact(t17, Arrays.asList("?x3", "?x4", "?x1", "?x7", "?x5", "?x6", "?x9"));

				}
				public void query_q6_spc_1() throws StorageException {
				PrunedQueryPart p1 = new PrunedQueryPart("p", null);
				p1.addEdge("?x1", "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#undergraduateDegreeFrom", "?x3");
				p1.addEdge("?x4", "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#publicationAuthor", "?x1");
				p1.addEdge("?x1", "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#advisor", "?x5");
				p1.addEdge("?x1", "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#memberOf", "?x2");
				p1.addEdge("?x1", "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#takesCourse", "?x7");
				Table<String> t1 = m_op.load(IndexDescription.POCS, SO, names("?x1", "GraduateStudent118@Department8.University0.edu"), DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#emailAddress", DataField.OBJECT, "GraduateStudent118@Department8.University0.edu");
				t1 = m_op.refineWithPrunedPart(p1, "?x1", t1);

				Table<String> t2 = m_op.load(IndexDescription.POCS, SO, names("?x6", "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#GraduateCourse"), DataField.PROPERTY, "http://www.w3.org/1999/02/22-rdf-syntax-ns#type", DataField.OBJECT, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#GraduateCourse");

				Table<String> t3_a = m_op.load(IndexDescription.POCS, SO, names("?x6", "GraduateCourse5"), DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#name", DataField.OBJECT, "GraduateCourse5");
				Table<String> t3 = m_op.mergeJoin(t3_a, t2, "?x6");
				t3 = m_op.compact(t3, Arrays.asList("?x6"));

				Table<String> t4_a = m_op.indexJoin(t1, "?x1", IndexDescription.PSOC, SO, names("?x1", "?x6"), DataField.SUBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#takesCourse");
				Table<String> t4 = m_op.mergeJoin(t4_a, t3, "?x6");

				}
				public void query_q7_spc_1() throws StorageException {
				PrunedQueryPart p1 = new PrunedQueryPart("p", null);
				p1.addEdge("?x6", "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#publicationAuthor", "?x1");
				p1.addEdge("?x7", "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#publicationAuthor", "?x1");
				p1.addEdge("?x1", "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#advisor", "?x5");
				p1.addEdge("?x1", "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#memberOf", "?x3");
				PrunedQueryPart p2 = new PrunedQueryPart("p", null);
				p2.addEdge("?x2", "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#publicationAuthor", "?x8");
				Table<String> t1 = m_op.load(IndexDescription.POCS, SO, names("?x1", "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#GraduateStudent"), DataField.PROPERTY, "http://www.w3.org/1999/02/22-rdf-syntax-ns#type", DataField.OBJECT, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#GraduateStudent");
				t1 = m_op.refineWithPrunedPart(p1, "?x1", t1);

				Table<String> t2_a = m_op.load(IndexDescription.POCS, SO, names("?x1", "GraduateStudent42@Department8.University0.edu"), DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#emailAddress", DataField.OBJECT, "GraduateStudent42@Department8.University0.edu");
				Table<String> t2 = m_op.mergeJoin(t2_a, t1, "?x1");
				t2 = m_op.compact(t2, Arrays.asList("?x1"));

				Table<String> t3 = m_op.load(IndexDescription.POCS, SO, names("?x4", "Course42"), DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#name", DataField.OBJECT, "Course42");

				Table<String> t4_a = m_op.load(IndexDescription.POCS, SO, names("?x4", "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#Course"), DataField.PROPERTY, "http://www.w3.org/1999/02/22-rdf-syntax-ns#type", DataField.OBJECT, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#Course");
				Table<String> t4 = m_op.mergeJoin(t4_a, t3, "?x4");
				t4 = m_op.compact(t4, Arrays.asList("?x4"));

				Table<String> t5_a = m_op.indexJoin(t2, "?x1", IndexDescription.PSOC, SO, names("?x1", "?x4"), DataField.SUBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#teachingAssistantOf");
				Table<String> t5 = m_op.mergeJoin(t5_a, t4, "?x4");

				Table<String> t6 = m_op.indexJoin(t5, "?x1", IndexDescription.POCS, SO, names("?x2", "?x1"), DataField.OBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#publicationAuthor");
				t6 = m_op.refineWithPrunedPart(p2, "?x2", t6);
				t6 = m_op.compact(t6, Arrays.asList("?x4", "?x1"));

				}
				public void query_q8_spc_1() throws StorageException {
				PrunedQueryPart p1 = new PrunedQueryPart("p", null);
				p1.addEdge("?x7", "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#publicationAuthor", "?x1");
				p1.addEdge("?x1", "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#takesCourse", "?x9");
				p1.addEdge("?x1", "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#takesCourse", "?x8");
				PrunedQueryPart p2 = new PrunedQueryPart("p", null);
				p2.addEdge("?x2", "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#publicationAuthor", "?x6");
				PrunedQueryPart p3 = new PrunedQueryPart("p", null);
				p3.addEdge("?x4", "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#teacherOf", "?x5");
				PrunedQueryPart p4 = new PrunedQueryPart("p", null);
				p4.addEdge("?x10", "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#headOf", "?x3");
				Table<String> t1 = m_op.load(IndexDescription.POCS, SO, names("?x1", "GraduateStudent15@Department6.University0.edu"), DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#emailAddress", DataField.OBJECT, "GraduateStudent15@Department6.University0.edu");
				t1 = m_op.refineWithPrunedPart(p1, "?x1", t1);

				Table<String> t2 = m_op.load(IndexDescription.POCS, SO, names("?x2", "Publication11"), DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#name", DataField.OBJECT, "Publication11");
				t2 = m_op.refineWithPrunedPart(p2, "?x2", t2);

				Table<String> t3 = m_op.load(IndexDescription.POCS, SO, names("?x4", "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#FullProfessor"), DataField.PROPERTY, "http://www.w3.org/1999/02/22-rdf-syntax-ns#type", DataField.OBJECT, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#FullProfessor");
				t3 = m_op.refineWithPrunedPart(p3, "?x4", t3);

				Table<String> t4_a = m_op.indexJoin(t1, "?x1", IndexDescription.PSOC, SO, names("?x1", "?x4"), DataField.SUBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#advisor");
				Table<String> t4 = m_op.mergeJoin(t4_a, t3, "?x4");

				Table<String> t5_a = m_op.indexJoin(t2, "?x2", IndexDescription.PSOC, SO, names("?x2", "?x1"), DataField.SUBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#publicationAuthor");
				Table<String> t5 = m_op.mergeJoin(t5_a, t4, "?x1");

				Table<String> t6 = m_op.indexJoin(t5, "?x1", IndexDescription.PSOC, SO, names("?x1", "?x3"), DataField.SUBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#memberOf");
				t6 = m_op.refineWithPrunedPart(p4, "?x3", t6);
				t6 = m_op.compact(t6, Arrays.asList("?x4", "?x1", "?x2"));

				}
				public void query_q9_spc_1() throws StorageException {
				PrunedQueryPart p1 = new PrunedQueryPart("p", null);
				p1.addEdge("?x15", "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#publicationAuthor", "?x5");
				PrunedQueryPart p2 = new PrunedQueryPart("p", null);
				p2.addEdge("?x14", "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#publicationAuthor", "?x11");
				Table<String> t1 = m_op.load(IndexDescription.POCS, SO, names("?x1", "Lecturer5"), DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#name", DataField.OBJECT, "Lecturer5");

				Table<String> t2 = m_op.load(IndexDescription.POCS, SO, names("?x3", "GraduateCourse47"), DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#name", DataField.OBJECT, "GraduateCourse47");

				Table<String> t3 = m_op.load(IndexDescription.POCS, SO, names("?x10", "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#Publication"), DataField.PROPERTY, "http://www.w3.org/1999/02/22-rdf-syntax-ns#type", DataField.OBJECT, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#Publication");

				Table<String> t4 = m_op.load(IndexDescription.POCS, SO, names("?x7", "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#University"), DataField.PROPERTY, "http://www.w3.org/1999/02/22-rdf-syntax-ns#type", DataField.OBJECT, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#University");

				Table<String> t5 = m_op.load(IndexDescription.POCS, SO, names("?x2", "GraduateStudent29"), DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#name", DataField.OBJECT, "GraduateStudent29");

				Table<String> t6 = m_op.load(IndexDescription.POCS, SO, names("?x12", "AssistantProfessor4"), DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#name", DataField.OBJECT, "AssistantProfessor4");

				Table<String> t7_a = m_op.indexJoin(t1, "?x1", IndexDescription.PSOC, SO, names("?x1", "?x7"), DataField.SUBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#doctoralDegreeFrom");
				Table<String> t7 = m_op.mergeJoin(t7_a, t4, "?x7");

				Table<String> t8_a = m_op.indexJoin(t3, "?x10", IndexDescription.PSOC, SO, names("?x10", "?x2"), DataField.SUBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#publicationAuthor");
				Table<String> t8 = m_op.mergeJoin(t8_a, t5, "?x2");

				Table<String> t9_a = m_op.indexJoin(t7, "?x1", IndexDescription.PSOC, SO, names("?x1", "?x3"), DataField.SUBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#teacherOf");
				Table<String> t9 = m_op.mergeJoin(t9_a, t2, "?x3");

				Table<String> t10_a = m_op.indexJoin(t8, "?x2", IndexDescription.PSOC, SO, names("?x2", "?x7"), DataField.SUBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#undergraduateDegreeFrom");
				Table<String> t10 = m_op.mergeJoin(t10_a, t9, "?x7");

				Table<String> t11 = m_op.indexJoin(t6, "?x12", IndexDescription.PSOC, SO, names("?x12", "?x9"), DataField.SUBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#mastersDegreeFrom");

				Table<String> t12 = m_op.indexJoin(t11, "?x12", IndexDescription.POCS, SO, names("?x5", "?x12"), DataField.OBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#advisor");
				t12 = m_op.refineWithPrunedPart(p1, "?x5", t12);
				t12 = m_op.compact(t12, Arrays.asList("?x12", "?x9"));

				Table<String> t13 = m_op.indexJoin(t10, "?x3", IndexDescription.POCS, SO, names("?x6", "?x3"), DataField.OBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#takesCourse");

				Table<String> t14_a = m_op.indexJoin(t13, "?x1", IndexDescription.PSOC, SO, names("?x1", "?x9"), DataField.SUBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#mastersDegreeFrom");
				Table<String> t14 = m_op.mergeJoin(t14_a, t12, "?x9");
				t14 = m_op.compact(t14, Arrays.asList("?x3", "?x1", "?x2", "?x7", "?x6", "?x12", "?x10"));

				Table<String> t15 = m_op.indexJoin(t14, "?x6", IndexDescription.POCS, SO, names("?x8", "?x6"), DataField.OBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#publicationAuthor");
				t15 = m_op.compact(t15, Arrays.asList("?x3", "?x1", "?x2", "?x7", "?x8", "?x12", "?x10"));

				Table<String> t16 = m_op.indexJoin(t15, "?x8", IndexDescription.PSOC, SO, names("?x8", "?x4"), DataField.SUBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#publicationAuthor");
				t16 = m_op.compact(t16, Arrays.asList("?x3", "?x4", "?x1", "?x2", "?x7", "?x12", "?x10"));

				Table<String> t17 = m_op.indexJoin(t16, "?x4", IndexDescription.PSOC, SO, names("?x4", "?x13"), DataField.SUBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#worksFor");
				t17 = m_op.compact(t17, Arrays.asList("?x3", "?x1", "?x2", "?x7", "?x12", "?x13", "?x10"));

				Table<String> t18 = m_op.indexJoin(t17, "?x13", IndexDescription.POCS, SO, names("?x11", "?x13"), DataField.OBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#worksFor");
				t18 = m_op.refineWithPrunedPart(p2, "?x11", t18);
				t18 = m_op.compact(t18, Arrays.asList("?x3", "?x1", "?x2", "?x7", "?x12", "?x10"));

				}
				public void query_q10_spc_1() throws StorageException {
				PrunedQueryPart p1 = new PrunedQueryPart("p", null);
				p1.addEdge("?x8", "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#takesCourse", "?x15");
				PrunedQueryPart p2 = new PrunedQueryPart("p", null);
				p2.addEdge("?x6", "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#doctoralDegreeFrom", "?x16");
				PrunedQueryPart p3 = new PrunedQueryPart("p", null);
				p3.addEdge("?x14", "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#publicationAuthor", "?x9");
				Table<String> t1 = m_op.load(IndexDescription.POCS, SO, names("?x1", "GraduateStudent11@Department7.University0.edu"), DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#emailAddress", DataField.OBJECT, "GraduateStudent11@Department7.University0.edu");

				Table<String> t2 = m_op.load(IndexDescription.POCS, SO, names("?x13", "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#AssistantProfessor"), DataField.PROPERTY, "http://www.w3.org/1999/02/22-rdf-syntax-ns#type", DataField.OBJECT, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#AssistantProfessor");

				Table<String> t3 = m_op.load(IndexDescription.POCS, SO, names("?x5", "Department7"), DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#name", DataField.OBJECT, "Department7");

				Table<String> t4 = m_op.load(IndexDescription.POCS, SO, names("?x7", "AssociateProfessor6"), DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#name", DataField.OBJECT, "AssociateProfessor6");

				Table<String> t5_a = m_op.indexJoin(t1, "?x1", IndexDescription.PSOC, SO, names("?x1", "?x5"), DataField.SUBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#memberOf");
				Table<String> t5 = m_op.mergeJoin(t5_a, t3, "?x5");

				Table<String> t6 = m_op.indexJoin(t5, "?x1", IndexDescription.PSOC, SO, names("?x1", "?x10"), DataField.SUBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#advisor");

				Table<String> t7 = m_op.indexJoin(t2, "?x13", IndexDescription.PSOC, SO, names("?x13", "?x11"), DataField.SUBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#teacherOf");

				Table<String> t8 = m_op.indexJoin(t4, "?x7", IndexDescription.POCS, SO, names("?x8", "?x7"), DataField.OBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#advisor");
				t8 = m_op.refineWithPrunedPart(p1, "?x8", t8);
				t8 = m_op.compact(t8, Arrays.asList("?x7"));

				Table<String> t9 = m_op.indexJoin(t6, "?x1", IndexDescription.POCS, SO, names("?x12", "?x1"), DataField.OBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#publicationAuthor");

				Table<String> t10 = m_op.indexJoin(t8, "?x7", IndexDescription.POCS, SO, names("?x3", "?x7"), DataField.OBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#publicationAuthor");

				Table<String> t11_a = m_op.indexJoin(t9, "?x1", IndexDescription.PSOC, SO, names("?x1", "?x11"), DataField.SUBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#teachingAssistantOf");
				Table<String> t11 = m_op.mergeJoin(t11_a, t7, "?x11");
				t11 = m_op.compact(t11, Arrays.asList("?x1", "?x5", "?x12", "?x13", "?x10"));

				Table<String> t12 = m_op.indexJoin(t11, "?x5", IndexDescription.PSOC, SO, names("?x5", "?x2"), DataField.SUBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#subOrganizationOf");

				Table<String> t13 = m_op.indexJoin(t12, "?x10", "?x5", IndexDescription.PSOC, SO, names("?x10", "?x5"), DataField.SUBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#worksFor");
				t13 = m_op.compact(t13, Arrays.asList("?x1", "?x2", "?x5", "?x12", "?x13"));

				Table<String> t14_a = m_op.indexJoin(t10, "?x3", IndexDescription.PSOC, SO, names("?x3", "?x1"), DataField.SUBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#publicationAuthor");
				Table<String> t14 = m_op.mergeJoin(t14_a, t13, "?x1");
				t14 = m_op.compact(t14, Arrays.asList("?x1", "?x2", "?x7", "?x5", "?x12", "?x13"));

				Table<String> t15 = m_op.indexJoin(t14, "?x12", IndexDescription.PSOC, SO, names("?x12", "?x4"), DataField.SUBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#publicationAuthor");
				t15 = m_op.compact(t15, Arrays.asList("?x4", "?x1", "?x2", "?x7", "?x5", "?x13"));

				Table<String> t16 = m_op.indexJoin(t15, "?x2", IndexDescription.POCS, SO, names("?x9", "?x2"), DataField.OBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#mastersDegreeFrom");
				t16 = m_op.refineWithPrunedPart(p3, "?x9", t16);
				t16 = m_op.compact(t16, Arrays.asList("?x4", "?x1", "?x7", "?x5", "?x13"));

				Table<String> t17 = m_op.indexJoin(t16, "?x4", IndexDescription.PSOC, SO, names("?x4", "?x6"), DataField.SUBJECT, DataField.PROPERTY, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#advisor");
				t17 = m_op.refineWithPrunedPart(p2, "?x6", t17);
				t17 = m_op.compact(t17, Arrays.asList("?x1", "?x7", "?x5", "?x13"));

				}
				}
