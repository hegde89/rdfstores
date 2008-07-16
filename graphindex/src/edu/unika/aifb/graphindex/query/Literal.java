package edu.unika.aifb.graphindex.query;

public class Literal {
	private Predicate m_predicate;
	private Term m_subject, m_object;
	
	public Literal(Predicate m_predicate, Term m_subject, Term m_object) {
		this.m_predicate = m_predicate;
		this.m_subject = m_subject;
		this.m_object = m_object;
	}

	public Predicate getPredicate() {
		return m_predicate;
	}
	
	public Term getSubject() {
		return m_subject;
	}
	
	public Term getObject() {
		return m_object;
	}
}
