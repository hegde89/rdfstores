package org.apexlab.service.session.semplore;

//import org.apexlab.service.session.datastructure.Concept;
//
//import com.ibm.semplore.btc.Graph;
//import com.ibm.semplore.model.Category;
//import com.ibm.semplore.model.CompoundCategory;
//import com.ibm.semplore.model.impl.SchemaFactoryImpl;
//
//public class ConceptOperation implements FacetOperation {
//
//	private Concept concept;
//	
//	public ConceptOperation(Concept c) {
//		concept = c;
//	}
//	
//	@Override
//	public Graph applyTo(Graph graph) {
//		try {
//			Category c = SchemaFactoryImpl.getInstance().createCategory((concept.getURI()));
//			CompoundCategory cc = (CompoundCategory)graph.getNode(graph.getTargetVariable());
//			cc.addComponentCategory(c);
//			return graph;
//		} catch (Exception e) {
//			e.printStackTrace();
//		}
//		return graph;
//	}
//
//	@Override
//	public Graph undo(Graph graph) {
//		try {
//			Category c = SchemaFactoryImpl.getInstance().createCategory((concept.getURI()));
//			CompoundCategory cc = (CompoundCategory)graph.getNode(graph.getTargetVariable());
//			cc.removeComponentCategory(c);
//			return graph;
//		} catch (Exception e) {
//			e.printStackTrace();
//		}
//		return graph;
//	}
//
//}
