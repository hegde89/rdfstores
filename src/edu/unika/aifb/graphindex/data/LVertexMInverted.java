package edu.unika.aifb.graphindex.data;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class LVertexMInverted extends AbstractVertex {

	private class Preimage {
		public long label;
		public List<IVertex> preimage;
		
		public Preimage(long label) {
			this.label = label;
			this.preimage = new ArrayList<IVertex>();
		}
	}
	
	private List<Preimage> m_preimages;
	
	public LVertexMInverted(long id) {
		super(id);
		m_preimages = new ArrayList<Preimage>();
	}
	
	public Set<Long> getEdgeLabels() {
		Set<Long> labels = new HashSet<Long>();
		for (Preimage i : m_preimages)
			labels.add(i.label);
		return labels;
	}

	private Preimage getImageObject(long label) {
		for (Preimage i : m_preimages) {
			if (i.label == label)
				return i;
		}
		return null;
	}
	
	public Map<Long,List<IVertex>> getImage() {
//		return m_image;
	
		Map<Long,List<IVertex>> image = new HashMap<Long,List<IVertex>>();
		for (Preimage i : m_preimages)
			image.put(i.label, i.preimage);
		return image;
	}
	
	public List<IVertex> getImage(long label) {
		Preimage i = getImageObject(label);
		if (i == null)
			return null;
		return i.preimage;
	}
	
	public void addToImage(long label, IVertex v) {
//		List<IVertex> image = m_image.get(label);
//		if (image == null) {
//			image = new ArrayList<IVertex>();
//			m_image.put(label, image);
//		}
		Preimage i = getImageObject(label);
		if (i == null) {
			i = new Preimage(label);
			m_preimages.add(i);
		}
		
		if (!i.preimage.contains(v))
			i.preimage.add(v);
	}
	
	public void setImage(long label, List<IVertex> image) {
		Preimage i = getImageObject(label);
		if (i == null) {
			i = new Preimage(label);
			m_preimages.add(i);
		}
		i.preimage = image;
	}
}
