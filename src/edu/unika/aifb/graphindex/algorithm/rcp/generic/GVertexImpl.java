package edu.unika.aifb.graphindex.algorithm.rcp.generic;

import java.util.ArrayList;
import java.util.List;

public class GVertexImpl<V,E> extends AbstractGVertex<V,E> {
	private class Image {
		public E label;
		public List<GVertex<V,E>> image;
		
		public Image(E label) {
			this.label = label;
			this.image = new ArrayList<GVertex<V,E>>();
		}
	}
	
	private List<Image> m_images;
	
	public GVertexImpl(V label) {
		super(label);
		m_images = new ArrayList<Image>();
	}
	
	private Image getImageObject(E label) {
		for (Image i : m_images) {
			if (i.label.equals(label))
				return i;
		}
		return null;
	}
	public List<GVertex<V,E>> getImage(E label) {
		Image i = getImageObject(label);
		if (i == null)
			return null;
		return i.image;
	}
	
	public void setImage(E label, List<GVertex<V,E>> image) {
		m_edgeLabels.add(label);
		Image i = getImageObject(label);
		if (i == null) {
			i = new Image(label);
			m_images.add(i);
		}
		i.image = image;
	}

	public void addToImage(E label, GVertex<V,E> vertex) {
		m_edgeLabels.add(label);
		Image i = getImageObject(label);
		if (i == null) {
			i = new Image(label);
			m_images.add(i);
		}
		i.image.add(vertex);
	}
}
