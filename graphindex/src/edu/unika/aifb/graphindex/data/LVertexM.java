package edu.unika.aifb.graphindex.data;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class LVertexM extends AbstractVertex {

	private class Info {
		public long label;
		public int ival;
		public int sval;
		
		public Info(long label, int ival, int sval) {
			this.label = label;
			this.ival = ival;
			this.sval = sval;
		}
	}
	
	private class Image {
		public long label;
		public List<IVertex> image;
		
		public Image(long label) {
			this.label = label;
			this.image = new ArrayList<IVertex>();
		}
	}
	
	private List<Info> m_infos;
//	private Map<Long,List<IVertex>> m_image;
	private List<Image> m_images;
	
	public LVertexM(long id) {
		super(id);
		m_infos = new ArrayList<Info>();
//		m_image = new HashMap<Long,List<IVertex>>();
		m_images = new ArrayList<Image>();
	}
	
	public Set<Long> getEdgeLabels() {
		Set<Long> labels = new HashSet<Long>();
		for (Image i : m_images)
			labels.add(i.label);
		return labels;
	}

	private Image getImageObject(long label) {
		for (Image i : m_images) {
			if (i.label == label)
				return i;
		}
		return null;
	}
	
	public Map<Long,List<IVertex>> getImage() {
//		return m_image;
	
		Map<Long,List<IVertex>> image = new HashMap<Long,List<IVertex>>();
		for (Image i : m_images)
			image.put(i.label, i.image);
		return image;
	}
	
	public List<IVertex> getImage(long label) {
		Image i = getImageObject(label);
		if (i == null)
			return null;
		return i.image;
	}
	
	public void addToImage(long label, IVertex v) {
//		List<IVertex> image = m_image.get(label);
//		if (image == null) {
//			image = new ArrayList<IVertex>();
//			m_image.put(label, image);
//		}
		Image i = getImageObject(label);
		if (i == null) {
			i = new Image(label);
			m_images.add(i);
		}
		
		if (!i.image.contains(v))
			i.image.add(v);
	}
	
	public void setImage(long label, List<IVertex> image) {
		Image i = getImageObject(label);
		if (i == null) {
			i = new Image(label);
			m_images.add(i);
		}
		i.image = image;
	}

	public void clearInfo() {
		m_infos = new ArrayList<Info>();
	}
	
	private Info getInfoObject(long label) {
		for (Info i : m_infos) {
			if (i.label == label)
				return i;
		}
		Info i = new Info(label, 0, 0);
		m_infos.add(i);
		return i;
	}

	public int getInfo(long label) {
		return getInfoObject(label).ival;
	}

	public int getSInfo(long label) {
		return getInfoObject(label).sval;
	}

	public void incInfo(long label) {
		getInfoObject(label).ival++;
	}

	public void incSInfo(long label) {
		getInfoObject(label).sval++;
	}
	
	public void decSInfo(long label) {
		getInfoObject(label).sval--;
	}

	public void setInfo(long label, int val) {
		getInfoObject(label).ival = val;
	}

	public void setSInfo(long label, int val) {
		getInfoObject(label).sval = val;
	}
}
