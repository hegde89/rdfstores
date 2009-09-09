package edu.unika.aifb.graphindex.evaluation;

import java.util.Iterator;
import java.util.Random;

import org.semanticweb.yars.nx.Node;
import org.semanticweb.yars.nx.Resource;

public class ErdosRenyi implements Iterator<Node[]>{
	int _n, _m;
	float _p;
	Random _r;
	int _i=0, _j=1;
	
	long _size=0;
	
	Node[] _cur;
	
	static String NODE_PREFIX = "http://example.org/node#";
	static String LINK_PREFIX = "http://example.org/link#";
	static String SOURCE_PREFIX = "http://example.org/source#";
	
	public ErdosRenyi(int n, int m, float p) {
		_n = n;
		_p = p;
		
		_m = m;
		
		// use the same seed to generate the same sequence of numbers
		_r = new Random(1);

		getNext();
	}
	
	void getNext() {
		boolean hit = false;
		
		while (hit == false) {
			if (_r.nextFloat() < _p) {
				_cur = new Node[4];
				_cur[0] = new Resource(NODE_PREFIX+_i);
				_cur[1] = new Resource(LINK_PREFIX + _r.nextInt(_m));
				_cur[2] = new Resource(NODE_PREFIX+_j);
				_cur[3] = new Resource(SOURCE_PREFIX);
				
				hit = true;
			}
			
			if (_j < (_n-1)) {
				_j++;
			} else if (_i < (_n-1)) {
				_i++;
				_j = _i+1;
			} else {
				_cur = null;
				return;
			}
		}
	}
	
	public boolean hasNext() {
		if (_cur != null) {
			return true;
		}
		
		return false;
	}
	
	public Node[] next() {
		Node[] result = new Node[4];
		
		System.arraycopy(_cur, 0, result, 0, 4);
		
		getNext();
		
		_size++;
		
		return result;
	}

	public long size() {
		return _size;
	}
	
	public void remove() {
		throw new UnsupportedOperationException("remove not possible");
	}
}