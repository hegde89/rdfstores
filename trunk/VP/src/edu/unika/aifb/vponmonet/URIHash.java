package edu.unika.aifb.vponmonet;

import java.math.BigInteger;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

public class URIHash {
	private static MessageDigest m_md;
	
	static {
		try {
			m_md = MessageDigest.getInstance("MD5");
		} catch (NoSuchAlgorithmException e) {
			e.printStackTrace();
		}
	}
	
	private URIHash() {
	}
	
	synchronized public static long hash(String uri) {
		byte[] hash = m_md.digest(uri.getBytes());
		return new BigInteger(new byte[] {hash[14], hash[12], hash[10], hash[8], hash[6], hash[4], hash[2], hash[0]}).longValue();
	}
}
