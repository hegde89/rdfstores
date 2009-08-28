package edu.unika.aifb.atwrank;
import edu.unika.aifb.graphindex.Demo;


public class Test {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		System.out.println("test");

		Demo demo = new Demo();
		String[] arg = {"-a", "test", "-o", "/Users/dmh/Documents/aifb/research/graphindex/data/index"};
		try {
			demo.main(arg);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		/*WikiReadTest wikitest = new WikiReadTest();
		try {
			System.out.println(wikitest.readTest());
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}*/
	
	}

}