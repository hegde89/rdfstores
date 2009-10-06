package edu.unika.aifb.ease;

import org.apache.log4j.Logger;

import edu.unika.aifb.ease.db.DBConfig;
import edu.unika.aifb.ease.index.DBIndexService;

public class Preprocessor {
	
	private static final Logger log = Logger.getLogger(Preprocessor.class);
	
	public static void main(String[] args) throws Exception {

//		if (args.length != 1) {
//			System.out.println("java Preprocessor configFilePath(String)");
//			return;
//		}
		long start = System.currentTimeMillis();

		DBConfig.setConfigFilePath("./res/config/config.cfg");
		DBConfig config = DBConfig.getConfig();

		DBIndexService indexBuilder = new DBIndexService(config);
//		indexBuilder.createTripleTable();
//		indexBuilder.createDatasourceTable();
//		indexBuilder.createSchemaTable();
//		indexBuilder.createEntityTable();
//		indexBuilder.createEntityRelationTable();
		indexBuilder.createKeywordEntityInclusionTable();
		int maxDistance = config.getMaxDistance();
//		for(int i = 2; i <= maxDistance; i++) {
//			indexBuilder.createEntityRelationTable(i);
//		}
//		indexBuilder.createRRadiusGraphCenterTable();
//		indexBuilder.findMaxRRadiusGraphCenter();
//		indexBuilder.createMaxRRadiusGraphTable();
//		indexBuilder.createKeywordGraphInclusionTable();
		indexBuilder.close();

		long end = System.currentTimeMillis();
		System.out.println("Time customing: " + (double)(end - start)/(double)1000 + "(sec)");
		System.out.println("Time customing: " + (double)(end - start)/(double)60000 + "(min)");
	}	
}
