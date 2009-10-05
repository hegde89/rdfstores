import edu.unika.aifb.ease.db.DBConfig;
import edu.unika.aifb.ease.db.DBService;


public class TestConfig {
	public static void main(String[] args) {
		DBConfig.setConfigFilePath("./res/config/config.cfg");
		DBConfig config = DBConfig.getConfig();
		
		String server = config.getDbServer();
		String username = config.getDbUsername();
		String password = config.getDbPassword();
		String port = config.getDbPort();
		String dbName = config.getDbName();
		DBService service = new DBService(server, username, password, port, dbName, true);
		
	} 
}
