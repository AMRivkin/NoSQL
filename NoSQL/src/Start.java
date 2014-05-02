
import java.io.File;
import java.io.IOException;

import java.util.Collection;

import org.apache.commons.io.FileUtils;



public class Start {

	/**
	 * @param args
	 * @throws IOException 
	 * @throws ClassNotFoundException 
	 */
	public static void main(String[] args) throws IOException, ClassNotFoundException {
		
		String[] hosts = {"hdp-test1.distr.fors.ru:5000","hdp-test3.distr.fors.ru:5000","hdp-test5.distr.fors.ru:5000"};
		String storeName = "mystore";
		
		NoSQL GRIBstore = new NoSQL(hosts, storeName);
		boolean success = GRIBstore.Connect();
	
		
		
		if (success)
		{
		
			Collection<File> files = FileUtils.listFiles(new File(args[0]), null, true);		
			
			
			for (File file : files) {
				GRIBstore.putFile("GRIB/"+file.getParentFile().getName(), file.getName(), file.toPath());
			}
			
			//GRIBstore.deleteAll();
			//GRIBstore.downloadAll("C:\\temp\\");

		}
		
		
		
		
		
	}

	
	
}
