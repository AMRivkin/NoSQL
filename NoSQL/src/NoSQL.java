
import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.commons.io.FileUtils;

import oracle.kv.Direction;
import oracle.kv.FaultException;
import oracle.kv.KVStore;
import oracle.kv.KVStoreConfig;
import oracle.kv.KVStoreFactory;
import oracle.kv.Key;
import oracle.kv.KeyValueVersion;
import oracle.kv.Value;

public class NoSQL {
	
	private String[] hosts;
	private String storeName;
	private KVStoreConfig kconfig;
	private KVStore kvstore;
	@SuppressWarnings("unused")
	private Charset defaultCharset = Charset.forName("UTF-8");
	
	
	public NoSQL(String[] hosts, String storeName) {
		
		this.hosts = hosts;
		this.storeName = storeName;
	}

	public boolean Connect()
	{
		if (kvstore != null) return true;
		
		try
		{
		kconfig= new KVStoreConfig(storeName, hosts);		
		kvstore = KVStoreFactory.getStore(kconfig);	
		return true;
		}
		catch (FaultException fe)
		{
			fe.printStackTrace();
			return false;
		}
		catch (IllegalArgumentException iae) {
			iae.printStackTrace();
			return false;
		}
		
		
		
	}
	
	public String[] getHhosts() {
		return hosts;
	}

	public void setHhosts(String[] hhosts) {
		this.hosts = hhosts;
	}

	public String getStoreName() {
		return storeName;
	}

	public void setStoreName(String storeName) {
		this.storeName = storeName;
	}

	public KVStore getKvstore() {
		return kvstore;
	}

	public void setKvstore(KVStore kvstore) {
		this.kvstore = kvstore;
	}





	public boolean putFile(String major,String minor,Path file)
	{
		
		List<String> majorPart = new ArrayList<String>();
		List<String> minorPart = new ArrayList<String>();
	
		for (String element : major.split("/")) {
			majorPart.add(element);
		}
		for (String element : minor.split("/")) {
			minorPart.add(element);
		}
		
		
		Key myKey = Key.createKey(majorPart, minorPart);
		byte[] data;
		try {
			data = Files.readAllBytes(file);
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return false;
		}
		Value myValue = Value.createValue(data);
		try
		{
			kvstore.put(myKey, myValue);
			System.out.println("File added: "+ myKey.toString());
		}
		catch (FaultException fe)
		{
			fe.printStackTrace();
			return false;
		}
		catch (IllegalArgumentException iae) {
			iae.printStackTrace();
			return false;
		}
			return true;
		}
	
	public boolean getFile(String major,String minor,File file)
	{
			
		List<String> majorPart = new ArrayList<String>();
		List<String> minorPart = new ArrayList<String>();
	
		for (String element : major.split("/")) {
			majorPart.add(element);
		}
		for (String element : minor.split("/")) {
			minorPart.add(element);
		}
				
		Key myKey = Key.createKey(majorPart, minorPart);
		try {
			FileUtils.writeByteArrayToFile(file, kvstore.get(myKey).getValue().getValue());
		
		} catch (FaultException | IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return false;
		}
		return true;
		
		}
	
	public boolean putValue(String user,List<String> path, byte[] value)
	{
		Key myKey = Key.createKey(user,path);
		Value myValue = Value.createValue(value);
		try
		{
			kvstore.put(myKey, myValue);
		}
		catch (FaultException fe)
		{
			fe.printStackTrace();
			return false;
		}
		catch (IllegalArgumentException iae) {
			iae.printStackTrace();
			return false;
		}
			return true;
		
	}
	
	@SuppressWarnings("unused")
	private byte[] getValue(String user,List<String> path)
	{
		Key myKey = Key.createKey(user,path);			
		return	kvstore.get(myKey).getValue().getValue();
				
	}
	
	
	public void deleteAll()
	{
		Iterator<Key> iterator = kvstore.storeKeysIterator(Direction.UNORDERED, 100);
		while (iterator.hasNext()) 
		 {
			 Key key = iterator.next(); 
			 if (kvstore.delete(key))
			 System.out.println("Deleted: "+key.toString());
		 }
	
	}

	public void downloadAll(String path)
	{
		
		Iterator<KeyValueVersion> iterator = kvstore.storeIterator(Direction.UNORDERED, 100);
		 while (iterator.hasNext()) 
		 {
			 KeyValueVersion element = iterator.next(); 
			 Key key = element.getKey();
			 Value value = element.getValue();
			 
			File file =  new File(path+key.toString().replace("-/", ""));
			try {
				FileUtils.writeByteArrayToFile(file, value.toByteArray());
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			 
		    }			
		}
		
	
	
}

	
	


