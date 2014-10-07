package be.ae.hdp.mr.examples.common;

public class Utils {
	private static long currentTime = System.currentTimeMillis();
	
	public static String getUniqueRootFolder(String appName, String folderName){
		return appName + "-" + folderName + currentTime;
	}
	
	public static String getUniqueOutputFolder(String appName, String folderName){
		return getUniqueRootFolder(appName, folderName) + "/output";
	}
	
	public static String getUniqueLogFolder(String appName, String folderName){
		return getUniqueRootFolder(appName, folderName) + "/logs";
	}
	
	@Deprecated
	public static String getUniqueOutputFolder(String name){
		return name + "-" + currentTime;
	}
}
