package be.ae.hdp.mr.examples.common;

public class Utils {
	public static String getUniqueOutputFolder(String name){
		return name + "-" + System.currentTimeMillis();
	}
}
