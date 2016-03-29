package usace.army.mil.erdc.pivots;

import java.io.UnsupportedEncodingException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

public class HashingTest {
	public static void main(String [] args) throws UnsupportedEncodingException, NoSuchAlgorithmException{
		String pivotString = "pivot_0";
		String pivotString2 = "pivot_1";
		System.out.println("Original string: " + pivotString);
		System.out.println("New string 1: " + pivotString.getBytes("UTF-8" ).toString());
		System.out.println("New string 2: " + pivotString2.getBytes("UTF-8" ));
		
		MessageDigest digest = MessageDigest.getInstance("SHA-256");
		digest.update(pivotString.getBytes("UTF-8"));
		byte[] hash = digest.digest();
		System.out.println("New string 1 : " + hash);
		
		MessageDigest digest2 = MessageDigest.getInstance("SHA-256");
		digest.update(pivotString2.getBytes("UTF-8"));
		byte[] hash2 = digest.digest();
		System.out.println("New string 2 : " + hash2);
		
		String pivot = "pivot_0";
		String point = "point_0";
		String hashedRow1 = new StringBuilder()
		.append(pivot)
		.append("_")
		.append(point).toString().getBytes("UTF-8").toString();
		System.out.println(hashedRow1);
	}
}
