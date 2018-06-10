package surfstore;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Base64;

public final class HashUtils {
	
	public static String sha256(String s) {
		MessageDigest digest = null;

		try {
			digest = MessageDigest.getInstance("SHA-256");
		} catch (NoSuchAlgorithmException e) {
			throw new RuntimeException(e);
		}

		byte[] hash = digest.digest(s.getBytes(StandardCharsets.UTF_8));
		String encoded = Base64.getEncoder().encodeToString(hash);

		return encoded;
	}

	public static String sha256(byte[] b) {
		MessageDigest digest = null;

		try {
			digest = MessageDigest.getInstance("SHA-256");
		} catch (NoSuchAlgorithmException e) {
			throw new RuntimeException(e);
		}

		byte[] hash = digest.digest(b);
		String encoded = Base64.getEncoder().encodeToString(hash);
		return encoded;
	}

	
	public static void main(String[] args) {
		if (args.length != 1) {
			System.err.println("Usage: ShaTest <string>");
			System.exit(1);
		}

        String text = args[0];
        System.out.println("Input: " + text);

        MessageDigest digest = null;
		try {
			digest = MessageDigest.getInstance("SHA-256");
		} catch (NoSuchAlgorithmException e) {
			e.printStackTrace();
			System.exit(2);
		}
        byte[] hash = digest.digest(text.getBytes(StandardCharsets.UTF_8));
        String encoded = Base64.getEncoder().encodeToString(hash);

        System.out.println("Output: " + encoded);
	}
	
}
