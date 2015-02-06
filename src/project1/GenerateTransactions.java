package project1;

import java.io.File;
import java.io.FileOutputStream;
import java.io.OutputStreamWriter;
import java.util.Random;

public class GenerateTransactions {
	public String getCustId(Random random) {
		int minCustId = 1;
		int maxCustId = 50000;
		int randomAge = random.nextInt(maxCustId) % (maxCustId - minCustId + 1) + minCustId;
		return String.valueOf(randomAge);
	}

	public String getTransTotal(Random random) {
		float minTransTotal = 10;
		float maxTransTotal = 1000;
		float randomTransTotal = random.nextFloat() * (maxTransTotal - minTransTotal) + minTransTotal;
		return String.valueOf(randomTransTotal);
	}

	public String getTransNumItems(Random random) {
		int minTransNumItems = 1;
		int maxTransNumItems = 10;
		int randomCountryCode = random.nextInt(maxTransNumItems) % (maxTransNumItems - minTransNumItems + 1) + minTransNumItems;
		return String.valueOf(randomCountryCode);
	}
	
	public String getTransDesc(Random random) {
		int minLength = 20;
		int maxLength = 50;
		String letters = "abcdefghijklmnopqrstuvwxyz";
	    StringBuffer sb = new StringBuffer();
	    int length = random.nextInt(maxLength) % (maxLength - minLength + 1) + minLength;
		for (int i = 0; i < length; i++) {
	        int number = random.nextInt(letters.length());
	        sb.append(letters.charAt(number));
	    }
	    return sb.toString();
	}


	public static void main(String[] args) {
		final int maxLine = 5000000;

		File customers = new File("transactions.txt");
		FileOutputStream fosCustomers = null;
		OutputStreamWriter outCustomers = null;

		GenerateTransactions generateTransactions = new GenerateTransactions();
		Random random = new Random();

		String line;
		String transId;
		String custId;
		String transTotal;
		String transNumItems;
		String transDesc;

		try {
			fosCustomers = new FileOutputStream(customers);
			outCustomers = new OutputStreamWriter(fosCustomers, "US-ASCII");
			for (int i = 0; i < maxLine; i++) {
				transId = String.valueOf(i + 1);
				custId = generateTransactions.getCustId(random);
				transTotal = generateTransactions.getTransTotal(random);
				transNumItems = generateTransactions.getTransNumItems(random);
				transDesc = generateTransactions.getTransDesc(random);
				line = new String(transId + "," + custId + "," + transTotal + ","
						+ transNumItems + "," + transDesc);
				outCustomers.write(line + "\r\n");
			}
			outCustomers.close();
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			try {
				outCustomers.close();
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}
}
