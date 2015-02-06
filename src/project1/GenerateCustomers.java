package project1;

import java.io.File;
import java.io.FileOutputStream;
import java.io.OutputStreamWriter;
import java.util.Random;

public class GenerateCustomers {
	public String getName(Random random) {
		int minLength = 10;
		int maxLength = 20;
		String letters = "abcdefghijklmnopqrstuvwxyz";
	    StringBuffer sb = new StringBuffer();
	    int length = random.nextInt(maxLength) % (maxLength - minLength + 1) + minLength;
		for (int i = 0; i < length; i++) {
	        int number = random.nextInt(letters.length());
	        sb.append(letters.charAt(number));
	    }
	    return sb.toString();
	}

	public String getAge(Random random) {
		int minAge = 10;
		int maxAge = 70;
		int randomAge = random.nextInt(maxAge) % (maxAge - minAge + 1) + minAge;
		return String.valueOf(randomAge);
	}

	public String getCountryCode(Random random) {
		int minCountrycode = 1;
		int maxCountryCode = 10;
		int randomCountryCode = random.nextInt(maxCountryCode) % (maxCountryCode - minCountrycode + 1) + minCountrycode;
		return String.valueOf(randomCountryCode);
	}

	public String getSalary(Random random) {
		float minSalary = 100;
		float maxSalary = 10000;
		float randomSalary = random.nextFloat() * (maxSalary - minSalary) + minSalary;
		return String.valueOf(randomSalary);
	}

	public static void main(String[] args) {
		final int maxLine = 50000;

		File customers = new File("customers.txt");
		FileOutputStream fosCustomers = null;
		OutputStreamWriter outCustomers = null;

		GenerateCustomers generateCustomers = new GenerateCustomers();
		Random random = new Random();

		String line;
		String id;
		String name;
		String age;
		String countryCode;
		String salary;

		try {
			fosCustomers = new FileOutputStream(customers);
			outCustomers = new OutputStreamWriter(fosCustomers, "US-ASCII");
			for (int i = 0; i < maxLine; i++) {
				id = String.valueOf(i + 1);
				name = generateCustomers.getName(random);
				age = generateCustomers.getAge(random);
				countryCode = generateCustomers.getCountryCode(random);
				salary = generateCustomers.getSalary(random);
				line = new String(id + "," + name + "," + age + ","
						+ countryCode + "," + salary);
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
