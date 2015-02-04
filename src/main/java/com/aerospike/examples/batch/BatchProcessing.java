package com.aerospike.examples.batch;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.PosixParser;
import org.apache.log4j.Logger;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.AerospikeException;
import com.aerospike.client.Bin;
import com.aerospike.client.Key;
import com.aerospike.client.Record;
import com.aerospike.client.ScanCallback;


/**
@author Peter Milne
*/
public class BatchProcessing {
	private AerospikeClient client;
	private String seedHost;
	private int port;
	private String namespace;
	private String set;
	
	private static final int BATCH_SIZE = 1000;

	private static Logger log = Logger.getLogger(BatchProcessing.class);
	public BatchProcessing(String host, int port, String namespace, String set) throws AerospikeException {
		this.client = new AerospikeClient(host, port);
		this.seedHost = host;
		this.port = port;
		this.namespace = namespace;
		this.set = set;
	}
	public BatchProcessing(AerospikeClient client, String namespace, String set) throws AerospikeException {
		this.client = client;
		this.namespace = namespace;
		this.set = set;
	}
	public static void main(String[] args) throws AerospikeException {
		try {
			Options options = new Options();
			options.addOption("h", "host", true, "Server hostname (default: 127.0.0.1)");
			options.addOption("p", "port", true, "Server port (default: 3000)");
			options.addOption("n", "namespace", true, "Namespace (default: test)");
			options.addOption("s", "set", true, "Set (default: demo)");
			options.addOption("u", "usage", false, "Print usage.");
			options.addOption("g", "gen", false, "Generate data");

			CommandLineParser parser = new PosixParser();
			CommandLine cl = parser.parse(options, args, false);


			String host = cl.getOptionValue("h", "127.0.0.1");
			String portString = cl.getOptionValue("p", "3000");
			int port = Integer.parseInt(portString);
			String namespace = cl.getOptionValue("n", "test");
			String set = cl.getOptionValue("s", "demo");
			log.debug("Host: " + host);
			log.debug("Port: " + port);
			log.debug("Namespace: " + namespace);
			log.debug("Set: " + set);
			@SuppressWarnings("unchecked")
			List<String> cmds = cl.getArgList();
			if (cl.hasOption("u")) {
				logUsage(options);
				return;
			}
			
			BatchProcessing as = new BatchProcessing(host, port, namespace, set);

			if (cl.hasOption("g")) {
				as.generateData();
			} else {
				as.batchUsingScanAll();
			}



		} catch (Exception e) {
			log.error("Critical error", e);
		}
	}
	/**
	 * Write usage to console.
	 */
	private static void logUsage(Options options) {
		HelpFormatter formatter = new HelpFormatter();
		StringWriter sw = new StringWriter();
		PrintWriter pw = new PrintWriter(sw);
		String syntax = BatchProcessing.class.getName() + " [<options>]";
		formatter.printHelp(pw, 100, syntax, "options:", options, 0, 2, null);
		log.info(sw.toString());
	}

	public void batchUsingScanAll() throws Exception {
		/*
		 * a List to contain a "batch" of records to process
		 */
		final List<Record> batchOfRecords = new ArrayList<Record>();
		/*
		 * Scan all "namespace" and "set to retrieve each record
		 */
		this.client.scanAll(null, this.namespace, this.set, new ScanCallback() {
			
			@Override
			public void scanCallback(Key key, Record record) throws AerospikeException {
				/*
				 * process each Record into a "batch" 
				 */
				if (batchOfRecords.size() == BATCH_SIZE){
					for (Record rec : batchOfRecords){
						/*
						 * do something with the batch 
						 */
						System.out.print("+");
					}
					System.out.println();
					System.out.println("Processed " + BATCH_SIZE + " records");
					batchOfRecords.clear();
				}
				batchOfRecords.add(record);
			}
		}, "username","password","gender","region","lasttweeted","tweetcount","interests");
	}
	
	public void generateData(){
		String[] genders = { "m", "f" };
		String[] regions = { "n", "s", "e", "w" };
		String[] randomInterests = { "Music", "Football", "Soccer", "Baseball", "Basketball", "Hockey", "Weekend Warrior", "Hiking", "Camping", "Travel", "Photography"};
		String username;
		ArrayList<Object> userInterests = null;
		int totalInterests = 0;
		int start = 1;
		int end = 100000;
		int totalUsers = end - start;
		Random rnd1 = new Random();
		Random rnd2 = new Random();
		Random rnd3 = new Random();


		for (int j = start; j <= end; j++) {
			// Write user record
			username = "user" + j;
			Key key = new Key(this.namespace, this.set, username);
			Bin bin1 = new Bin("username", "user" + j);
			Bin bin2 = new Bin("password", "pwd" + j);
			Bin bin3 = new Bin("gender", genders[rnd1.nextInt(2)]);
			Bin bin4 = new Bin("region", regions[rnd2.nextInt(4)]);
			Bin bin5 = new Bin("lasttweeted", 0);
			Bin bin6 = new Bin("tweetcount", 0);

			totalInterests = rnd3.nextInt(7);
			userInterests = new ArrayList<Object>();
			for(int i = 0; i < totalInterests; i++) {
				userInterests.add(randomInterests[rnd3.nextInt(randomInterests.length)]);
			}
			Bin bin7 = Bin.asList("interests", userInterests);

			client.put(null, key, bin1, bin2, bin3, bin4, bin5, bin6, bin7);
		}
		System.out.println("Completed creating " + totalUsers);
		
	}

}