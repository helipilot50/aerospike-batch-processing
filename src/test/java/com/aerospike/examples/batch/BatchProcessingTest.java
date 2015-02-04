package com.aerospike.examples.batch;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.aerospike.client.AerospikeClient;

public class BatchProcessingTest {

	AerospikeClient client;
	BatchProcessing subject;

	@Before
	public void setUp() throws Exception {
		client = new AerospikeClient("localhost", 3000);
		subject = new BatchProcessing(client, "test", "demo");
	}

	@After
	public void tearDown() throws Exception {
		client.close();
	}

	@Test
	public void testBatchProcessing() throws Exception {
		subject.batchUsingScanAll();
	}

}
