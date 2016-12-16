/**
 * 
 */
package com.github.ansell.concurrent.jparallel;

import static org.junit.Assert.*;

import java.util.concurrent.Callable;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * @author mint
 *
 */
public class Producers2ConsumersTest {

	/**
	 * @throws java.lang.Exception
	 */
	@Before
	public void setUp() throws Exception {
	}

	/**
	 * @throws java.lang.Exception
	 */
	@After
	public void tearDown() throws Exception {
	}

	/**
	 * Test method for {@link com.github.ansell.concurrent.jparallel.Producers2Consumers#builder(java.util.concurrent.Callable)}.
	 */
	@Test
	public final void testBuilder() {
		Callable<Integer> producerCode = () -> 0;
		Producers2Consumers.builder(producerCode).concurrency(10).setup();
	}

}
