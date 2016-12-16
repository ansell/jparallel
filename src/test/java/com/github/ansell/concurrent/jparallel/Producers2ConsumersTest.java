/**
 * 
 */
package com.github.ansell.concurrent.jparallel;

import static org.junit.Assert.*;

import java.util.function.Function;
import java.util.function.Supplier;

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
	 * Test method for
	 * {@link com.github.ansell.concurrent.jparallel.Producers2Consumers#builder(java.util.concurrent.Callable)}.
	 */
	@Test
	public final void testBuilder() {
		Supplier<Integer> producerCode = () -> 0;
		Function<Integer, String> consumerCode = i -> Integer.toHexString(i);
		Producers2Consumers.builder(producerCode, consumerCode).concurrency(10).setup();
	}

}
