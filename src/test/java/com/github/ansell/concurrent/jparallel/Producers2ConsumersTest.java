/**
 * 
 */
package com.github.ansell.concurrent.jparallel;

import static org.junit.Assert.*;

import java.util.Queue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.function.Consumer;
import java.util.function.Function;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Test for {@link Producers2Consumers}
 * 
 * @author Peter Ansell
 */
public class Producers2ConsumersTest {

	private final Logger logger = LoggerFactory.getLogger(this.getClass());

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
	public final void testBuilderDefaults() {
		Function<Integer, String> processFunction = i -> Integer.toHexString(i);
		Queue<String> results = new LinkedBlockingQueue<>();
		Consumer<String> outputFunction = results::add;
		Producers2Consumers<Integer, String> setup = Producers2Consumers.builder(processFunction, outputFunction).setup();
		try {
			for (int i = 0; i < 10000; i++) {
				setup.addInput(i);
			}
		} finally {
			setup.close();
		}
		
		assertEquals(10000, results.size());
	}

	/**
	 * Test method for
	 * {@link com.github.ansell.concurrent.jparallel.Producers2Consumers#builder(java.util.concurrent.Callable)}.
	 */
	@Test
	public final void testBuilderCustom() {
		Function<Integer, String> processFunction = i -> Integer.toHexString(i);
		Queue<String> results = new LinkedBlockingQueue<>();
		Consumer<String> outputFunction = results::add;
		Producers2Consumers<Integer, String> setup = Producers2Consumers.builder(processFunction, outputFunction)
				.inputProcessors(1).outputBuffer(40)
				.setup();
		try {
			for (int i = 0; i < 10000; i++) {
				setup.addInput(i);
			}
		} finally {
			setup.close();
		}
		
		assertEquals(10000, results.size());
	}

}
