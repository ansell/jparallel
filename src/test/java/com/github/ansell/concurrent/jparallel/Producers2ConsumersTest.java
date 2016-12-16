/**
 * 
 */
package com.github.ansell.concurrent.jparallel;

import static org.junit.Assert.*;

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
	public final void testBuilder() {
		Function<Integer, String> processFunction = i -> Integer.toHexString(i);
		Consumer<String> outputFunction = System.out::println;
		Producers2Consumers<Integer, String> setup = Producers2Consumers.builder(processFunction, outputFunction)
				.inputProcessors(10).outputBuffer(100)
				.uncaughtExceptionHandler((t, e) -> logger.error("Uncaught error in Producers2ConsumersTest", e))
				.setup();
		try {
			for (int i = 0; i < 10000; i++) {
				setup.addInput(i);
			}
		} finally {
			setup.close();
		}
	}

}
