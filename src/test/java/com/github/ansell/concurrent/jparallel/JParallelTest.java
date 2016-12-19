/*
 * Copyright (c) 2016, Peter Ansell
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.github.ansell.concurrent.jparallel;

import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Function;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.Timeout;

/**
 * Test for {@link JParallel}
 * 
 * @author Peter Ansell
 */
public class JParallelTest {

	@Rule
	public Timeout timeout = new Timeout(1, TimeUnit.MINUTES);

	@Rule
	public ExpectedException thrown = ExpectedException.none();

	private int count;
	private int baseDelay;

	/**
	 * @throws java.lang.Exception
	 */
	@Before
	public void setUp() throws Exception {
		count = 100;
		baseDelay = 1;
	}

	/**
	 * @throws java.lang.Exception
	 */
	@After
	public void tearDown() throws Exception {
	}

	@Test
	public final void testSerial() {
		Queue<String> results = new ArrayBlockingQueue<>(count);
		for (int i = 0; i < count; i++) {
			results.add(Integer.toHexString(i));
		}
		assertEquals(count, results.size());
		List<String> sortedResults = new ArrayList<>(results);
		Collections.sort(sortedResults);
		Set<String> uniqueResults = new LinkedHashSet<>(sortedResults);
		assertEquals(count, uniqueResults.size());
	}

	/**
	 * Test method for
	 * {@link com.github.ansell.concurrent.jparallel.JParallel#builder(java.util.concurrent.Callable)}.
	 */
	@Test
	public final void testBuilderDefaults() {
		Function<Integer, String> processFunction = i -> Integer.toHexString(i);
		Queue<String> results = new ArrayBlockingQueue<>(count);
		Consumer<String> outputFunction = results::add;

		try (JParallel<Integer, String> setup = JParallel.forFunctions(processFunction, outputFunction).start();) {
			for (int i = 0; i < count; i++) {
				setup.add(i);
			}
		}

		assertEquals(count, results.size());
		List<String> sortedResults = new ArrayList<>(results);
		Collections.sort(sortedResults);
		Set<String> uniqueResults = new LinkedHashSet<>(sortedResults);
		assertEquals(count, uniqueResults.size());
	}

	/**
	 * Test method for
	 * {@link com.github.ansell.concurrent.jparallel.JParallel#builder(java.util.concurrent.Callable)}.
	 */
	@Test
	public final void testBuilderCustom() {
		Function<Integer, String> processFunction = i -> Integer.toHexString(i);
		Queue<String> results = new ArrayBlockingQueue<>(count);
		Consumer<String> outputFunction = results::add;

		try (JParallel<Integer, String> setup = JParallel.forFunctions(processFunction, outputFunction)
				.inputProcessors(1).outputConsumers(2).inputBuffer(0).outputBuffer(0).start();) {
			for (int i = 0; i < count; i++) {
				setup.add(i);
			}
		}

		assertEquals(count, results.size());
		List<String> sortedResults = new ArrayList<>(results);
		Collections.sort(sortedResults);
		Set<String> uniqueResults = new LinkedHashSet<>(sortedResults);
		assertEquals(count, uniqueResults.size());
	}

	/**
	 * Test method for
	 * {@link com.github.ansell.concurrent.jparallel.JParallel#builder(java.util.concurrent.Callable)}.
	 */
	@Test
	public final void testBuilderInputQueueWaitZeroQueueSizeNonZero() {
		Function<Integer, String> processFunction = i -> Integer.toHexString(i);
		Queue<String> results = new ArrayBlockingQueue<>(count);
		Consumer<String> outputFunction = results::add;

		try (JParallel<Integer, String> setup = JParallel.forFunctions(processFunction, outputFunction)
				.queueWaitTime(0, TimeUnit.MILLISECONDS).inputBuffer(1).start();) {
			for (int i = 0; i < count; i++) {
				setup.add(i);
			}
		}

		assertEquals(count, results.size());
		List<String> sortedResults = new ArrayList<>(results);
		Collections.sort(sortedResults);
		Set<String> uniqueResults = new LinkedHashSet<>(sortedResults);
		assertEquals(count, uniqueResults.size());
	}

	/**
	 * Test method for
	 * {@link com.github.ansell.concurrent.jparallel.JParallel#builder(java.util.concurrent.Callable)}.
	 */
	@Test
	public final void testBuilderInputQueueWaitZeroQueueSizeZero() {
		Function<Integer, String> processFunction = i -> Integer.toHexString(i);
		Queue<String> results = new ArrayBlockingQueue<>(count);
		Consumer<String> outputFunction = results::add;

		try (JParallel<Integer, String> setup = JParallel.forFunctions(processFunction, outputFunction)
				.queueWaitTime(0, TimeUnit.MILLISECONDS).inputBuffer(0).start();) {
			for (int i = 0; i < count; i++) {
				setup.add(i);
			}
		}

		assertEquals(count, results.size());
		List<String> sortedResults = new ArrayList<>(results);
		Collections.sort(sortedResults);
		Set<String> uniqueResults = new LinkedHashSet<>(sortedResults);
		assertEquals(count, uniqueResults.size());
	}

	/**
	 * Test method for
	 * {@link com.github.ansell.concurrent.jparallel.JParallel#builder(java.util.concurrent.Callable)}.
	 */
	@Test
	public final void testBuilderOutputQueueWaitZeroQueueSizeNonZero() {
		Function<Integer, String> processFunction = i -> Integer.toHexString(i);
		Queue<String> results = new ArrayBlockingQueue<>(count);
		Consumer<String> outputFunction = results::add;

		try (JParallel<Integer, String> setup = JParallel.forFunctions(processFunction, outputFunction)
				.queueWaitTime(0, TimeUnit.MILLISECONDS).outputBuffer(1).start();) {
			for (int i = 0; i < count; i++) {
				setup.add(i);
			}
		}

		assertEquals(count, results.size());
		List<String> sortedResults = new ArrayList<>(results);
		Collections.sort(sortedResults);
		Set<String> uniqueResults = new LinkedHashSet<>(sortedResults);
		assertEquals(count, uniqueResults.size());
	}

	/**
	 * Test method for
	 * {@link com.github.ansell.concurrent.jparallel.JParallel#builder(java.util.concurrent.Callable)}.
	 */
	@Test
	public final void testBuilderOutputQueueWaitZeroQueueSizeZero() {
		Function<Integer, String> processFunction = i -> Integer.toHexString(i);
		Queue<String> results = new ArrayBlockingQueue<>(count);
		Consumer<String> outputFunction = results::add;

		try (JParallel<Integer, String> setup = JParallel.forFunctions(processFunction, outputFunction)
				.queueWaitTime(0, TimeUnit.MILLISECONDS).outputBuffer(0).start();) {
			for (int i = 0; i < count; i++) {
				setup.add(i);
			}
		}

		assertEquals(count, results.size());
		List<String> sortedResults = new ArrayList<>(results);
		Collections.sort(sortedResults);
		Set<String> uniqueResults = new LinkedHashSet<>(sortedResults);
		assertEquals(count, uniqueResults.size());
	}

	/**
	 * Test method for
	 * {@link com.github.ansell.concurrent.jparallel.JParallel#builder(java.util.concurrent.Callable)}.
	 */
	@Test
	public final void testBuilderWithSerial() {

		int[] delays = new int[count];
		for (int i = 0; i < count; i++) {
			delays[i] = baseDelay + (int) Math.round(Math.random() + baseDelay);
		}

		Function<Integer, String> processFunction = i -> {
			try {
				Thread.sleep(delays[i]);
			} catch (InterruptedException e) {
				Thread.currentThread().interrupt();
			}
			return Integer.toHexString(i);
		};
		Queue<String> results = new ArrayBlockingQueue<>(count);
		Consumer<String> outputFunction = results::add;

		long startParallel = System.currentTimeMillis();
		try (JParallel<Integer, String> setup = JParallel.forFunctions(processFunction, outputFunction).start();) {
			for (int i = 0; i < count; i++) {
				setup.add(i);
			}
		}
		System.out.println("Parallel processing took: " + (System.currentTimeMillis() - startParallel) + " ms");

		Queue<String> serialResults = new ArrayBlockingQueue<>(count);
		long startSerial = System.currentTimeMillis();
		for (int i = 0; i < count; i++) {
			try {
				Thread.sleep(delays[i]);
			} catch (InterruptedException e) {
				Thread.currentThread().interrupt();
			}
			serialResults.add(Integer.toHexString(i));
		}
		System.out.println("Serial processing took: " + (System.currentTimeMillis() - startSerial) + " ms");

		assertEquals(count, results.size());
		List<String> sortedResults = new ArrayList<>(results);
		Collections.sort(sortedResults);
		Set<String> uniqueResults = new LinkedHashSet<>(sortedResults);
		assertEquals(count, uniqueResults.size());

		assertEquals(count, serialResults.size());
		List<String> sortedSerialResults = new ArrayList<>(serialResults);
		Collections.sort(sortedSerialResults);
		Set<String> uniqueSerialResults = new LinkedHashSet<>(sortedSerialResults);
		assertEquals(count, uniqueSerialResults.size());
	}

	/**
	 * Test method for
	 * {@link com.github.ansell.concurrent.jparallel.JParallel#builder(java.util.concurrent.Callable)}.
	 */
	@Test
	public final void testBuilderDefaultUncaughtExceptionHandlerInput() {
		Function<Integer, String> processFunction = i -> {
			throw new RuntimeException("Testing uncaught exception handler on inputs");
		};
		Queue<String> results = new ArrayBlockingQueue<>(count);
		Consumer<String> outputFunction = results::add;

		try (JParallel<Integer, String> setup = JParallel.forFunctions(processFunction, outputFunction)
				.queueWaitTime(5, TimeUnit.MILLISECONDS).start();) {
			for (int i = 0; i < count; i++) {
				setup.add(i);
			}
		}

		assertEquals(0, results.size());
	}

	/**
	 * Test method for
	 * {@link com.github.ansell.concurrent.jparallel.JParallel#builder(java.util.concurrent.Callable)}.
	 */
	@Test
	public final void testBuilderDefaultUncaughtExceptionHandlerOutput() {
		Function<Integer, String> processFunction = i -> Integer.toHexString(i);
		Queue<String> results = new ArrayBlockingQueue<>(count);
		Consumer<String> outputFunction = s -> {
			throw new RuntimeException("Testing uncaught exception handler on outputs");
		};

		try (JParallel<Integer, String> setup = JParallel.forFunctions(processFunction, outputFunction)
				.queueWaitTime(5, TimeUnit.MILLISECONDS).start();) {
			for (int i = 0; i < count; i++) {
				setup.add(i);
			}
		}

		assertEquals(0, results.size());
	}

	/**
	 * Test method for
	 * {@link com.github.ansell.concurrent.jparallel.JParallel#builder(java.util.concurrent.Callable)}.
	 */
	@Test
	public final void testBuilderStateCloseBeforeStart() {
		Function<Integer, String> processFunction = i -> {
			throw new RuntimeException("Process function should not be called");
		};
		Consumer<String> outputFunction = s -> {
			throw new RuntimeException("Consume function should not be called");
		};

		thrown.expect(IllegalStateException.class);
		JParallel.forFunctions(processFunction, outputFunction).close();
	}

	/**
	 * Test method for
	 * {@link com.github.ansell.concurrent.jparallel.JParallel#builder(java.util.concurrent.Callable)}.
	 */
	@Test
	public final void testBuilderStateConfigAfterStart() {
		Function<Integer, String> processFunction = i -> {
			throw new RuntimeException("Process function should not be called");
		};
		Consumer<String> outputFunction = s -> {
			throw new RuntimeException("Consume function should not be called");
		};

		thrown.expect(IllegalStateException.class);
		JParallel.forFunctions(processFunction, outputFunction).start().inputBuffer(0);
	}

	/**
	 * Test method for
	 * {@link com.github.ansell.concurrent.jparallel.JParallel#builder(java.util.concurrent.Callable)}.
	 */
	@Test
	public final void testBuilderStateConfigAfterClose() {
		Function<Integer, String> processFunction = i -> {
			throw new RuntimeException("Process function should not be called");
		};
		Consumer<String> outputFunction = s -> {
			throw new RuntimeException("Consume function should not be called");
		};

		JParallel<Integer,String> jParallel = JParallel.forFunctions(processFunction, outputFunction).start();
		jParallel.close();
		thrown.expect(IllegalStateException.class);
		jParallel.inputBuffer(0);
	}

	/**
	 * Test method for
	 * {@link com.github.ansell.concurrent.jparallel.JParallel#builder(java.util.concurrent.Callable)}.
	 */
	@Test
	public final void testBuilderStateAddAfterClose() {
		Function<Integer, String> processFunction = i -> {
			throw new RuntimeException("Process function should not be called");
		};
		Consumer<String> outputFunction = s -> {
			throw new RuntimeException("Consume function should not be called");
		};

		JParallel<Integer,String> jParallel = JParallel.forFunctions(processFunction, outputFunction).start();
		jParallel.close();
		thrown.expect(IllegalStateException.class);
		jParallel.add(0);
	}

	/**
	 * Test method for
	 * {@link com.github.ansell.concurrent.jparallel.JParallel#builder(java.util.concurrent.Callable)}.
	 */
	@Test
	public final void testBuilderStateAddBeforeStart() {
		Function<Integer, String> processFunction = i -> {
			throw new RuntimeException("Process function should not be called");
		};
		Consumer<String> outputFunction = s -> {
			throw new RuntimeException("Consume function should not be called");
		};

		JParallel<Integer,String> jParallel = JParallel.forFunctions(processFunction, outputFunction);
		thrown.expect(IllegalStateException.class);
		jParallel.add(0);
	}

	/**
	 * Test method for
	 * {@link com.github.ansell.concurrent.jparallel.JParallel#builder(java.util.concurrent.Callable)}.
	 */
	@Test
	public final void testBuilderInvalidInputProcessors() {
		Function<Integer, String> processFunction = i -> {
			throw new RuntimeException("Process function should not be called");
		};
		Consumer<String> outputFunction = s -> {
			throw new RuntimeException("Consume function should not be called");
		};

		thrown.expect(IllegalArgumentException.class);
		JParallel.forFunctions(processFunction, outputFunction).inputProcessors(0);
	}

	/**
	 * Test method for
	 * {@link com.github.ansell.concurrent.jparallel.JParallel#builder(java.util.concurrent.Callable)}.
	 */
	@Test
	public final void testBuilderInvalidOutputConsumers() {
		Function<Integer, String> processFunction = i -> {
			throw new RuntimeException("Process function should not be called");
		};
		Consumer<String> outputFunction = s -> {
			throw new RuntimeException("Consume function should not be called");
		};

		thrown.expect(IllegalArgumentException.class);
		JParallel.forFunctions(processFunction, outputFunction).outputConsumers(0);
	}

	/**
	 * Test method for
	 * {@link com.github.ansell.concurrent.jparallel.JParallel#builder(java.util.concurrent.Callable)}.
	 */
	@Test
	public final void testBuilderInvalidInputBuffer() {
		Function<Integer, String> processFunction = i -> {
			throw new RuntimeException("Process function should not be called");
		};
		Consumer<String> outputFunction = s -> {
			throw new RuntimeException("Consume function should not be called");
		};

		thrown.expect(IllegalArgumentException.class);
		JParallel.forFunctions(processFunction, outputFunction).inputBuffer(-1);
	}

	/**
	 * Test method for
	 * {@link com.github.ansell.concurrent.jparallel.JParallel#builder(java.util.concurrent.Callable)}.
	 */
	@Test
	public final void testBuilderInvalidOutputBuffer() {
		Function<Integer, String> processFunction = i -> {
			throw new RuntimeException("Process function should not be called");
		};
		Consumer<String> outputFunction = s -> {
			throw new RuntimeException("Consume function should not be called");
		};

		thrown.expect(IllegalArgumentException.class);
		JParallel.forFunctions(processFunction, outputFunction).outputBuffer(-1);
	}

	/**
	 * Test method for
	 * {@link com.github.ansell.concurrent.jparallel.JParallel#builder(java.util.concurrent.Callable)}.
	 */
	@Test
	public final void testBuilderInvalidThreadNameFormat() {
		Function<Integer, String> processFunction = i -> {
			throw new RuntimeException("Process function should not be called");
		};
		Consumer<String> outputFunction = s -> {
			throw new RuntimeException("Consume function should not be called");
		};

		thrown.expect(NullPointerException.class);
		JParallel.forFunctions(processFunction, outputFunction).threadNameFormat(null);
	}

	/**
	 * Test method for
	 * {@link com.github.ansell.concurrent.jparallel.JParallel#builder(java.util.concurrent.Callable)}.
	 */
	@Test
	public final void testBuilderInvalidThreadPriority() {
		Function<Integer, String> processFunction = i -> {
			throw new RuntimeException("Process function should not be called");
		};
		Consumer<String> outputFunction = s -> {
			throw new RuntimeException("Consume function should not be called");
		};

		JParallel.forFunctions(processFunction, outputFunction).threadPriority(Thread.MIN_PRIORITY - 1).start().close();
		JParallel.forFunctions(processFunction, outputFunction).threadPriority(Thread.MAX_PRIORITY + 1).start().close();
	}

	/**
	 * Test method for
	 * {@link com.github.ansell.concurrent.jparallel.JParallel#builder(java.util.concurrent.Callable)}.
	 */
	@Test
	public final void testBuilderInvalidUncaughtExceptionHandler() {
		Function<Integer, String> processFunction = i -> {
			throw new RuntimeException("Process function should not be called");
		};
		Consumer<String> outputFunction = s -> {
			throw new RuntimeException("Consume function should not be called");
		};

		thrown.expect(NullPointerException.class);
		JParallel.forFunctions(processFunction, outputFunction).uncaughtExceptionHandler(null);
	}

	/**
	 * Test method for
	 * {@link com.github.ansell.concurrent.jparallel.JParallel#builder(java.util.concurrent.Callable)}.
	 */
	@Test
	public final void testBuilderInvalidQueueWaitTime() {
		Function<Integer, String> processFunction = i -> {
			throw new RuntimeException("Process function should not be called");
		};
		Consumer<String> outputFunction = s -> {
			throw new RuntimeException("Consume function should not be called");
		};

		thrown.expect(IllegalArgumentException.class);
		JParallel.forFunctions(processFunction, outputFunction).queueWaitTime(-1, TimeUnit.MILLISECONDS);
	}

	/**
	 * Test method for
	 * {@link com.github.ansell.concurrent.jparallel.JParallel#builder(java.util.concurrent.Callable)}.
	 */
	@Test
	public final void testBuilderInvalidQueueWaitTimeUnit() {
		Function<Integer, String> processFunction = i -> {
			throw new RuntimeException("Process function should not be called");
		};
		Consumer<String> outputFunction = s -> {
			throw new RuntimeException("Consume function should not be called");
		};

		thrown.expect(NullPointerException.class);
		JParallel.forFunctions(processFunction, outputFunction).queueWaitTime(0, null);
	}

	/**
	 * Test method for
	 * {@link com.github.ansell.concurrent.jparallel.JParallel#builder(java.util.concurrent.Callable)}.
	 */
	@Test
	public final void testBuilderInvalidTerminationWaitTime() {
		Function<Integer, String> processFunction = i -> {
			throw new RuntimeException("Process function should not be called");
		};
		Consumer<String> outputFunction = s -> {
			throw new RuntimeException("Consume function should not be called");
		};

		thrown.expect(IllegalArgumentException.class);
		JParallel.forFunctions(processFunction, outputFunction).terminationWaitTime(-1, TimeUnit.MILLISECONDS);
	}

	/**
	 * Test method for
	 * {@link com.github.ansell.concurrent.jparallel.JParallel#builder(java.util.concurrent.Callable)}.
	 */
	@Test
	public final void testBuilderInvalidTerminationWaitTimeUnit() {
		Function<Integer, String> processFunction = i -> {
			throw new RuntimeException("Process function should not be called");
		};
		Consumer<String> outputFunction = s -> {
			throw new RuntimeException("Consume function should not be called");
		};

		thrown.expect(NullPointerException.class);
		JParallel.forFunctions(processFunction, outputFunction).terminationWaitTime(0, null);
	}

	/**
	 * Test method for
	 * {@link com.github.ansell.concurrent.jparallel.JParallel#builder(java.util.concurrent.Callable)}.
	 */
	@Test
	public final void testBuilderInvalidQueueCloseRetries() {
		Function<Integer, String> processFunction = i -> {
			throw new RuntimeException("Process function should not be called");
		};
		Consumer<String> outputFunction = s -> {
			throw new RuntimeException("Consume function should not be called");
		};

		thrown.expect(IllegalArgumentException.class);
		JParallel.forFunctions(processFunction, outputFunction).queueCloseRetries(-1, 0L);
	}

	/**
	 * Test method for
	 * {@link com.github.ansell.concurrent.jparallel.JParallel#builder(java.util.concurrent.Callable)}.
	 */
	@Test
	public final void testBuilderInvalidQueueCloseRetrySleep() {
		Function<Integer, String> processFunction = i -> {
			throw new RuntimeException("Process function should not be called");
		};
		Consumer<String> outputFunction = s -> {
			throw new RuntimeException("Consume function should not be called");
		};

		thrown.expect(IllegalArgumentException.class);
		JParallel.forFunctions(processFunction, outputFunction).queueCloseRetries(0, -1L);
	}

}