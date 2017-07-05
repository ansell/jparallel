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
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.function.Function;

import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
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

	@Test
	public final void testDefaults() {
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

	@Test
	public final void testCustom() {
		Function<Integer, String> processFunction = i -> Integer.toHexString(i);
		Queue<String> results = new ArrayBlockingQueue<>(count);
		Consumer<String> outputFunction = results::add;

		try (JParallel<Integer, String> setup = JParallel.forFunctions(processFunction, outputFunction)
				.inputProcessors(1).outputConsumers(2).inputBuffer(0).outputBuffer(0)
				.queueCloseRetries(10, 10L, TimeUnit.SECONDS).terminationWaitTime(5, TimeUnit.SECONDS)
				.uncaughtExceptionHandler((t, e) -> {
				}).threadNameFormat("custom-thread-name-%d").start();) {
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

	@Test
	public final void testInputQueueWaitZeroQueueSizeNonZero() {
		Function<Integer, String> processFunction = i -> Integer.toHexString(i);
		Queue<String> results = new ArrayBlockingQueue<>(count);
		Consumer<String> outputFunction = results::add;

		try (JParallel<Integer, String> setup = JParallel.forFunctions(processFunction, outputFunction)
				.inputQueueWaitTime(0, TimeUnit.MILLISECONDS).inputBuffer(1).start();) {
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

	@Test
	public final void testInputQueueWaitZeroQueueSizeZero() {
		Function<Integer, String> processFunction = i -> Integer.toHexString(i);
		Queue<String> results = new ArrayBlockingQueue<>(count);
		Consumer<String> outputFunction = results::add;

		try (JParallel<Integer, String> setup = JParallel.forFunctions(processFunction, outputFunction)
				.inputQueueWaitTime(0, TimeUnit.MILLISECONDS).inputBuffer(0).start();) {
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

	@Test
	public final void testOutputQueueWaitZeroQueueSizeNonZero() {
		Function<Integer, String> processFunction = i -> Integer.toHexString(i);
		Queue<String> results = new ArrayBlockingQueue<>(count);
		Consumer<String> outputFunction = results::add;

		try (JParallel<Integer, String> setup = JParallel.forFunctions(processFunction, outputFunction)
				.outputQueueWaitTime(0, TimeUnit.MILLISECONDS).outputBuffer(1).start();) {
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

	@Test
	public final void testOutputQueueWaitZeroQueueSizeZero() {
		Function<Integer, String> processFunction = i -> Integer.toHexString(i);
		Queue<String> results = new ArrayBlockingQueue<>(count);
		Consumer<String> outputFunction = results::add;

		try (JParallel<Integer, String> setup = JParallel.forFunctions(processFunction, outputFunction)
				.outputQueueWaitTime(0, TimeUnit.MILLISECONDS).outputBuffer(0).start();) {
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

	@Test
	public final void testWithSerial() {

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

	@Test
	public final void testDefaultUncaughtExceptionHandlerInput() {
		Function<Integer, String> processFunction = i -> {
			throw new RuntimeException("Testing uncaught exception handler on inputs");
		};
		Queue<String> results = new ArrayBlockingQueue<>(count);
		Consumer<String> outputFunction = results::add;

		try (JParallel<Integer, String> setup = JParallel.forFunctions(processFunction, outputFunction)
				.inputQueueWaitTime(10, TimeUnit.MILLISECONDS).start();) {
			for (int i = 0; i < count; i++) {
				setup.add(i);
			}
		}

		assertEquals(0, results.size());
	}

	@Test
	public final void testDefaultUncaughtExceptionHandlerOutput() {
		Function<Integer, String> processFunction = i -> Integer.toHexString(i);
		Queue<String> results = new ArrayBlockingQueue<>(count);
		Consumer<String> outputFunction = s -> {
			throw new RuntimeException("Testing uncaught exception handler on outputs");
		};

		try (JParallel<Integer, String> setup = JParallel.forFunctions(processFunction, outputFunction)
				.inputQueueWaitTime(10, TimeUnit.MILLISECONDS).start();) {
			for (int i = 0; i < count; i++) {
				setup.add(i);
			}
		}

		assertEquals(0, results.size());
	}

	@Test
	public final void testStateCloseBeforeStart() {
		Function<Integer, String> processFunction = i -> {
			throw new RuntimeException("Process function should not be called");
		};
		Consumer<String> outputFunction = s -> {
			throw new RuntimeException("Consume function should not be called");
		};

		thrown.expect(IllegalStateException.class);
		JParallel.forFunctions(processFunction, outputFunction).close();
	}

	@Test
	public final void testStateConfigAfterStart() {
		Function<Integer, String> processFunction = i -> {
			throw new RuntimeException("Process function should not be called");
		};
		Consumer<String> outputFunction = s -> {
			throw new RuntimeException("Consume function should not be called");
		};

		thrown.expect(IllegalStateException.class);
		JParallel.forFunctions(processFunction, outputFunction).start().inputBuffer(0);
	}

	@Test
	public final void testStateConfigAfterClose() {
		Function<Integer, String> processFunction = i -> {
			throw new RuntimeException("Process function should not be called");
		};
		Consumer<String> outputFunction = s -> {
			throw new RuntimeException("Consume function should not be called");
		};

		JParallel<Integer, String> jParallel = JParallel.forFunctions(processFunction, outputFunction).start();
		jParallel.close();
		thrown.expect(IllegalStateException.class);
		jParallel.inputBuffer(0);
	}

	@Test
	public final void testStateAddAfterClose() {
		Function<Integer, String> processFunction = i -> {
			throw new RuntimeException("Process function should not be called");
		};
		Consumer<String> outputFunction = s -> {
			throw new RuntimeException("Consume function should not be called");
		};

		JParallel<Integer, String> jParallel = JParallel.forFunctions(processFunction, outputFunction).start();
		jParallel.close();
		thrown.expect(IllegalStateException.class);
		jParallel.add(0);
	}

	@Test
	public final void testStateAddBeforeStart() throws Exception {
		Function<Integer, String> processFunction = i -> Integer.toHexString(i);
		Queue<String> results = new ArrayBlockingQueue<>(count);
		Consumer<String> outputFunction = results::add;

		JParallel<Integer, String> jParallel = JParallel.forFunctions(processFunction, outputFunction);
		// Ensure that we don't need to call start explicitly for add to be
		// successfully called
		jParallel.add(0);

		// Sleep to allow asynchronous processing to proceed
		Thread.sleep(100);

		assertEquals(1, results.size());
		assertTrue(results.contains("0"));
	}

	@Test
	public final void testInvalidInputProcessors() {
		Function<Integer, String> processFunction = i -> {
			throw new RuntimeException("Process function should not be called");
		};
		Consumer<String> outputFunction = s -> {
			throw new RuntimeException("Consume function should not be called");
		};

		thrown.expect(IllegalArgumentException.class);
		JParallel.forFunctions(processFunction, outputFunction).inputProcessors(0);
	}

	@Test
	public final void testInvalidOutputConsumers() {
		Function<Integer, String> processFunction = i -> {
			throw new RuntimeException("Process function should not be called");
		};
		Consumer<String> outputFunction = s -> {
			throw new RuntimeException("Consume function should not be called");
		};

		thrown.expect(IllegalArgumentException.class);
		JParallel.forFunctions(processFunction, outputFunction).outputConsumers(0);
	}

	@Test
	public final void testInvalidInputBuffer() {
		Function<Integer, String> processFunction = i -> {
			throw new RuntimeException("Process function should not be called");
		};
		Consumer<String> outputFunction = s -> {
			throw new RuntimeException("Consume function should not be called");
		};

		thrown.expect(IllegalArgumentException.class);
		JParallel.forFunctions(processFunction, outputFunction).inputBuffer(-1);
	}

	@Test
	public final void testInvalidOutputBuffer() {
		Function<Integer, String> processFunction = i -> {
			throw new RuntimeException("Process function should not be called");
		};
		Consumer<String> outputFunction = s -> {
			throw new RuntimeException("Consume function should not be called");
		};

		thrown.expect(IllegalArgumentException.class);
		JParallel.forFunctions(processFunction, outputFunction).outputBuffer(-1);
	}

	@Test
	public final void testInvalidThreadNameFormat() {
		Function<Integer, String> processFunction = i -> {
			throw new RuntimeException("Process function should not be called");
		};
		Consumer<String> outputFunction = s -> {
			throw new RuntimeException("Consume function should not be called");
		};

		thrown.expect(NullPointerException.class);
		JParallel.forFunctions(processFunction, outputFunction).threadNameFormat(null);
	}

	@Test
	public final void testInvalidThreadPriority() {
		Function<Integer, String> processFunction = i -> {
			throw new RuntimeException("Process function should not be called");
		};
		Consumer<String> outputFunction = s -> {
			throw new RuntimeException("Consume function should not be called");
		};

		JParallel.forFunctions(processFunction, outputFunction).threadPriority(Thread.MIN_PRIORITY - 1).start().close();
		JParallel.forFunctions(processFunction, outputFunction).threadPriority(Thread.MAX_PRIORITY + 1).start().close();
	}

	@Test
	public final void testInvalidUncaughtExceptionHandler() {
		Function<Integer, String> processFunction = i -> {
			throw new RuntimeException("Process function should not be called");
		};
		Consumer<String> outputFunction = s -> {
			throw new RuntimeException("Consume function should not be called");
		};

		thrown.expect(NullPointerException.class);
		JParallel.forFunctions(processFunction, outputFunction).uncaughtExceptionHandler(null);
	}

	@Test
	public final void testInvalidInputQueueWaitTime() {
		Function<Integer, String> processFunction = i -> {
			throw new RuntimeException("Process function should not be called");
		};
		Consumer<String> outputFunction = s -> {
			throw new RuntimeException("Consume function should not be called");
		};

		thrown.expect(IllegalArgumentException.class);
		JParallel.forFunctions(processFunction, outputFunction).inputQueueWaitTime(-1, TimeUnit.MILLISECONDS);
	}

	@Test
	public final void testInvalidInputQueueWaitTimeUnit() {
		Function<Integer, String> processFunction = i -> {
			throw new RuntimeException("Process function should not be called");
		};
		Consumer<String> outputFunction = s -> {
			throw new RuntimeException("Consume function should not be called");
		};

		thrown.expect(NullPointerException.class);
		JParallel.forFunctions(processFunction, outputFunction).inputQueueWaitTime(0, null);
	}

	@Test
	public final void testInvalidOutputQueueWaitTime() {
		Function<Integer, String> processFunction = i -> {
			throw new RuntimeException("Process function should not be called");
		};
		Consumer<String> outputFunction = s -> {
			throw new RuntimeException("Consume function should not be called");
		};

		thrown.expect(IllegalArgumentException.class);
		JParallel.forFunctions(processFunction, outputFunction).outputQueueWaitTime(-1, TimeUnit.MILLISECONDS);
	}

	@Test
	public final void testInvalidOutputQueueWaitTimeUnit() {
		Function<Integer, String> processFunction = i -> {
			throw new RuntimeException("Process function should not be called");
		};
		Consumer<String> outputFunction = s -> {
			throw new RuntimeException("Consume function should not be called");
		};

		thrown.expect(NullPointerException.class);
		JParallel.forFunctions(processFunction, outputFunction).outputQueueWaitTime(0, null);
	}

	@Test
	public final void testInvalidTerminationWaitTime() {
		Function<Integer, String> processFunction = i -> {
			throw new RuntimeException("Process function should not be called");
		};
		Consumer<String> outputFunction = s -> {
			throw new RuntimeException("Consume function should not be called");
		};

		thrown.expect(IllegalArgumentException.class);
		JParallel.forFunctions(processFunction, outputFunction).terminationWaitTime(-1, TimeUnit.MILLISECONDS);
	}

	@Test
	public final void testInvalidTerminationWaitTimeUnit() {
		Function<Integer, String> processFunction = i -> {
			throw new RuntimeException("Process function should not be called");
		};
		Consumer<String> outputFunction = s -> {
			throw new RuntimeException("Consume function should not be called");
		};

		thrown.expect(NullPointerException.class);
		JParallel.forFunctions(processFunction, outputFunction).terminationWaitTime(0, null);
	}

	@Test
	public final void testInvalidQueueCloseRetries() {
		Function<Integer, String> processFunction = i -> {
			throw new RuntimeException("Process function should not be called");
		};
		Consumer<String> outputFunction = s -> {
			throw new RuntimeException("Consume function should not be called");
		};

		thrown.expect(IllegalArgumentException.class);
		JParallel.forFunctions(processFunction, outputFunction).queueCloseRetries(-1, 0L, TimeUnit.NANOSECONDS);
	}

	@Test
	public final void testInvalidQueueCloseRetrySleep() {
		Function<Integer, String> processFunction = i -> {
			throw new RuntimeException("Process function should not be called");
		};
		Consumer<String> outputFunction = s -> {
			throw new RuntimeException("Consume function should not be called");
		};

		thrown.expect(IllegalArgumentException.class);
		JParallel.forFunctions(processFunction, outputFunction).queueCloseRetries(0, -1L, TimeUnit.NANOSECONDS);
	}

	@Test
	public final void testInvalidQueueCloseRetrySleepTimeUnit() {
		Function<Integer, String> processFunction = i -> {
			throw new RuntimeException("Process function should not be called");
		};
		Consumer<String> outputFunction = s -> {
			throw new RuntimeException("Consume function should not be called");
		};

		thrown.expect(IllegalArgumentException.class);
		JParallel.forFunctions(processFunction, outputFunction).queueCloseRetries(0, 1L, null);
	}

	@Test
	public final void testWaitAndInterruptDuringAddProcess() throws Exception {
		CountDownLatch testLatch = new CountDownLatch(1);
		Function<Integer, String> processFunction = i -> {
			try {
				testLatch.await();
			} catch (InterruptedException e) {
				throw new RuntimeException(e);
			}
			return Integer.toHexString(i);
		};
		Queue<String> results = new ArrayBlockingQueue<>(count);
		Consumer<String> outputFunction = results::add;

		Thread testThread = new Thread(() -> {
			try (JParallel<Integer, String> setup = JParallel.forFunctions(processFunction, outputFunction).start();) {
				for (int i = 0; i < count; i++) {
					setup.add(i);
				}
			}
		});
		testThread.start();
		Thread.sleep(1000);
		testThread.interrupt();
		testLatch.countDown();

		assertEquals(0, results.size());
	}

	@Test
	public final void testWaitAndInterruptDuringAddConsume() throws Exception {
		CountDownLatch testLatch = new CountDownLatch(1);
		Function<Integer, String> processFunction = i -> {
			return Integer.toHexString(i);
		};
		Queue<String> results = new ArrayBlockingQueue<>(count);
		Consumer<String> outputFunction = s -> {
			try {
				testLatch.await();
			} catch (InterruptedException e) {
				throw new RuntimeException(e);
			}
			results.add(s);
		};

		Thread testThread = new Thread(() -> {
			try (JParallel<Integer, String> setup = JParallel.forFunctions(processFunction, outputFunction).start();) {
				for (int i = 0; i < count; i++) {
					setup.add(i);
				}
			}
		});
		testThread.start();
		Thread.sleep(1000);
		testThread.interrupt();
		testLatch.countDown();

		assertEquals(0, results.size());
	}

	@Test
	public final void testWaitAndInterruptAfterStart() throws Exception {
		CountDownLatch testLatch = new CountDownLatch(1);
		Function<Integer, String> processFunction = i -> {
			return Integer.toHexString(i);
		};
		Queue<String> results = new ArrayBlockingQueue<>(count);
		Consumer<String> outputFunction = s -> {
			try {
				testLatch.await();
			} catch (InterruptedException e) {
				throw new RuntimeException(e);
			}
			results.add(s);
		};

		Thread testThread = new Thread(() -> {
			try (JParallel<Integer, String> setup = JParallel.forFunctions(processFunction, outputFunction).start();) {
				for (int i = 0; i < count; i++) {
					setup.add(i);
				}
			}
		});
		testThread.start();
		testThread.interrupt();
		Thread.sleep(1000);
		testLatch.countDown();

		assertEquals(0, results.size());
	}

	@Ignore("Not possible to reliably interrupt the thread, due to the opaque implementation of the internal ExecutorService which is free to substitute threads as necessary")
	@Test
	public final void testWaitAndInterruptAfterAdd() throws Exception {
		CountDownLatch testLatch = new CountDownLatch(1);
		Function<Integer, String> processFunction = i -> {
			return Integer.toHexString(i);
		};
		Queue<String> results = new ArrayBlockingQueue<>(count);
		Consumer<String> outputFunction = s -> {
			try {
				testLatch.await();
				Thread.currentThread().interrupt();
			} catch (InterruptedException e) {
				e.printStackTrace();
				throw new RuntimeException(e);
			}
			results.add(s);
		};

		Thread testThread = new Thread(() -> {
			try (JParallel<Integer, String> setup = JParallel.forFunctions(processFunction, outputFunction).start();) {
				for (int i = 0; i < count; i++) {
					setup.add(i);
					Thread.currentThread().interrupt();
				}
			}
		});
		assertEquals("Results size was not correct before running test", 0, results.size());
		testThread.start();
		assertEquals("Results size was not correct after starting test", 0, results.size());
		Thread.sleep(1000);
		assertEquals("Results size was not correct after sleep", 0, results.size());
		testLatch.countDown();
		Thread.sleep(1000);
		assertEquals("Results size was not correct after countdown", 0, results.size());
		assertEquals(0, results.size());
	}

	@Test
	public final void testInputQueueFailedToAcceptTimeout() throws Exception {
		CountDownLatch testLatch = new CountDownLatch(1);
		AtomicBoolean interruptFailure = new AtomicBoolean(false);
		// Indefinitely wait in the processor to test the side effects of adding
		// multiple items to a very short queueWaitTime queue
		Function<Integer, String> processFunction = i -> {
			try {
				testLatch.await();
			} catch (InterruptedException e) {
				interruptFailure.set(true);
				e.printStackTrace();
			}
			return Integer.toHexString(i);
		};
		Queue<String> results = new ArrayBlockingQueue<>(count);
		Consumer<String> outputFunction = results::add;

		Thread testThread = new Thread(() -> {
			try (JParallel<Integer, String> setup = JParallel.forFunctions(processFunction, outputFunction)
					.inputQueueWaitTime(1, TimeUnit.NANOSECONDS).inputBuffer(1).inputProcessors(1).outputBuffer(1)
					.outputConsumers(1).start();) {
				for (int i = 0; i < count; i++) {
					setup.add(i);
				}
			}
		});
		assertEquals("Results size was not correct before running test", 0, results.size());
		testThread.start();
		assertEquals("Results size was not correct after starting test", 0, results.size());
		Thread.sleep(1000);
		assertEquals("Results size was not correct after sleep", 0, results.size());
		testLatch.countDown();
		Thread.sleep(1000);
		// The output of this test is uncertain, other than that we always
		// expect at least 1 item to make it through, given at least one makes
		// it past the queue wait time and should trigger the processor
		assertFalse(
				"There should have been at least 1 item making it through to the results from the heavily restricted queue",
				results.isEmpty());
		assertFalse("No interrupts were expected during the process function", interruptFailure.get());
	}
}