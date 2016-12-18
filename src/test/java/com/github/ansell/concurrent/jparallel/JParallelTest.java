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
import org.junit.rules.Timeout;

/**
 * Test for {@link JParallel}
 * 
 * @author Peter Ansell
 */
public class JParallelTest {

	@Rule
	public Timeout timeout = new Timeout(1, TimeUnit.MINUTES);

	private int count;
	private int baseDelay;

	/**
	 * @throws java.lang.Exception
	 */
	@Before
	public void setUp() throws Exception {
		count = 1000;
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
				.inputProcessors(1).outputBuffer(40).start();) {
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

}
