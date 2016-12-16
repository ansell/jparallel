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

import java.util.Queue;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.function.Consumer;
import java.util.function.Function;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Test for {@link JParallel}
 * 
 * @author Peter Ansell
 */
public class JParallelTest {

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
	 * {@link com.github.ansell.concurrent.jparallel.JParallel#builder(java.util.concurrent.Callable)}.
	 */
	@Test
	public final void testBuilderDefaults() {
		Function<Integer, String> processFunction = i -> Integer.toHexString(i);
		Queue<String> results = new LinkedBlockingQueue<>();
		Consumer<String> outputFunction = results::add;

		try (JParallel<Integer, String> setup = JParallel.forFunctions(processFunction, outputFunction).start();) {
			for (int i = 0; i < 10000; i++) {
				setup.add(i);
			}
		}

		assertEquals(10000, results.size());
	}

	/**
	 * Test method for
	 * {@link com.github.ansell.concurrent.jparallel.JParallel#builder(java.util.concurrent.Callable)}.
	 */
	@Test
	public final void testBuilderDefaultsArray() {
		Function<Integer, String> processFunction = i -> Integer.toHexString(i);
		Queue<String> results = new ArrayBlockingQueue<>(10000);
		Consumer<String> outputFunction = results::add;

		try (JParallel<Integer, String> setup = JParallel.forFunctions(processFunction, outputFunction).start();) {
			for (int i = 0; i < 10000; i++) {
				setup.add(i);
			}
		}

		assertEquals(10000, results.size());
	}

	/**
	 * Test method for
	 * {@link com.github.ansell.concurrent.jparallel.JParallel#builder(java.util.concurrent.Callable)}.
	 */
	@Test
	public final void testBuilderCustom() {
		Function<Integer, String> processFunction = i -> Integer.toHexString(i);
		Queue<String> results = new LinkedBlockingQueue<>();
		Consumer<String> outputFunction = results::add;

		try (JParallel<Integer, String> setup = JParallel.forFunctions(processFunction, outputFunction)
				.inputProcessors(1).outputBuffer(40).start();) {
			for (int i = 0; i < 10000; i++) {
				setup.add(i);
			}
		}

		assertEquals(10000, results.size());
	}

}
