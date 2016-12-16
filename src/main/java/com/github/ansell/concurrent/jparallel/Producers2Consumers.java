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

import java.lang.Thread.UncaughtExceptionHandler;
import java.util.Objects;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.function.Function;
import java.util.function.Supplier;

import com.google.common.util.concurrent.ThreadFactoryBuilder;

/**
 * Builder pattern for a multi-producer, multi consumer application.
 * 
 * @author Peter Ansell p_ansell@yahoo.com
 */
public class Producers2Consumers<P, C> {

	private Function<P, C> functionCode;
	private int fixedQueueSize = 0;
	private BlockingQueue<C> queue;
	private int concurrencyLevel;
	private ExecutorService producersService;
	private int threadPriority;
	private String threadNameFormat;
	private UncaughtExceptionHandler uncaughtExceptionHandler = (t, e) -> {
	};
	private boolean setup;

	/**
	 * Only accessed through the builder pattern.
	 */
	private Producers2Consumers(Function<P, C> functionCode) {
		this.functionCode = functionCode;
	}

	public static <P, C> Producers2Consumers<P, C> builder(Function<P, C> functionCode) {
		return new Producers2Consumers<P, C>(Objects.requireNonNull(functionCode, "Function code must not be null"));
	}

	public Producers2Consumers<P, C> concurrency(int concurrencyLevel) {
		if (concurrencyLevel < 0) {
			throw new IllegalArgumentException("Concurrency level must be non-negative.");
		}

		this.concurrencyLevel = concurrencyLevel;
		return this;
	}

	public Producers2Consumers<P, C> buffer(int fixedQueueSize) {
		this.fixedQueueSize = fixedQueueSize;
		return this;
	}

	public Producers2Consumers<P, C> uncaughtExceptionHandler(UncaughtExceptionHandler uncaughtExceptionHandler) {
		this.uncaughtExceptionHandler = uncaughtExceptionHandler;
		return this;
	}

	public Producers2Consumers<P, C> setup() {
		if (this.setup) {
			throw new IllegalStateException("Already setup");
		}

		if (this.fixedQueueSize > 0) {
			this.queue = new ArrayBlockingQueue<>(fixedQueueSize);
		} else {
			this.queue = new LinkedBlockingQueue<>();
		}

		ThreadFactory nextThreadFactory = new ThreadFactoryBuilder()
				.setUncaughtExceptionHandler(uncaughtExceptionHandler).setNameFormat(threadNameFormat)
				.setPriority(threadPriority).build();
		if (concurrencyLevel > 0) {
			this.producersService = Executors.newFixedThreadPool(concurrencyLevel, nextThreadFactory);
		} else {
			this.producersService = Executors.newCachedThreadPool(nextThreadFactory);
		}

		this.setup = true;

		return this;
	}
}
