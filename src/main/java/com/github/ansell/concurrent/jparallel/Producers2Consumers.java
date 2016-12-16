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
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;

import com.google.common.util.concurrent.ThreadFactoryBuilder;

/**
 * Builder pattern for a multi-producer, multi consumer application.
 * 
 * @author Peter Ansell p_ansell@yahoo.com
 */
public class Producers2Consumers<P, C> implements AutoCloseable {

	private int inputQueueSize = Runtime.getRuntime().availableProcessors() * 10;
	private int outputQueueSize = Runtime.getRuntime().availableProcessors() * 10;
	private int concurrencyLevel = Runtime.getRuntime().availableProcessors();
	private BlockingQueue<P> inputQueue = null;
	private Function<P, C> functionCode;
	private BlockingQueue<C> outputQueue = null;
	private ExecutorService producersService;
	private int threadPriority = Thread.NORM_PRIORITY;
	private String threadNameFormat = "pool-thread-%d";
	private UncaughtExceptionHandler uncaughtExceptionHandler = (t, e) -> {
	};
	private long waitTime = 1;
	private TimeUnit waitUnit = TimeUnit.MINUTES;

	private final AtomicBoolean setup = new AtomicBoolean(false);

	@SuppressWarnings("unchecked")
	private final P inputSentinel = (P) new Object();
	@SuppressWarnings("unchecked")
	private final C outputSentinel = (C) new Object();

	/**
	 * Only accessed through the builder pattern.
	 */
	private Producers2Consumers(Function<P, C> functionCode) {
		this.functionCode = functionCode;
	}

	public static <P, C> Producers2Consumers<P, C> builder(Function<P, C> functionCode) {
		return new Producers2Consumers<P, C>(Objects.requireNonNull(functionCode, "Function code must not be null"));
	}

	/**
	 * The concurrency level to use when constructing threads for processing
	 * inputs.
	 * 
	 * @param concurrencyLevel
	 *            The number of threads to use for processing inputs.
	 * @return Fluent return of this object.
	 */
	public Producers2Consumers<P, C> concurrency(int concurrencyLevel) {
		checkNotSetup();

		if (concurrencyLevel < 1) {
			throw new IllegalArgumentException("Concurrency level must be positive.");
		}

		this.concurrencyLevel = concurrencyLevel;
		return this;
	}

	public Producers2Consumers<P, C> inputBuffer(int inputQueueSize) {
		checkNotSetup();

		if (inputQueueSize < 0) {
			throw new IllegalArgumentException("Input queue size must be non-negative.");
		}

		this.inputQueueSize = inputQueueSize;
		return this;
	}

	public Producers2Consumers<P, C> outputBuffer(int outputQueueSize) {
		checkNotSetup();

		if (inputQueueSize < 0) {
			throw new IllegalArgumentException("Output queue size must be non-negative.");
		}

		this.outputQueueSize = outputQueueSize;
		return this;
	}

	public Producers2Consumers<P, C> threadNameFormat(String threadNameFormat) {
		checkNotSetup();

		this.threadNameFormat = Objects.requireNonNull(threadNameFormat, "Thread name format must not be null");
		return this;
	}

	public Producers2Consumers<P, C> threadPriority(int threadPriority) {
		checkNotSetup();

		if (Math.max(threadPriority, Thread.MIN_PRIORITY) == Thread.MIN_PRIORITY) {
			threadPriority = Thread.MIN_PRIORITY;
		} else if (Math.min(threadPriority, Thread.MAX_PRIORITY) == Thread.MAX_PRIORITY) {
			threadPriority = Thread.MAX_PRIORITY;
		}

		this.threadPriority = threadPriority;
		return this;
	}

	public Producers2Consumers<P, C> uncaughtExceptionHandler(UncaughtExceptionHandler uncaughtExceptionHandler) {
		checkNotSetup();

		this.uncaughtExceptionHandler = uncaughtExceptionHandler;
		return this;
	}

	public Producers2Consumers<P, C> terminationWaitTime(long waitTime, TimeUnit waitUnit) {
		checkNotSetup();

		if (waitTime < 0) {
			throw new IllegalArgumentException("Termination wait time must be non-negative.");
		}

		this.waitTime = waitTime;
		this.waitUnit = Objects.requireNonNull(waitUnit, "Termination wait time unit must not be null");
		return this;
	}

	public Producers2Consumers<P, C> setup() {
		if (this.setup.compareAndSet(false, true)) {
			if (this.inputQueueSize > 0) {
				this.inputQueue = new ArrayBlockingQueue<>(inputQueueSize);
			} else {
				this.inputQueue = new LinkedBlockingQueue<>();
			}

			if (this.outputQueueSize > 0) {
				this.outputQueue = new ArrayBlockingQueue<>(outputQueueSize);
			} else {
				this.outputQueue = new LinkedBlockingQueue<>();
			}

			ThreadFactory nextThreadFactory = new ThreadFactoryBuilder()
					.setUncaughtExceptionHandler(uncaughtExceptionHandler).setNameFormat(threadNameFormat)
					.setPriority(threadPriority).build();
			if (concurrencyLevel > 0) {
				this.producersService = Executors.newFixedThreadPool(concurrencyLevel, nextThreadFactory);
			} else {
				this.producersService = Executors.newCachedThreadPool(nextThreadFactory);
			}
		} else {
			checkNotSetup();
		}

		return this;
	}

	private void checkNotSetup() {
		if (this.setup.get()) {
			throw new IllegalStateException("Already setup");
		}
	}
	
	public void add(P toConsume) {
		try {
			if (Thread.interrupted()) {
				this.close();
				throwShutdownException();
			}

			if (this.inputQueueSize > 0) {
				this.inputQueue.put(toConsume);
			} else {
				this.inputQueue.add(toConsume);
			}
		} catch (InterruptedException e) {
			this.close();
			throwShutdownException(e);
		}
	}

	public void finish() {
		try {
			add(inputSentinel);
			Thread.sleep(1);
		} catch (InterruptedException e) {
			Thread.currentThread().interrupt();
			throwShutdownException(e);
		} finally {
			close();
		}
	}

	private void throwShutdownException() {
		throw new RuntimeException("Execution was interrupted");
	}

	private void throwShutdownException(Throwable e) {
		throw new RuntimeException("Execution was interrupted", e);
	}

	@Override
	public void close() {
		try {
			this.outputQueue.add(outputSentinel);
			this.producersService.shutdown();
			this.producersService.awaitTermination(waitTime, waitUnit);
		} catch (InterruptedException e) {
			Thread.currentThread().interrupt();
			throwShutdownException(e);
		} finally {
			if (!this.producersService.isTerminated()) {
				this.producersService.shutdownNow();
			}
		}
	}
}
