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
import java.util.List;
import java.util.Objects;
import java.util.Queue;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.function.Function;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.util.concurrent.ThreadFactoryBuilder;

/**
 * Builder pattern for a multi-producer, multi-consumer queue based parallel
 * processing utility. <br>
 * The configuration methods are not threadsafe, but the {@link #add(Object)} is
 * threadsafe. <br>
 * Although there are safeties in the {@link #start()} and {@link #close()}
 * methods to prevent multiple invocations, you should confirm that
 * {@link #start()} is complete before calling {@link #add(Object)} and you
 * should confirm that all {@link #add(Object)} calls are complete before
 * calling {@link #close()}.
 * 
 * @author Peter Ansell p_ansell@yahoo.com
 */
public final class JParallel<P, C> implements AutoCloseable {

	private final Logger logger = LoggerFactory.getLogger(this.getClass());

	private int inputProcessors = Math.max(1, Runtime.getRuntime().availableProcessors() - 1);
	private int outputConsumers = 1;
	private int inputQueueSize = inputProcessors * 10;
	private int outputQueueSize = inputProcessors * 10;
	private BlockingQueue<P> inputQueue = null;
	private ExecutorService inputExecutor;
	private Function<P, C> processorFunction;
	private BlockingQueue<C> outputQueue = null;
	private ExecutorService outputExecutor;
	private Consumer<C> consumerFunction;
	private int threadPriority = Thread.NORM_PRIORITY;
	private String threadNameFormat = "jparallel-thread-%d";
	private UncaughtExceptionHandler uncaughtExceptionHandler = (t, e) -> {
		logger.error("Uncaught exception occurred in thread: " + t.getName(), e);
	};
	private long queueWaitTime = 0;
	private TimeUnit queueWaitUnit = TimeUnit.MILLISECONDS;
	private long terminationWaitTime = 1;
	private TimeUnit terminationWaitUnit = TimeUnit.MINUTES;

	private int queueCloseRetries = 5;
	private long queueCloseRetrySleep = 1;

	private final AtomicBoolean started = new AtomicBoolean(false);
	private final CountDownLatch startCompleted = new CountDownLatch(1);
	private final AtomicBoolean closed = new AtomicBoolean(false);

	@SuppressWarnings("unchecked")
	private final P inputSentinel = (P) new Object();
	@SuppressWarnings("unchecked")
	private final C outputSentinel = (C) new Object();

	/**
	 * Only accessed through the builder pattern.
	 * 
	 * @see JParallel#forFunctions(Function, Consumer)
	 */
	private JParallel(final Function<P, C> inputProcessor, final Consumer<C> outputConsumer) {
		this.processorFunction = inputProcessor;
		this.consumerFunction = outputConsumer;
	}

	/**
	 * Constructor function for all uses of JParallel. Takes an input processor
	 * function and an output consumer function.
	 * 
	 * @param inputProcessor
	 *            The function to transform input objects to output objects.
	 * @param outputConsumer
	 *            The function to send the outputs to.
	 * @return An instance of JParallel that can be configured and started,
	 *         before calling the {@link #add(Object)} method to send items
	 *         through the parallel processing.
	 */
	public static <P, C> JParallel<P, C> forFunctions(final Function<P, C> inputProcessor,
			final Consumer<C> outputConsumer) {
		return new JParallel<P, C>(Objects.requireNonNull(inputProcessor, "Processor function code must not be null"),
				Objects.requireNonNull(outputConsumer, "Consumer function code must not be null"));
	}

	/**
	 * The concurrency level to use when constructing threads for processing
	 * inputs.
	 * 
	 * @param inputProcessors
	 *            The number of threads to use for processing inputs.
	 * @return This object, for fluent programming
	 */
	public final JParallel<P, C> inputProcessors(final int inputProcessors) {
		checkState();

		if (inputProcessors < 1) {
			throw new IllegalArgumentException("Input processor concurrency level must be positive.");
		}

		this.inputProcessors = inputProcessors;
		return this;
	}

	/**
	 * The concurrency level to use when constructing threads for consuming
	 * outputs.
	 * 
	 * @param outputConsumers
	 *            The number of threads to use for consuming outputs.
	 * @return This object, for fluent programming
	 */
	public final JParallel<P, C> outputConsumers(final int outputConsumers) {
		checkState();

		if (outputConsumers < 1) {
			throw new IllegalArgumentException("Output consumer concurrency level must be positive.");
		}

		this.outputConsumers = outputConsumers;
		return this;
	}

	/**
	 * Sets up a fixed input queue for sizes greater than 0, or 0 to use an
	 * unbounded queue.
	 * 
	 * @param inputQueueSize
	 *            The input queue size to use, or 0 to use an unbounded queue
	 * @return This object, for fluent programming
	 */
	public final JParallel<P, C> inputBuffer(final int inputQueueSize) {
		checkState();

		if (inputQueueSize < 0) {
			throw new IllegalArgumentException("Input queue size must be non-negative.");
		}

		this.inputQueueSize = inputQueueSize;
		return this;
	}

	/**
	 * Sets up a fixed output queue for sizes greater than 0, or 0 to use an
	 * unbounded queue.
	 * 
	 * @param outputQueueSize
	 *            The output queue size to use, or 0 to use an unbounded queue
	 * @return This object, for fluent programming
	 */
	public final JParallel<P, C> outputBuffer(final int outputQueueSize) {
		checkState();

		if (outputQueueSize < 0) {
			throw new IllegalArgumentException("Output queue size must be non-negative.");
		}

		this.outputQueueSize = outputQueueSize;
		return this;
	}

	/**
	 * A custom thread name format to use for the {@link Thread#getName()}. Can
	 * be parameterised with <code>%d</code> to add a number into the thread
	 * name.
	 * 
	 * @param threadNameFormat
	 *            A custom thread name format
	 * @return This object, for fluent programming
	 */
	public final JParallel<P, C> threadNameFormat(final String threadNameFormat) {
		checkState();

		this.threadNameFormat = Objects.requireNonNull(threadNameFormat, "Thread name format must not be null");
		return this;
	}

	/**
	 * Set a custom thread priority to use, between {@link Thread#MIN_PRIORITY}
	 * and {@value Thread#MAX_PRIORITY}. Defaults to
	 * {@link Thread#NORM_PRIORITY}.
	 * 
	 * @param threadPriority
	 *            The thread priority to use
	 * @return This object, for fluent programming
	 */
	public final JParallel<P, C> threadPriority(final int threadPriority) {
		checkState();

		int correctedThreadPriority = threadPriority;
		if (Math.max(threadPriority, Thread.MIN_PRIORITY) == Thread.MIN_PRIORITY) {
			correctedThreadPriority = Thread.MIN_PRIORITY;
		} else if (Math.min(threadPriority, Thread.MAX_PRIORITY) == Thread.MAX_PRIORITY) {
			correctedThreadPriority = Thread.MAX_PRIORITY;
		}

		this.threadPriority = correctedThreadPriority;
		return this;
	}

	/**
	 * Sets an {@link UncaughtExceptionHandler} to use for responding to
	 * exceptions that are thrown by threads spawned internally, but otherwise
	 * not accessible to the main thread.
	 * 
	 * @param uncaughtExceptionHandler
	 *            A handler for uncaught exceptions in internally spawned
	 *            threads
	 * @return This object, for fluent programming
	 */
	public final JParallel<P, C> uncaughtExceptionHandler(final UncaughtExceptionHandler uncaughtExceptionHandler) {
		checkState();

		this.uncaughtExceptionHandler = uncaughtExceptionHandler;
		return this;
	}

	/**
	 * The amount of time to wait for space in a queue, during
	 * {@link #add(Object)} before failing.
	 * 
	 * @param terminationWaitTime
	 *            The time to wait
	 * @param terminationWaitUnit
	 *            The {@link TimeUnit} that specifies the units for the wait
	 *            time
	 * @return This object, for fluent programming
	 */
	public final JParallel<P, C> queueWaitTime(final long queueWaitTime, final TimeUnit queueWaitUnit) {
		checkState();

		if (queueWaitTime < 0) {
			throw new IllegalArgumentException("Queue wait time must be non-negative.");
		}

		this.queueWaitTime = queueWaitTime;
		this.queueWaitUnit = Objects.requireNonNull(queueWaitUnit, "Queue wait time unit must not be null");
		return this;
	}

	/**
	 * The amount of time to wait for each of the {@link ExecutorService}
	 * objects internally to shutdown.
	 * 
	 * @param terminationWaitTime
	 *            The time to wait
	 * @param terminationWaitUnit
	 *            The {@link TimeUnit} that specifies the units for the wait
	 *            time
	 * @return This object, for fluent programming
	 */
	public final JParallel<P, C> terminationWaitTime(final long terminationWaitTime,
			final TimeUnit terminationWaitUnit) {
		checkState();

		if (terminationWaitTime < 0) {
			throw new IllegalArgumentException("Termination wait time must be non-negative.");
		}

		this.terminationWaitTime = terminationWaitTime;
		this.terminationWaitUnit = Objects.requireNonNull(terminationWaitUnit,
				"Termination wait time unit must not be null");
		return this;
	}

	/**
	 * Specify the number of times to retry closing the queue during the
	 * {@link #close()} method, and the amount of time to sleep between retries.
	 * 
	 * @param queueCloseRetries
	 *            The number of times to retry
	 * @param queueCloseRetrySleep
	 *            The time to sleep between retries, in milliseconds.
	 * @return This object, for fluent programming
	 */
	public final JParallel<P, C> queueCloseRetries(final int queueCloseRetries, final long queueCloseRetrySleep) {
		checkState();

		if (queueCloseRetries < 0) {
			throw new IllegalArgumentException("Queue close retries must be non-negative.");
		}
		if (queueCloseRetrySleep < 0) {
			throw new IllegalArgumentException("Queue close retry sleep time must be non-negative.");
		}

		this.queueCloseRetries = queueCloseRetries;
		this.queueCloseRetrySleep = queueCloseRetrySleep;
		return this;
	}

	/**
	 * Constructs the internal {@link Queue} and {@link ExecutorService} objects
	 * and makes them reason to accept objects using the {@link #add(Object)}
	 * method.
	 * 
	 * @return This object, for fluent programming
	 */
	public final JParallel<P, C> start() {
		if (this.started.compareAndSet(false, true)) {
			try {
				if (this.logger.isInfoEnabled()) {
					this.logger.info("Setup called on : {}", this.toString());
				}
				if (this.inputQueueSize > 0) {
					this.inputQueue = new ArrayBlockingQueue<>(this.inputQueueSize);
				} else {
					this.inputQueue = new LinkedBlockingQueue<>();
				}

				if (this.outputQueueSize > 0) {
					this.outputQueue = new ArrayBlockingQueue<>(this.outputQueueSize);
				} else {
					this.outputQueue = new LinkedBlockingQueue<>();
				}

				ThreadFactory nextThreadFactory = new ThreadFactoryBuilder()
						.setUncaughtExceptionHandler(this.uncaughtExceptionHandler).setNameFormat(this.threadNameFormat)
						.setPriority(this.threadPriority).build();
				this.inputExecutor = Executors.newFixedThreadPool(this.inputProcessors, nextThreadFactory);
				addInputProcessors();
				this.outputExecutor = Executors.newFixedThreadPool(this.outputConsumers, nextThreadFactory);
				addOutputConsumers();
			} finally {
				this.startCompleted.countDown();
			}
		}

		return this;
	}

	/**
	 * Checks that this object has both not been closed or started to enforce
	 * the immutable pattern after starting/closing.
	 * 
	 * @throws IllegalStateException
	 *             If the object has been either closed or started.
	 */
	private final void checkState() {
		if (this.closed.get()) {
			throw new IllegalStateException("Already closed");
		}
		if (this.started.get()) {
			throw new IllegalStateException("Already started");
		}
	}

	/**
	 * Call this from the supplier threads to add items to the processing queue.
	 * <br>
	 * This method will block if the input queue is a fixed size queue and the
	 * queue is full, otherwise it will return immediately after adding the item
	 * to the queue.
	 * 
	 * @param toProcess
	 *            The item to be processed, must not be null.
	 * @return This object, for fluent programming, although typically this
	 *         method will be called separately and the return value ignored
	 * @throws IllegalStateException
	 *             If closed has been called or start has not been called before
	 *             this method is called.
	 * @throws NullPointerException
	 *             If the object to process is null
	 */
	public final JParallel<P, C> add(final P toProcess) {
		if (this.closed.get()) {
			throw new IllegalStateException("Already closed");
		}
		if (!this.started.get()) {
			throw new IllegalStateException("Start was not called");
		}
		Objects.requireNonNull(toProcess, "Processing items cannot be null");
		try {
			this.startCompleted.await();
			if (Thread.currentThread().isInterrupted()) {
				this.close();
				throwShutdownException();
			}

			if (this.queueWaitTime > 0) {
				this.inputQueue.offer(toProcess, this.queueWaitTime, this.queueWaitUnit);
			} else if (this.inputQueueSize > 0) {
				this.inputQueue.put(toProcess);
			} else {
				this.inputQueue.add(toProcess);
			}
		} catch (InterruptedException e) {
			Thread.currentThread().interrupt();
			this.close();
			throwShutdownException(e);
		}

		return this;
	}

	@Override
	public final void close() {
		if (this.closed.compareAndSet(false, true)) {
			try {
				try {
					// Add one copy of the sentinel for each processor
					for (int i = 0; i < this.inputProcessors; i++) {
						int nextRetryCount = 0;
						while (!this.inputQueue.offer(this.inputSentinel) && nextRetryCount < this.queueCloseRetries) {
							Thread.sleep(this.queueCloseRetrySleep);
							nextRetryCount++;
						}
						if (nextRetryCount >= this.queueCloseRetries) {
							this.logger.warn("Failed to add sentinel to input queue after {} retries",
									this.queueCloseRetries);
						}
					}
				} finally {
					try {
						// Wait for the input processors to complete normally
						this.inputExecutor.shutdown();
						this.inputExecutor.awaitTermination(this.terminationWaitTime, this.terminationWaitUnit);
					} finally {
						try {
							// Add one copy of the sentinel for each consumer
							for (int i = 0; i < this.outputConsumers; i++) {
								int nextRetryCount = 0;
								while (!this.outputQueue.offer(this.outputSentinel)
										&& nextRetryCount < this.queueCloseRetries) {
									Thread.sleep(this.queueCloseRetrySleep);
									nextRetryCount++;
								}
								if (nextRetryCount >= this.queueCloseRetries) {
									this.logger.warn("Failed to add sentinel to output queue after {} retries",
											this.queueCloseRetries);
								}
							}
						} finally {
							try {
								// Wait for the output consumers to complete
								// normally
								this.outputExecutor.shutdown();
								this.outputExecutor.awaitTermination(this.terminationWaitTime,
										this.terminationWaitUnit);
							} finally {
								try {
									// If input termination didn't occur
									// normally, hard shutdown
									if (!this.inputExecutor.isTerminated()) {
										List<Runnable> shutdownNow = this.inputExecutor.shutdownNow();
										if (!shutdownNow.isEmpty()) {
											throwShutdownException(
													new RuntimeException("There were " + shutdownNow.size()
															+ " input executors that failed to shutdown in time."));
										}
									}
								} finally {
									// If output termination didn't occur
									// normally, hard shutdown
									if (!this.outputExecutor.isTerminated()) {
										List<Runnable> shutdownNow = this.outputExecutor.shutdownNow();
										if (!shutdownNow.isEmpty()) {
											throwShutdownException(
													new RuntimeException("There were " + shutdownNow.size()
															+ " output executors that failed to shutdown in time."));
										}
									}
								}
							}
						}
					}
				}
			} catch (InterruptedException e) {
				Thread.currentThread().interrupt();
				throwShutdownException(e);
			}
		}
	}

	@Override
	public final String toString() {
		return "JParallel [inputQueueSize=" + inputQueueSize + ", outputQueueSize=" + outputQueueSize
				+ ", inputProcessors=" + inputProcessors + ", outputConsumers=" + outputConsumers + ", threadPriority="
				+ threadPriority + ", threadNameFormat=" + threadNameFormat + ", waitTime=" + terminationWaitTime
				+ ", waitUnit=" + terminationWaitUnit + "]";
	}

	private final void addInputProcessors() {
		for (int i = 0; i < this.inputProcessors; i++) {
			this.inputExecutor.submit(() -> {
				try {
					while (true) {
						if (Thread.currentThread().isInterrupted()) {
							close();
							throwShutdownException();
							break;
						}
						P take = this.inputQueue.take();
						if (take == null || take == this.inputSentinel) {
							break;
						}

						C toConsume = this.processorFunction.apply(take);

						// Null return from functionCode indicates that we don't
						// consume the result
						if (toConsume != null) {
							if (this.queueWaitTime > 0) {
								this.outputQueue.offer(toConsume, this.queueWaitTime, this.queueWaitUnit);
							} else if (this.outputQueueSize > 0) {
								this.outputQueue.put(toConsume);
							} else {
								this.outputQueue.add(toConsume);
							}
						}
					}
				} catch (InterruptedException e) {
					Thread.currentThread().interrupt();
					close();
					throwShutdownException();
				}
			});
		}
	}

	private final void addOutputConsumers() {
		for (int i = 0; i < this.outputConsumers; i++) {
			this.outputExecutor.submit(() -> {
				try {
					while (true) {
						if (Thread.currentThread().isInterrupted()) {
							close();
							throwShutdownException();
							break;
						}

						C toConsume = this.outputQueue.take();

						if (toConsume == null || toConsume == this.outputSentinel) {
							break;
						}
						this.consumerFunction.accept(toConsume);
					}
				} catch (InterruptedException e) {
					Thread.currentThread().interrupt();
					close();
					throwShutdownException(e);
				}
			});
		}
	}

	private final void throwShutdownException() {
		RuntimeException toThrow = new RuntimeException("Execution was interrupted");
		this.logger.error("Shutdown exception occurred", toThrow);
		throw toThrow;
	}

	private final void throwShutdownException(Throwable e) {
		RuntimeException toThrow = new RuntimeException("Execution was interrupted", e);
		this.logger.error("Shutdown exception occurred", toThrow);
		throw toThrow;
	}
}
