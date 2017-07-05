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
 * The configuration methods are not threadsafe, but the {@link #start()}
 * {@link #add(Object)} and {@link #close()} methods are all threadsafe
 * individually. <br>
 * Although there are safeties in the {@link #start()} and {@link #close()}
 * methods to prevent the effects of multiple invocations, you should confirm
 * that all {@link #add(Object)} calls are complete before calling
 * {@link #close()}.
 * 
 * @author Peter Ansell p_ansell@yahoo.com
 * @param <P>
 *            The type of the input objects to be processed
 * @param <C>
 *            The type of the output objects to be consumed
 */
public final class JParallel<P, C> implements AutoCloseable {

	private final Logger logger = LoggerFactory.getLogger(this.getClass());

	// Default to either 1 or the number of available processors - 1, whichever
	// is greater
	private int inputProcessors = Math.max(1, Runtime.getRuntime().availableProcessors() - 1);
	// Default presumes that output consuming requires less resources than input
	private int outputConsumers = 1;
	// Default queue sizes are both based on the default number of input
	// processors
	private int inputQueueSize = inputProcessors * 10;
	private int outputQueueSize = inputProcessors * 10;

	// Both queues are created when start is called, after the configuration is
	// complete
	private BlockingQueue<P> inputQueue = null;
	private BlockingQueue<C> outputQueue = null;

	// Both executors are created when start is called, after the configuration
	// is complete
	private ExecutorService inputExecutor;
	private ExecutorService outputExecutor;

	// These functions must be given to the constructor
	private Function<P, C> processorFunction;
	private Consumer<C> consumerFunction;

	// These functions are overridable, but default to being logged
	private Consumer<P> inputQueueFailureFunction = p -> logger
			.error("Input queue failed to accept offered item within time allowed.");
	private Consumer<C> outputQueueFailureFunction = c -> logger
			.error("Output queue failed to accept offered item within time allowed.");

	// Thread configuration for the threads used by the executors
	private int threadPriority = Thread.NORM_PRIORITY;
	private String threadNameFormat = "jparallel-thread-%d";
	private UncaughtExceptionHandler uncaughtExceptionHandler = (nextThread, nextException) -> {
		logger.error("Uncaught exception occurred in thread: " + nextThread.getName(), nextException);
	};

	private long inputQueueWaitTime = 10;
	private TimeUnit inputQueueWaitUnit = TimeUnit.SECONDS;

	private long outputQueueWaitTime = 20;
	private TimeUnit outputQueueWaitUnit = TimeUnit.SECONDS;

	private long terminationWaitTime = 30;
	private TimeUnit terminationWaitUnit = TimeUnit.SECONDS;

	private int queueCloseRetries = 1;
	private long queueCloseRetrySleep = 5;
	private TimeUnit queueCloseRetrySleepTimeUnit = TimeUnit.SECONDS;

	// Tracks the running state
	private final AtomicBoolean started = new AtomicBoolean(false);
	private final CountDownLatch startCompleted = new CountDownLatch(1);
	private final AtomicBoolean closed = new AtomicBoolean(false);

	// Internal verification that a programming bug here should not accidentally
	// add multiples of the required consumers
	private final AtomicBoolean inputProcessorsInitialised = new AtomicBoolean(false);
	private final AtomicBoolean outputConsumersInitialised = new AtomicBoolean(false);

	@SuppressWarnings("unchecked")
	private final P inputSentinel = (P) new Object();
	@SuppressWarnings("unchecked")
	private final C outputSentinel = (C) new Object();

	/**
	 * Only accessed through the builder pattern.
	 * 
	 * @param inputProcessor
	 *            The function to transform input objects to output objects.
	 * @param outputConsumer
	 *            The function to send the outputs to.
	 * @throws NullPointerException
	 *             if any parameter is null
	 * @see JParallel#forFunctions(Function, Consumer)
	 */
	private JParallel(final Function<P, C> inputProcessor, final Consumer<C> outputConsumer) {
		this.processorFunction = Objects.requireNonNull(inputProcessor, "Processor function code must not be null");
		this.consumerFunction = Objects.requireNonNull(outputConsumer, "Consumer function code must not be null");
	}

	/**
	 * Constructor function for all uses of JParallel. Takes an input processor
	 * function and an output consumer function.
	 * 
	 * @param inputProcessor
	 *            The function to transform input objects to output objects.
	 * @param outputConsumer
	 *            The function to send the outputs to.
	 * @param <P>
	 *            The type of the input objects to be processed
	 * @param <C>
	 *            The type of the output objects to be consumed
	 * @return An instance of JParallel that can be configured and started,
	 *         before calling the {@link #add(Object)} method to send items
	 *         through the parallel processing.
	 * @throws NullPointerException
	 *             if any parameter is null
	 */
	public static <P, C> JParallel<P, C> forFunctions(final Function<P, C> inputProcessor,
			final Consumer<C> outputConsumer) {
		return new JParallel<P, C>(inputProcessor, outputConsumer);
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
		checkStateBeforeMutator();

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
		checkStateBeforeMutator();

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
		checkStateBeforeMutator();

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
		checkStateBeforeMutator();

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
		checkStateBeforeMutator();

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
		checkStateBeforeMutator();

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
		checkStateBeforeMutator();

		this.uncaughtExceptionHandler = Objects.requireNonNull(uncaughtExceptionHandler,
				"Uncaught exception handler must not be null");

		return this;
	}

	/**
	 * The amount of time to wait for space in the input queue, during
	 * {@link #add(Object)} before failing.
	 * 
	 * @param inputQueueWaitTime
	 *            The time to wait
	 * @param inputQueueWaitUnit
	 *            The {@link TimeUnit} that specifies the units for the wait
	 *            time
	 * @return This object, for fluent programming
	 */
	public final JParallel<P, C> inputQueueWaitTime(final long inputQueueWaitTime, final TimeUnit inputQueueWaitUnit) {
		checkStateBeforeMutator();

		if (inputQueueWaitTime < 0) {
			throw new IllegalArgumentException("Input queue wait time must be non-negative.");
		}

		this.inputQueueWaitTime = inputQueueWaitTime;
		this.inputQueueWaitUnit = Objects.requireNonNull(inputQueueWaitUnit,
				"Input queue wait time unit must not be null");
		return this;
	}

	/**
	 * The amount of time to wait for space in the output queue, during the
	 * processing phase before failing.
	 * 
	 * @param inputQueueWaitTime
	 *            The time to wait
	 * @param inputQueueWaitUnit
	 *            The {@link TimeUnit} that specifies the units for the wait
	 *            time
	 * @return This object, for fluent programming
	 */
	public final JParallel<P, C> outputQueueWaitTime(final long outputQueueWaitTime,
			final TimeUnit outputQueueWaitUnit) {
		checkStateBeforeMutator();

		if (outputQueueWaitTime < 0) {
			throw new IllegalArgumentException("Output queue wait time must be non-negative.");
		}

		this.outputQueueWaitTime = outputQueueWaitTime;
		this.outputQueueWaitUnit = Objects.requireNonNull(outputQueueWaitUnit,
				"Output queue wait time unit must not be null");
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
		checkStateBeforeMutator();

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
	 *            The time to sleep between retries, in the time units given.
	 * @param queueCloseRetrySleepTimeUnit
	 *            The time units used to interpret the queueCloseRetrySleep
	 *            parameter.
	 * @return This object, for fluent programming
	 */
	public final JParallel<P, C> queueCloseRetries(final int queueCloseRetries, final long queueCloseRetrySleep,
			final TimeUnit queueCloseRetrySleepTimeUnit) {
		checkStateBeforeMutator();

		if (queueCloseRetries < 0) {
			throw new IllegalArgumentException("Queue close retries must be non-negative.");
		}
		if (queueCloseRetrySleep < 0) {
			throw new IllegalArgumentException("Queue close retry sleep time must be non-negative.");
		}
		if (queueCloseRetrySleepTimeUnit == null) {
			throw new IllegalArgumentException("Queue close retry sleep time unit must not be null");
		}

		this.queueCloseRetries = queueCloseRetries;
		this.queueCloseRetrySleep = queueCloseRetrySleep;
		this.queueCloseRetrySleepTimeUnit = queueCloseRetrySleepTimeUnit;
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
	private final void checkStateBeforeMutator() {
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
			// Trigger start ourselves if it was not called already
			start();
		}
		Objects.requireNonNull(toProcess, "Processing items cannot be null");
		try {
			this.startCompleted.await();
			if (Thread.currentThread().isInterrupted()) {
				this.close();
				throwShutdownException();
			}

			if (this.inputQueueWaitTime > 0) {
				boolean offer = this.inputQueue.offer(toProcess, this.inputQueueWaitTime, this.inputQueueWaitUnit);
				if (!offer) {
					inputQueueFailureFunction.accept(toProcess);
				}
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
		if (!this.started.get()) {
			throw new IllegalStateException("Start was not called");
		}
		if (this.closed.compareAndSet(false, true)) {
			try {
				try {
					// Add one copy of the sentinel for each processor
					for (int i = 0; i < this.inputProcessors; i++) {
						int nextRetryCount = 0;
						while (!this.inputQueue.offer(this.inputSentinel, this.queueCloseRetrySleep,
								this.queueCloseRetrySleepTimeUnit) && !Thread.currentThread().isInterrupted()
								&& nextRetryCount < this.queueCloseRetries) {
							nextRetryCount++;
						}
						if (nextRetryCount >= this.queueCloseRetries) {
							this.logger.warn("Failed to add sentinel to input queue after {} retries",
									this.queueCloseRetries);
							this.inputQueue.clear();
							int nextEmergencyRetryCount = 0;
							while (!this.inputQueue.offer(this.inputSentinel)
									&& nextEmergencyRetryCount < this.queueCloseRetries) {
								nextEmergencyRetryCount++;
								this.inputQueue.clear();
							}
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
								while (!this.outputQueue.offer(this.outputSentinel, this.queueCloseRetrySleep,
										this.queueCloseRetrySleepTimeUnit) && !Thread.currentThread().isInterrupted()
										&& nextRetryCount < this.queueCloseRetries) {
									nextRetryCount++;
								}
								if (nextRetryCount >= this.queueCloseRetries) {
									this.logger.warn("Failed to add sentinel to output queue after {} retries",
											this.queueCloseRetries);
									this.inputQueue.clear();
									int nextEmergencyRetryCount = 0;
									while (!this.outputQueue.offer(this.outputSentinel)
											&& nextEmergencyRetryCount < this.queueCloseRetries) {
										nextEmergencyRetryCount++;
										this.outputQueue.clear();
									}
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
		if (inputProcessorsInitialised.compareAndSet(false, true)) {
			for (int i = 0; i < this.inputProcessors; i++) {
				this.inputExecutor.submit(() -> {
					try {
						while (true) {
							if (Thread.currentThread().isInterrupted()) {
								close();
								throwShutdownException();
								break;
							}
							P take = this.inputQueue.poll(this.inputQueueWaitTime, this.inputQueueWaitUnit);
							if (take == null) {
								// Continue back to the start of the loop,
								// checking the interrupt status before looking
								// for more inputs
								continue;
							}
							if (take == this.inputSentinel) {
								// This is our cue to leave
								break;
							}

							C toConsume = this.processorFunction.apply(take);

							// Null return from functionCode indicates that we
							// don't consume the result
							if (toConsume != null) {
								if (this.outputQueueWaitTime > 0) {
									boolean offer = this.outputQueue.offer(toConsume, this.outputQueueWaitTime,
											this.outputQueueWaitUnit);
									if (!offer) {
										outputQueueFailureFunction.accept(toConsume);
									}
								} else if (this.outputQueueSize > 0) {
									this.outputQueue.put(toConsume);
								} else {
									try {
										this.outputQueue.add(toConsume);
									} catch (IllegalStateException e) {
										outputQueueFailureFunction.accept(toConsume);
									}
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
	}

	private final void addOutputConsumers() {
		if (outputConsumersInitialised.compareAndSet(false, true)) {
			for (int i = 0; i < this.outputConsumers; i++) {
				this.outputExecutor.submit(() -> {
					try {
						while (true) {
							if (Thread.currentThread().isInterrupted()) {
								close();
								throwShutdownException();
								break;
							}

							C toConsume = this.outputQueue.poll(this.outputQueueWaitTime, this.outputQueueWaitUnit);

							if (toConsume == null) {
								// Continue back to the start of the loop,
								// checking the interrupt status before looking
								// for more inputs
								continue;
							}
							if (toConsume == this.outputSentinel) {
								// This is our cue to leave
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
