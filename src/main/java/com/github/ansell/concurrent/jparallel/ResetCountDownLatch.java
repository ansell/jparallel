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

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

/**
 * A version of {@link CountDownLatch} that is resettable.
 * 
 * Overrides CountDownLatch so that this can be used as a drop in replacement
 * wherever CountDownLatch is required by an API, but delegates all calls to
 * another copy.
 * 
 * Resetting the latch releases all threads waiting on the current latch, after
 * atomically replacing the internal delegate with a fresh instance of
 * CountDownLatch.
 * 
 * @author Peter Ansell p_ansell@yahoo.com
 */
public final class ResetCountDownLatch extends CountDownLatch {

	private final int resetValue;
	private final AtomicReference<CountDownLatch> delegate;

	public ResetCountDownLatch(int count) {
		super(count);
		resetValue = count;
		delegate = new AtomicReference<>(new CountDownLatch(count));
	}

	/**
	 * Replaces the internal CountDownLatch with a brand new latch, using the
	 * same initial value was the previous copy.
	 * 
	 * The previous latch is counted down to release all threads waiting on it
	 * after replacing the existing latch atomically with another copy.
	 * 
	 * IMPORTANT: Only call this if you know the releasing of all existing
	 * threads blocked by calls to either {@link #await()} or
	 * {@link #await(long, TimeUnit)} is appropriate.
	 */
	public void reset() {
		CountDownLatch retiredLatch = delegate.getAndSet(new CountDownLatch(resetValue));
		if (retiredLatch != null) {
			// Count down to get the retired latch back to zero and release any
			// threads waiting on it
			// Multiple calls to countDown after it reaches 0 are safe, making
			// it
			// unnecessary to synchronise here
			for (int count = 0; count < resetValue; count++) {
				retiredLatch.countDown();
			}
		}
	}

	@Override
	public void await() throws InterruptedException {
		delegate.get().await();
	}

	@Override
	public boolean await(long timeout, TimeUnit unit) throws InterruptedException {
		return delegate.get().await(timeout, unit);
	}

	@Override
	public void countDown() {
		delegate.get().countDown();
	}

	@Override
	public long getCount() {
		return delegate.get().getCount();
	}

	@Override
	public String toString() {
		return delegate.get().toString();
	}

}
