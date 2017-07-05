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

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.Test;

/**
 * Tests for {@link ResetCountDownLatch}.
 * 
 * @author Peter Ansell p_ansell@yahoo.com
 */
public class ResetCountDownLatchTest {

	@Test
	public final void testNoReset() throws Exception {
		AtomicBoolean threadInterruptSuccess = new AtomicBoolean(false);
		AtomicBoolean threadInterruptFail = new AtomicBoolean(false);
		AtomicBoolean threadNormalSuccess = new AtomicBoolean(false);
		ResetCountDownLatch testLatch = new ResetCountDownLatch(2);

		Thread testInterruptThread = new Thread(() -> {
			try {
				testLatch.await();
				threadInterruptFail.set(true);
			} catch (InterruptedException e) {
				threadInterruptSuccess.set(true);
				e.printStackTrace();
				Thread.currentThread().interrupt();
				throw new RuntimeException(e);
			}
		});
		testInterruptThread.start();

		Thread testNormalThread = new Thread(() -> {
			try {
				testLatch.await();
				threadNormalSuccess.set(true);
			} catch (InterruptedException e) {
				e.printStackTrace();
				Thread.currentThread().interrupt();
				throw new RuntimeException(e);
			}
		});
		testNormalThread.start();

		Thread.sleep(100);
		assertEquals(2L, testLatch.getCount());
		assertFalse(threadInterruptSuccess.get());
		assertFalse(threadInterruptFail.get());
		assertFalse(threadNormalSuccess.get());

		// Verify that counting down once doesn't cause either thread to
		// complete
		testLatch.countDown();
		Thread.sleep(100);
		assertEquals(1L, testLatch.getCount());
		assertFalse(threadInterruptSuccess.get());
		assertFalse(threadInterruptFail.get());
		assertFalse(threadNormalSuccess.get());

		// Verify that interrupting the thread doesn't change
		// ResetCountDownLatch
		testInterruptThread.interrupt();
		Thread.sleep(100);
		assertEquals(1L, testLatch.getCount());
		assertTrue(threadInterruptSuccess.get());
		assertFalse(threadInterruptFail.get());
		assertFalse(threadNormalSuccess.get());

		// Verify that the thread now makes it past the call to await without
		// throwing an exception
		testLatch.countDown();
		Thread.sleep(100);
		assertEquals(0L, testLatch.getCount());
		assertTrue(threadInterruptSuccess.get());
		assertFalse(threadInterruptFail.get());
		assertTrue(threadNormalSuccess.get());

		assertTrue("The toString representation should be non-empty", testLatch.toString().length() > 0);
	}

	@Test
	public final void testNoResetAwaitWithTimeout() throws Exception {
		AtomicBoolean threadInterruptSuccess = new AtomicBoolean(false);
		AtomicBoolean threadInterruptFail = new AtomicBoolean(false);
		AtomicBoolean threadNormalSuccess = new AtomicBoolean(false);
		ResetCountDownLatch testLatch = new ResetCountDownLatch(2);

		Thread testInterruptThread = new Thread(() -> {
			try {
				testLatch.await(1, TimeUnit.MINUTES);
				threadInterruptFail.set(true);
			} catch (InterruptedException e) {
				threadInterruptSuccess.set(true);
				e.printStackTrace();
				Thread.currentThread().interrupt();
				throw new RuntimeException(e);
			}
		});
		testInterruptThread.start();

		Thread testNormalThread = new Thread(() -> {
			try {
				testLatch.await(1, TimeUnit.MINUTES);
				threadNormalSuccess.set(true);
			} catch (InterruptedException e) {
				e.printStackTrace();
				Thread.currentThread().interrupt();
				throw new RuntimeException(e);
			}
		});
		testNormalThread.start();

		Thread.sleep(100);
		assertEquals(2L, testLatch.getCount());
		assertFalse(threadInterruptSuccess.get());
		assertFalse(threadInterruptFail.get());
		assertFalse(threadNormalSuccess.get());

		// Verify that counting down once doesn't cause either thread to
		// complete
		testLatch.countDown();
		Thread.sleep(100);
		assertEquals(1L, testLatch.getCount());
		assertFalse(threadInterruptSuccess.get());
		assertFalse(threadInterruptFail.get());
		assertFalse(threadNormalSuccess.get());

		// Verify that interrupting the thread doesn't change
		// ResetCountDownLatch
		testInterruptThread.interrupt();
		Thread.sleep(100);
		assertEquals(1L, testLatch.getCount());
		assertTrue(threadInterruptSuccess.get());
		assertFalse(threadInterruptFail.get());
		assertFalse(threadNormalSuccess.get());

		// Verify that the thread now makes it past the call to await without
		// throwing an exception
		testLatch.countDown();
		Thread.sleep(100);
		assertEquals(0L, testLatch.getCount());
		assertTrue(threadInterruptSuccess.get());
		assertFalse(threadInterruptFail.get());
		assertTrue(threadNormalSuccess.get());

		assertTrue("The toString representation should be non-empty", testLatch.toString().length() > 0);
	}

	@Test
	public final void testWithResetAfterCompletion() throws Exception {
		AtomicBoolean threadInterruptSuccess = new AtomicBoolean(false);
		AtomicBoolean threadInterruptFail = new AtomicBoolean(false);
		AtomicBoolean threadNormalSuccess = new AtomicBoolean(false);
		ResetCountDownLatch testLatch = new ResetCountDownLatch(2);

		Thread testInterruptThread = new Thread(() -> {
			try {
				testLatch.await();
				threadInterruptFail.set(true);
			} catch (InterruptedException e) {
				threadInterruptSuccess.set(true);
				e.printStackTrace();
				Thread.currentThread().interrupt();
				throw new RuntimeException(e);
			}
		});
		testInterruptThread.start();

		Thread testNormalThread = new Thread(() -> {
			try {
				testLatch.await();
				threadNormalSuccess.set(true);
			} catch (InterruptedException e) {
				e.printStackTrace();
				Thread.currentThread().interrupt();
				throw new RuntimeException(e);
			}
		});
		testNormalThread.start();

		Thread.sleep(100);
		assertEquals(2L, testLatch.getCount());
		assertFalse(threadInterruptSuccess.get());
		assertFalse(threadInterruptFail.get());
		assertFalse(threadNormalSuccess.get());

		// Verify that counting down once doesn't cause either thread to
		// complete
		testLatch.countDown();
		Thread.sleep(100);
		assertEquals(1L, testLatch.getCount());
		assertFalse(threadInterruptSuccess.get());
		assertFalse(threadInterruptFail.get());
		assertFalse(threadNormalSuccess.get());

		// Verify that interrupting the thread doesn't change
		// ResetCountDownLatch
		testInterruptThread.interrupt();
		Thread.sleep(100);
		assertEquals(1L, testLatch.getCount());
		assertTrue(threadInterruptSuccess.get());
		assertFalse(threadInterruptFail.get());
		assertFalse(threadNormalSuccess.get());

		// Verify that the thread now makes it past the call to await without
		// throwing an exception
		testLatch.countDown();
		Thread.sleep(100);
		assertEquals(0L, testLatch.getCount());
		assertTrue(threadInterruptSuccess.get());
		assertFalse(threadInterruptFail.get());
		assertTrue(threadNormalSuccess.get());

		// Now test the reset method to ensure the count goes back to 2 and we
		// can use await to wait for more countdowns
		testLatch.reset();
		assertEquals(2L, testLatch.getCount());

		AtomicBoolean threadAfterResetSuccess1 = new AtomicBoolean(false);
		AtomicBoolean threadAfterResetSuccess2 = new AtomicBoolean(false);

		Thread testAfterResetThread1 = new Thread(() -> {
			try {
				testLatch.await();
				threadAfterResetSuccess1.set(true);
			} catch (InterruptedException e) {
				e.printStackTrace();
				Thread.currentThread().interrupt();
				throw new RuntimeException(e);
			}
		});
		testAfterResetThread1.start();

		Thread testAfterResetThread2 = new Thread(() -> {
			try {
				testLatch.await();
				threadAfterResetSuccess2.set(true);
			} catch (InterruptedException e) {
				e.printStackTrace();
				Thread.currentThread().interrupt();
				throw new RuntimeException(e);
			}
		});
		testAfterResetThread2.start();

		Thread.sleep(100);
		assertEquals(2L, testLatch.getCount());
		assertFalse(threadAfterResetSuccess1.get());
		assertFalse(threadAfterResetSuccess2.get());

		// Count down once to verify proper behaviour after reset
		testLatch.countDown();
		Thread.sleep(100);
		assertEquals(1L, testLatch.getCount());
		assertFalse(threadAfterResetSuccess1.get());
		assertFalse(threadAfterResetSuccess2.get());

		testLatch.countDown();
		Thread.sleep(100);
		assertEquals(0L, testLatch.getCount());
		assertTrue(threadAfterResetSuccess1.get());
		assertTrue(threadAfterResetSuccess2.get());

		// Verify that another reset is possible and sets the count back to 2
		// again
		testLatch.reset();
		assertEquals(2L, testLatch.getCount());
	}

	@Test
	public final void testWithResetBeforeCountdownCompletes() throws Exception {
		AtomicBoolean threadInterruptSuccess = new AtomicBoolean(false);
		AtomicBoolean threadInterruptFail = new AtomicBoolean(false);
		AtomicBoolean threadNormalSuccess = new AtomicBoolean(false);
		ResetCountDownLatch testLatch = new ResetCountDownLatch(2);

		Thread testInterruptThread = new Thread(() -> {
			try {
				testLatch.await();
				threadInterruptFail.set(true);
			} catch (InterruptedException e) {
				threadInterruptSuccess.set(true);
				e.printStackTrace();
				Thread.currentThread().interrupt();
				throw new RuntimeException(e);
			}
		});
		testInterruptThread.start();

		Thread testNormalThread = new Thread(() -> {
			try {
				testLatch.await();
				threadNormalSuccess.set(true);
			} catch (InterruptedException e) {
				e.printStackTrace();
				Thread.currentThread().interrupt();
				throw new RuntimeException(e);
			}
		});
		testNormalThread.start();

		Thread.sleep(100);
		assertEquals(2L, testLatch.getCount());
		assertFalse(threadInterruptSuccess.get());
		assertFalse(threadInterruptFail.get());
		assertFalse(threadNormalSuccess.get());

		// Verify that counting down once doesn't cause either thread to
		// complete
		testLatch.countDown();
		Thread.sleep(100);
		assertEquals(1L, testLatch.getCount());
		assertFalse(threadInterruptSuccess.get());
		assertFalse(threadInterruptFail.get());
		assertFalse(threadNormalSuccess.get());

		// Verify that interrupting the thread doesn't change
		// ResetCountDownLatch
		testInterruptThread.interrupt();
		Thread.sleep(100);
		assertEquals(1L, testLatch.getCount());
		assertTrue(threadInterruptSuccess.get());
		assertFalse(threadInterruptFail.get());
		assertFalse(threadNormalSuccess.get());

		// Verify that resetting causes the existing awaits to be released
		testLatch.reset();
		Thread.sleep(100);
		// The latch should be back to two, and the threads that were blocked
		// should have succeeded normally
		assertEquals(2L, testLatch.getCount());
		assertTrue(threadInterruptSuccess.get());
		assertFalse(threadInterruptFail.get());
		assertTrue(threadNormalSuccess.get());

		AtomicBoolean threadAfterResetSuccess1 = new AtomicBoolean(false);
		AtomicBoolean threadAfterResetSuccess2 = new AtomicBoolean(false);

		Thread testAfterResetThread1 = new Thread(() -> {
			try {
				testLatch.await();
				threadAfterResetSuccess1.set(true);
			} catch (InterruptedException e) {
				e.printStackTrace();
				Thread.currentThread().interrupt();
				throw new RuntimeException(e);
			}
		});
		testAfterResetThread1.start();

		Thread testAfterResetThread2 = new Thread(() -> {
			try {
				testLatch.await();
				threadAfterResetSuccess2.set(true);
			} catch (InterruptedException e) {
				e.printStackTrace();
				Thread.currentThread().interrupt();
				throw new RuntimeException(e);
			}
		});
		testAfterResetThread2.start();

		Thread.sleep(100);
		assertEquals(2L, testLatch.getCount());
		assertFalse(threadAfterResetSuccess1.get());
		assertFalse(threadAfterResetSuccess2.get());

		// Count down once to verify proper behaviour after reset
		testLatch.countDown();
		Thread.sleep(100);
		assertEquals(1L, testLatch.getCount());
		assertFalse(threadAfterResetSuccess1.get());
		assertFalse(threadAfterResetSuccess2.get());

		// Call reset again to verify that it succeeds again
		testLatch.reset();
		Thread.sleep(100);
		assertEquals(2L, testLatch.getCount());
		assertTrue(threadAfterResetSuccess1.get());
		assertTrue(threadAfterResetSuccess2.get());
	}

	@Test
	public final void testWithResetBeforeCountdownCalled() throws Exception {
		AtomicBoolean threadInterruptSuccess = new AtomicBoolean(false);
		AtomicBoolean threadInterruptFail = new AtomicBoolean(false);
		AtomicBoolean threadNormalSuccess = new AtomicBoolean(false);
		ResetCountDownLatch testLatch = new ResetCountDownLatch(2);

		Thread testInterruptThread = new Thread(() -> {
			try {
				testLatch.await();
				threadInterruptFail.set(true);
			} catch (InterruptedException e) {
				threadInterruptSuccess.set(true);
				e.printStackTrace();
				Thread.currentThread().interrupt();
				throw new RuntimeException(e);
			}
		});
		testInterruptThread.start();

		Thread testNormalThread = new Thread(() -> {
			try {
				testLatch.await();
				threadNormalSuccess.set(true);
			} catch (InterruptedException e) {
				e.printStackTrace();
				Thread.currentThread().interrupt();
				throw new RuntimeException(e);
			}
		});
		testNormalThread.start();

		Thread.sleep(100);
		assertEquals(2L, testLatch.getCount());
		assertFalse(threadInterruptSuccess.get());
		assertFalse(threadInterruptFail.get());
		assertFalse(threadNormalSuccess.get());

		// Verify that interrupting the thread doesn't change
		// ResetCountDownLatch
		testInterruptThread.interrupt();
		Thread.sleep(100);
		assertEquals(2L, testLatch.getCount());
		assertTrue(threadInterruptSuccess.get());
		assertFalse(threadInterruptFail.get());
		assertFalse(threadNormalSuccess.get());

		// Verify that resetting causes the existing awaits to be released
		testLatch.reset();
		Thread.sleep(100);
		// The latch should be still reporting its count as two, but the threads
		// that were blocked should have succeeded normally
		assertEquals(2L, testLatch.getCount());
		assertTrue(threadInterruptSuccess.get());
		assertFalse(threadInterruptFail.get());
		assertTrue(threadNormalSuccess.get());

		AtomicBoolean threadAfterResetSuccess1 = new AtomicBoolean(false);
		AtomicBoolean threadAfterResetSuccess2 = new AtomicBoolean(false);

		Thread testAfterResetThread1 = new Thread(() -> {
			try {
				testLatch.await();
				threadAfterResetSuccess1.set(true);
			} catch (InterruptedException e) {
				e.printStackTrace();
				Thread.currentThread().interrupt();
				throw new RuntimeException(e);
			}
		});
		testAfterResetThread1.start();

		Thread testAfterResetThread2 = new Thread(() -> {
			try {
				testLatch.await();
				threadAfterResetSuccess2.set(true);
			} catch (InterruptedException e) {
				e.printStackTrace();
				Thread.currentThread().interrupt();
				throw new RuntimeException(e);
			}
		});
		testAfterResetThread2.start();

		Thread.sleep(100);
		assertEquals(2L, testLatch.getCount());
		assertFalse(threadAfterResetSuccess1.get());
		assertFalse(threadAfterResetSuccess2.get());

		// Call reset again to verify that it succeeds again
		testLatch.reset();
		Thread.sleep(100);
		assertEquals(2L, testLatch.getCount());
		assertTrue(threadAfterResetSuccess1.get());
		assertTrue(threadAfterResetSuccess2.get());
	}
}
