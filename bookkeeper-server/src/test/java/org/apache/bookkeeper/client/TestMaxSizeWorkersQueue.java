package org.apache.bookkeeper.client;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.Enumeration;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.bookkeeper.client.AsyncCallback.AddCallback;
import org.apache.bookkeeper.client.AsyncCallback.ReadCallback;
import org.apache.bookkeeper.client.BookKeeper.DigestType;
import org.apache.bookkeeper.test.BaseTestCase;
import org.junit.Test;

import com.google.common.collect.Lists;

public class TestMaxSizeWorkersQueue extends BaseTestCase {

    DigestType digestType;

    public TestMaxSizeWorkersQueue(DigestType digestType) {
        super(1);
        this.digestType = digestType;

        baseConf.setNumReadWorkerThreads(1);
        baseConf.setNumAddWorkerThreads(1);

        // Configure very small queue sizes
        baseConf.setMaxPendingReadRequestPerThread(1);
        baseConf.setMaxPendingAddRequestPerThread(1);
    }

    @Test(timeout = 60000)
    public void testReadRejected() throws Exception {
        LedgerHandle lh = bkc.createLedger(1, 1, digestType, new byte[0]);
        byte[] content = new byte[100];

        final int N = 1000;
        // Write few entries
        for (int i = 0; i < N; i++) {
            lh.addEntry(content);
        }

        // Read asynchronously:
        // - 1st read must always succeed
        // - Subsequent reads may fail, depending on timing
        // - At least few, we expect to fail with TooManyRequestException
        final CountDownLatch counter = new CountDownLatch(2);

        final AtomicInteger rcFirstReadOperation = new AtomicInteger();

        lh.asyncReadEntries(0, 0, new ReadCallback() {
            @Override
            public void readComplete(int rc, LedgerHandle lh, Enumeration<LedgerEntry> seq, Object ctx) {
                rcFirstReadOperation.set(rc);
                counter.countDown();
            }
        }, lh);

        final AtomicInteger rcSecondReadOperation = new AtomicInteger();

        lh.asyncReadEntries(0, N - 1, new ReadCallback() {
            @Override
            public void readComplete(int rc, LedgerHandle lh, Enumeration<LedgerEntry> seq, Object ctx) {
                rcSecondReadOperation.set(rc);
                counter.countDown();
            }
        }, lh);

        counter.await();

        // assertEquals(BKException.Code.OK, rcFirstReadOperation.get());
        assertEquals(BKException.Code.TooManyRequestsException, rcSecondReadOperation.get());
    }

    @Test(timeout = 60000)
    public void testAddRejected() throws Exception {
        LedgerHandle lh = bkc.createLedger(1, 1, digestType, new byte[0]);
        byte[] content = new byte[100];

        final int N = 1000;

        // Write asynchronously, and expect at least few writes to have failed with NotEnoughBookies,
        // because when we get the TooManyRequestException, the client will try to form a new ensemble and that
        // operation will fail since we only have 1 bookie available
        final CountDownLatch counter = new CountDownLatch(N);
        final AtomicBoolean receivedTooManyRequestsException = new AtomicBoolean();

        // Write few entries
        for (int i = 0; i < N; i++) {
            lh.asyncAddEntry(content, new AddCallback() {
                @Override
                public void addComplete(int rc, LedgerHandle lh, long entryId, Object ctx) {
                    if (rc == BKException.Code.NotEnoughBookiesException) {
                        receivedTooManyRequestsException.set(true);
                    }

                    counter.countDown();
                }
            }, null);
        }

        counter.await();

        assertTrue(receivedTooManyRequestsException.get());
    }

    @Test(timeout = 60000)
    public void testRecoveryNotRejected() throws Exception {
        LedgerHandle lh = bkc.createLedger(1, 1, digestType, new byte[0]);
        byte[] content = new byte[100];

        final int numEntriesToRead = 1000;
        // Write few entries
        for (int i = 0; i < numEntriesToRead; i++) {
            lh.addEntry(content);
        }

        final int numLedgersToRecover = 10;
        List<Long> ledgersToRecover = Lists.newArrayList();
        for (int i = 0; i < numLedgersToRecover; i++) {
            LedgerHandle lhr = bkc.createLedger(1, 1, digestType, new byte[0]);
            lhr.addEntry(content);
            // Leave the ledger in open state
            ledgersToRecover.add(lhr.getId());
        }

        ExecutorService executor = Executors.newCachedThreadPool();
        final CyclicBarrier barrier = new CyclicBarrier(1 + numLedgersToRecover);

        List<Future<?>> futures = Lists.newArrayList();
        futures.add(executor.submit(new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                barrier.await();
                try {
                    lh.readEntries(0, numEntriesToRead - 1);
                    fail("Should have thrown exception");
                } catch (Exception e) {
                    // Expected
                }
                return null;
            }
        }));

        for (long ledgerId : ledgersToRecover) {
            futures.add(executor.submit(new Callable<Void>() {
                @Override
                public Void call() throws Exception {
                    barrier.await();

                    // Recovery should always succeed
                    bkc.openLedger(ledgerId, digestType, new byte[0]);
                    return null;
                }
            }));
        }

        for (Future<?> future : futures) {
            future.get();
        }
    }
}
