/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.bookkeeper.client;

import static com.google.common.base.Preconditions.checkNotNull;
import io.netty.buffer.ByteBuf;
import io.netty.util.Recycler;
import io.netty.util.Recycler.Handle;
import io.netty.util.ReferenceCountUtil;

import java.util.concurrent.TimeUnit;

import org.apache.bookkeeper.client.AsyncCallback.AddCallback;
import org.apache.bookkeeper.net.BookieSocketAddress;
import org.apache.bookkeeper.proto.BookieProtocol;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks.WriteCallback;
import org.apache.bookkeeper.stats.OpStatsLogger;
import org.apache.bookkeeper.util.MathUtils;
import org.apache.bookkeeper.util.SafeRunnable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.carrotsearch.hppc.IntArrayList;
import com.carrotsearch.hppc.IntHashSet;
import com.carrotsearch.hppc.procedures.IntProcedure;

/**
 * This represents a pending add operation. When it has got success from all
 * bookies, it sees if its at the head of the pending adds queue, and if yes,
 * sends ack back to the application. If a bookie fails, a replacement is made
 * and placed at the same position in the ensemble. The pending adds are then
 * rereplicated.
 *
 *
 */
class PendingAddOp extends SafeRunnable implements WriteCallback, IntProcedure {
    private final static Logger LOG = LoggerFactory.getLogger(PendingAddOp.class);

    ByteBuf payload;
    ByteBuf toSend;
    AddCallback cb;
    Object ctx;
    long entryId;
    int entryLength;
    final IntHashSet writeSet = new IntHashSet();

    DistributionSchedule.AckSet ackSet;
    boolean completed = false;

    LedgerHandle lh;
    boolean isRecoveryAdd = false;
    long requestTimeNanos;
    OpStatsLogger addOpLogger;
    long currentLedgerLength;
    int pendingWriteRequests;
    boolean callbackTriggered;

    static PendingAddOp create(LedgerHandle lh, ByteBuf payload, AddCallback cb, Object ctx) {
        PendingAddOp op = RECYCLER.get();
        op.lh = lh;
        op.cb = cb;
        op.ctx = ctx;
        op.entryId = LedgerHandle.INVALID_ENTRY_ID;
        op.payload = payload;
        op.entryLength = payload.readableBytes();
        op.writeSet.clear();
        op.completed = false;
        op.ackSet = lh.distributionSchedule.getAckSet();
        op.addOpLogger = lh.bk.getAddOpLogger();
        op.pendingWriteRequests = 0;
        op.callbackTriggered = false;
        return op;
    }

    /**
     * Enable the recovery add flag for this operation.
     * @see LedgerHandle#asyncRecoveryAddEntry
     */
    PendingAddOp enableRecoveryAdd() {
        isRecoveryAdd = true;
        return this;
    }

    void setEntryId(long entryId) {
        this.entryId = entryId;

        IntArrayList ws = lh.distributionSchedule.getWriteSet(entryId);
        this.writeSet.clear();
        for (int i = 0; i < ws.size(); i++) {
            this.writeSet.add(ws.get(i));
        }
    }

    void sendWriteRequest(int bookieIndex) {
        int flags = isRecoveryAdd ? BookieProtocol.FLAG_RECOVERY_ADD : BookieProtocol.FLAG_NONE;

        lh.bk.bookieClient.addEntry(lh.metadata.currentEnsemble.get(bookieIndex), lh.ledgerId, lh.ledgerKey, entryId, toSend,
                this, bookieIndex, flags);
        ++pendingWriteRequests;
    }

    void unsetSuccessAndSendWriteRequest(int bookieIndex) {
        if (toSend == null) {
            // this addOp hasn't yet had its mac computed. When the mac is
            // computed, its write requests will be sent, so no need to send it
            // now
            return;
        }
        // Suppose that unset doesn't happen on the write set of an entry. In this
        // case we don't need to resend the write request upon an ensemble change.
        // We do need to invoke #sendAddSuccessCallbacks() for such entries because
        // they may have already completed, but they are just waiting for the ensemble
        // to change.
        // E.g.
        // ensemble (A, B, C, D), entry k is written to (A, B, D). An ensemble change
        // happens to replace C with E. Entry k does not complete until C is
        // replaced with E successfully. When the ensemble change completes, it tries
        // to unset entry k. C however is not in k's write set, so no entry is written
        // again, and no one triggers #sendAddSuccessCallbacks. Consequently, k never
        // completes.
        //
        // We call sendAddSuccessCallback when unsetting t cover this case.
        if (!writeSet.contains(bookieIndex)) {
            lh.sendAddSuccessCallbacks();
            return;
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("Unsetting success for ledger: " + lh.ledgerId + " entry: " + entryId + " bookie index: "
                      + bookieIndex);
        }

        // if we had already heard a success from this array index, need to
        // increment our number of responses that are pending, since we are
        // going to unset this success
        ackSet.removeBookie(bookieIndex);
        completed = false;

        sendWriteRequest(bookieIndex);
    }

    /**
     * Initiate the add operation
     */
    public void safeRun() {
        this.requestTimeNanos = MathUtils.nowInNano();
        checkNotNull(lh);
        checkNotNull(lh.macManager);

        this.toSend = lh.macManager.computeDigestAndPackageForSending(entryId, lh.lastAddConfirmed, currentLedgerLength,
                payload);

        // Iterate over set and trigger the sendWriteRequests
        writeSet.forEach(this);
    }

    /** Called when iterating over writeSet. Trick to avoid creating
     * an iterator over the set
     */
    @Override
    public void apply(int bookieIndex) {
        sendWriteRequest(bookieIndex);
    }

    @Override
    public void writeComplete(int rc, long ledgerId, long entryId, BookieSocketAddress addr, Object ctx) {
        int bookieIndex = (Integer) ctx;
        --pendingWriteRequests;

        if (completed) {
            // I am already finished, ignore incoming responses.
            // otherwise, we might hit the following error handling logic, which might cause bad things.
            if (callbackTriggered && pendingWriteRequests == 0) {
                // We can recycle this object only when no other request is expected to complete
                recycle();
            }
            return;
        }

        switch (rc) {
        case BKException.Code.OK:
            // continue
            break;
        case BKException.Code.ClientClosedException:
            // bookie client is closed.
            lh.errorOutPendingAdds(rc);
            return;
        case BKException.Code.LedgerFencedException:
            LOG.warn("Fencing exception on write: L{} E{} on {}",
                     new Object[] { ledgerId, entryId, addr });
            lh.handleUnrecoverableErrorDuringAdd(rc);
            return;
        case BKException.Code.UnauthorizedAccessException:
            LOG.warn("Unauthorized access exception on write: L{} E{} on {}",
                     new Object[] { ledgerId, entryId, addr });
            lh.handleUnrecoverableErrorDuringAdd(rc);
            return;
        default:
            LOG.warn("Write did not succeed: L{} E{} on {}, rc = {}",
                     new Object[] { ledgerId, entryId, addr, rc });
            lh.handleBookieFailure(addr, bookieIndex);
            return;
        }

        if (!writeSet.contains(bookieIndex)) {
            LOG.warn("Received a response for (lid:{}, eid:{}) from {}@{}, but it doesn't belong to {}.",
                     new Object[] { ledgerId, entryId, addr, bookieIndex, writeSet });
            return;
        }

        if (ackSet.addBookieAndCheck(bookieIndex) && !completed) {
            completed = true;

            if (LOG.isDebugEnabled()) {
                LOG.debug("Complete (lid:{}, eid:{}).", ledgerId, entryId);
            }
            // when completed an entry, try to send success add callbacks in order
            lh.sendAddSuccessCallbacks();
        }
    }

    void submitCallback(final int rc) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("Submit callback (lid:{}, eid:{}). rc: {}", new Object[] {lh.getId(), entryId, rc});
        }

        ReferenceCountUtil.release(toSend);
        long latencyNanos = MathUtils.elapsedNanos(requestTimeNanos);
        if (rc != BKException.Code.OK) {
            addOpLogger.registerFailedEvent(latencyNanos, TimeUnit.NANOSECONDS);
            LOG.error("Write of ledger entry to quorum failed: L{} E{}",
                      lh.getId(), entryId);
        } else {
            addOpLogger.registerSuccessfulEvent(latencyNanos, TimeUnit.NANOSECONDS);
        }
        cb.addComplete(rc, lh, entryId, ctx);
        callbackTriggered = true;

        if (pendingWriteRequests == 0) {
            recycle();
        }
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("PendingAddOp(lid:").append(lh.ledgerId)
          .append(", eid:").append(entryId).append(", completed:")
          .append(completed).append(")");
        return sb.toString();
    }

    private final Handle recyclerHandle;
    private static final Recycler<PendingAddOp> RECYCLER = new Recycler<PendingAddOp>() {
        protected PendingAddOp newObject(Recycler.Handle handle) {
            return new PendingAddOp(handle);
        }
    };

    private PendingAddOp(Handle recyclerHandle) {
        this.recyclerHandle = recyclerHandle;
    }

    private void recycle() {
        payload = null;
        toSend = null;
        cb = null;
        ctx = null;
        ackSet.recycle();
        ackSet = null;
        lh = null;
        isRecoveryAdd = false;
        addOpLogger = null;
        completed = false;
        pendingWriteRequests = 0;
        callbackTriggered = false;
        RECYCLER.recycle(this, recyclerHandle);
    }
}
