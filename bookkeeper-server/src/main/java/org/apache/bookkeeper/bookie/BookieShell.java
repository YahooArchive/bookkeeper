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

package org.apache.bookkeeper.bookie;

import static com.google.common.base.Charsets.UTF_8;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.FileSystem;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Enumeration;
import java.util.Formatter;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.SortedMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import org.apache.bookkeeper.bookie.BookieException.InvalidCookieException;
import org.apache.bookkeeper.bookie.EntryLogger.EntryLogScanner;
import org.apache.bookkeeper.bookie.Journal.JournalScanner;
import org.apache.bookkeeper.bookie.storage.ldb.DbLedgerStorage;
import org.apache.bookkeeper.bookie.storage.ldb.EntryLocationIndex;
import org.apache.bookkeeper.bookie.storage.ldb.KeyValueStorage;
import org.apache.bookkeeper.bookie.storage.ldb.KeyValueStorage.CloseableIterator;
import org.apache.bookkeeper.bookie.storage.ldb.KeyValueStorageLevelDB;
import org.apache.bookkeeper.bookie.storage.ldb.KeyValueStorageRocksDB;
import org.apache.bookkeeper.bookie.storage.ldb.LocationsIndexRebuildOp;
import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.client.BookKeeper.DigestType;
import org.apache.bookkeeper.client.BookKeeperAdmin;
import org.apache.bookkeeper.client.LedgerEntry;
import org.apache.bookkeeper.client.LedgerHandle;
import org.apache.bookkeeper.client.LedgerMetadata;
import org.apache.bookkeeper.client.UpdateLedgerOp;
import org.apache.bookkeeper.conf.ClientConfiguration;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.meta.LedgerManager;
import org.apache.bookkeeper.meta.LedgerManager.LedgerRange;
import org.apache.bookkeeper.meta.LedgerManager.LedgerRangeIterator;
import org.apache.bookkeeper.meta.LedgerManagerFactory;
import org.apache.bookkeeper.meta.LedgerUnderreplicationManager;
import org.apache.bookkeeper.net.BookieSocketAddress;
import org.apache.bookkeeper.proto.BookieClient;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks.GenericCallback;
import org.apache.bookkeeper.replication.AuditorElector;
import org.apache.bookkeeper.stats.NullStatsLogger;
import org.apache.bookkeeper.util.EntryFormatter;
import org.apache.bookkeeper.util.MathUtils;
import org.apache.bookkeeper.util.OrderedSafeExecutor;
import org.apache.bookkeeper.util.Tool;
import org.apache.bookkeeper.util.ZkUtils;
import org.apache.bookkeeper.versioning.Version;
import org.apache.bookkeeper.versioning.Versioned;
import org.apache.bookkeeper.zookeeper.ZooKeeperClient;
import org.apache.bookkeeper.zookeeper.ZooKeeperWatcherBase;
import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.MissingArgumentException;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.configuration.CompositeConfiguration;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.commons.lang.StringUtils;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.util.concurrent.AbstractFuture;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;

/**
 * Bookie Shell is to provide utilities for users to administer a bookkeeper cluster.
 */
public class BookieShell implements Tool {

    static final Logger LOG = LoggerFactory.getLogger(BookieShell.class);

    static final String ENTRY_FORMATTER_CLASS = "entryFormatterClass";

    static final String CMD_METAFORMAT = "metaformat";
    static final String CMD_BOOKIEFORMAT = "bookieformat";
    static final String CMD_RECOVER = "recover";
    static final String CMD_LEDGER = "ledger";
    static final String CMD_READ_LEDGER_ENTRIES = "readledger";
    static final String CMD_LISTLEDGERS = "listledgers";
    static final String CMD_LEDGERMETADATA = "ledgermetadata";
    static final String CMD_LISTUNDERREPLICATED = "listunderreplicated";
    static final String CMD_WHOISAUDITOR = "whoisauditor";
    static final String CMD_SIMPLETEST = "simpletest";
    static final String CMD_BOOKIESANITYTEST = "bookiesanity";
    static final String CMD_READLOG = "readlog";
    static final String CMD_READJOURNAL = "readjournal";
    static final String CMD_LASTMARK = "lastmark";
    static final String CMD_AUTORECOVERY = "autorecovery";
    static final String CMD_LISTBOOKIES = "listbookies";
    static final String CMD_UPDATECOOKIE = "updatecookie";
    static final String CMD_UPDATELEDGER = "updateledgers";
    static final String CMD_CONVERT_TO_DB_STORAGE = "convert-to-db-storage";
    static final String CMD_CONVERT_TO_INTERLEAVED_STORAGE = "convert-to-interleaved-storage";
    static final String CMD_CONVERT_ROCKSDB_TO_LEVELDB_STORAGE = "convert-rocksdb-to-leveldb-storage";
    static final String CMD_REBUILD_DB_LEDGER_LOCATIONS_INDEX = "rebuild-db-ledger-locations-index";
    static final String CMD_HELP = "help";

    final ServerConfiguration bkConf = new ServerConfiguration();
    File[] indexDirectories;
    File[] ledgerDirectories;
    File[] journalDirectories;

    EntryLogger entryLogger = null;
    List<Journal> journals = null;
    EntryFormatter formatter;

    int pageSize;
    int entriesPerPage;

    interface Command {
        public int runCmd(String[] args) throws Exception;
        public void printUsage();
    }

    abstract class MyCommand implements Command {
        abstract Options getOptions();
        abstract String getDescription();
        abstract String getUsage();
        abstract int runCmd(CommandLine cmdLine) throws Exception;

        String cmdName;

        MyCommand(String cmdName) {
            this.cmdName = cmdName;
        }

        @Override
        public int runCmd(String[] args) throws Exception {
            try {
                BasicParser parser = new BasicParser();
                CommandLine cmdLine = parser.parse(getOptions(), args);
                return runCmd(cmdLine);
            } catch (ParseException e) {
                LOG.error("Error parsing command line arguments : ", e);
                printUsage();
                return -1;
            }
        }

        @Override
        public void printUsage() {
            HelpFormatter hf = new HelpFormatter();
            System.err.println(cmdName + ": " + getDescription());
            hf.printHelp(getUsage(), getOptions());
        }
    }

    /**
     * Format the bookkeeper metadata present in zookeeper
     */
    class MetaFormatCmd extends MyCommand {
        Options opts = new Options();

        MetaFormatCmd() {
            super(CMD_METAFORMAT);
            opts.addOption("n", "nonInteractive", false,
                    "Whether to confirm if old data exists..?");
            opts.addOption("f", "force", false,
                    "If [nonInteractive] is specified, then whether"
                            + " to force delete the old data without prompt.");
        }

        @Override
        Options getOptions() {
            return opts;
        }

        @Override
        String getDescription() {
            return "Format bookkeeper metadata in zookeeper";
        }

        @Override
        String getUsage() {
            return "metaformat   [-nonInteractive] [-force]";
        }

        @Override
        int runCmd(CommandLine cmdLine) throws Exception {
            boolean interactive = (!cmdLine.hasOption("n"));
            boolean force = cmdLine.hasOption("f");

            ClientConfiguration adminConf = new ClientConfiguration(bkConf);
            boolean result = BookKeeperAdmin.format(adminConf, interactive,
                    force);
            return (result) ? 0 : 1;
        }
    }

    /**
     * Formats the local data present in current bookie server
     */
    class BookieFormatCmd extends MyCommand {
        Options opts = new Options();

        public BookieFormatCmd() {
            super(CMD_BOOKIEFORMAT);
            opts.addOption("n", "nonInteractive", false,
                    "Whether to confirm if old data exists..?");
            opts.addOption("f", "force", false,
                    "If [nonInteractive] is specified, then whether"
                            + " to force delete the old data without prompt..?");
            opts.addOption("d", "deleteCookie", false, "Delete its cookie on zookeeper");
        }

        @Override
        Options getOptions() {
            return opts;
        }

        @Override
        String getDescription() {
            return "Format the current server contents";
        }

        @Override
        String getUsage() {
            return "bookieformat [-nonInteractive] [-force] [-deleteCookie]";
        }

        @Override
        int runCmd(CommandLine cmdLine) throws Exception {
            boolean interactive = (!cmdLine.hasOption("n"));
            boolean force = cmdLine.hasOption("f");

            ServerConfiguration conf = new ServerConfiguration(bkConf);
            boolean result = Bookie.format(conf, interactive, force);
            // delete cookie
            if (cmdLine.hasOption("d")) {
                ZooKeeperClient zkc =
                        ZooKeeperClient.createConnectedZooKeeperClient(conf.getZkServers(), conf.getZkTimeout());
                try {
                    Versioned<Cookie> cookie = Cookie.readFromZooKeeper(zkc, conf);
                    cookie.getValue().deleteFromZooKeeper(zkc, conf, cookie.getVersion());
                } catch (KeeperException.NoNodeException nne) {
                    LOG.warn("No cookie to remove : ", nne);
                } finally {
                    zkc.close();
                }
            }
            return (result) ? 0 : 1;
        }
    }

    /**
     * Recover command for ledger data recovery for failed bookie
     */
    class RecoverCmd extends MyCommand {
        Options opts = new Options();

        public RecoverCmd() {
            super(CMD_RECOVER);
            opts.addOption("d", "deleteCookie", false, "Delete cookie node for the bookie.");
        }

        @Override
        Options getOptions() {
            return opts;
        }

        @Override
        String getDescription() {
            return "Recover the ledger data for failed bookie";
        }

        @Override
        String getUsage() {
            return "recover [-deleteCookie] <bookieSrc> [bookieDest]";
        }

        @Override
        int runCmd(CommandLine cmdLine) throws Exception {
            String[] args = cmdLine.getArgs();
            if (args.length < 1) {
                throw new MissingArgumentException(
                        "'bookieSrc' argument required");
            }

            ClientConfiguration adminConf = new ClientConfiguration(bkConf);
            BookKeeperAdmin admin = new BookKeeperAdmin(adminConf);
            try {
                return bkRecovery(adminConf, admin, args, cmdLine.hasOption("d"));
            } finally {
                admin.close();
            }
        }

        private int bkRecovery(ClientConfiguration conf, BookKeeperAdmin bkAdmin,
                               String[] args, boolean deleteCookie)
                throws InterruptedException, BKException, KeeperException, IOException {
            final String bookieSrcString[] = args[0].split(":");
            if (bookieSrcString.length != 2) {
                System.err.println("BookieSrc inputted has invalid format"
                        + "(host:port expected): " + args[0]);
                return -1;
            }
            final BookieSocketAddress bookieSrc = new BookieSocketAddress(
                    bookieSrcString[0], Integer.parseInt(bookieSrcString[1]));
            BookieSocketAddress bookieDest = null;
            if (args.length >= 2) {
                final String bookieDestString[] = args[1].split(":");
                if (bookieDestString.length < 2) {
                    System.err.println("BookieDest inputted has invalid format"
                            + "(host:port expected): " + args[1]);
                    return -1;
                }
                bookieDest = new BookieSocketAddress(bookieDestString[0],
                        Integer.parseInt(bookieDestString[1]));
            }

            bkAdmin.recoverBookieData(bookieSrc, bookieDest);
            if (deleteCookie) {
                try {
                    Versioned<Cookie> cookie = Cookie.readFromZooKeeper(bkAdmin.getZooKeeper(), conf, bookieSrc);
                    cookie.getValue().deleteFromZooKeeper(bkAdmin.getZooKeeper(), conf, bookieSrc, cookie.getVersion());
                } catch (KeeperException.NoNodeException nne) {
                    LOG.warn("No cookie to remove for {} : ", bookieSrc, nne);
                }
            }
            return 0;
        }
    }

    /**
     * Ledger Command Handles ledger related operations
     */
    class LedgerCmd extends MyCommand {
        Options lOpts = new Options();

        LedgerCmd() {
            super(CMD_LEDGER);
            lOpts.addOption("m", "meta", false, "Print meta information");
        }

        @Override
        public int runCmd(CommandLine cmdLine) throws Exception {
            String[] leftArgs = cmdLine.getArgs();
            if (leftArgs.length <= 0) {
                System.err.println("ERROR: missing ledger id");
                printUsage();
                return -1;
            }

            boolean printMeta = false;
            if (cmdLine.hasOption("m")) {
                printMeta = true;
            }
            long ledgerId;
            try {
                ledgerId = Long.parseLong(leftArgs[0]);
            } catch (NumberFormatException nfe) {
                System.err.println("ERROR: invalid ledger id " + leftArgs[0]);
                printUsage();
                return -1;
            }
            if (printMeta) {
                // print meta
                readLedgerMeta(ledgerId);
            }
            // dump ledger info
            readLedgerIndexEntries(ledgerId);
            return 0;
        }

        @Override
        String getDescription() {
            return "Dump ledger index entries into readable format.";
        }

        @Override
        String getUsage() {
            return "ledger       [-m] <ledger_id>";
        }

        @Override
        Options getOptions() {
            return lOpts;
        }
    }

    /**
     * Command for reading ledger entries from local bookkeeper
     */
    class ReadLedgerEntriesCmd extends MyCommand {
        Options lOpts = new Options();

        ReadLedgerEntriesCmd() {
            super(CMD_READ_LEDGER_ENTRIES);
            lOpts.addOption("r", "force-recovery", false, "Ensure the ledger is properly closed before reading");
            lOpts.addOption("b", "bookie", true, "Only read from a specific bookie");
        }

        @Override
        Options getOptions() {
            return lOpts;
        }

        @Override
        String getDescription() {
            return "Read a range of entries from a ledger";
        }

        @Override
        String getUsage() {
            return "readledger [-force-recovery] [-bookie <address:port>] <ledger_id> [<start_entry_id> [<end_entry_id>]]";
        }

        @Override
        int runCmd(CommandLine cmdLine) throws Exception {
            String[] leftArgs = cmdLine.getArgs();
            if (leftArgs.length <= 0) {
                System.err.println("ERROR: missing ledger id");
                printUsage();
                return -1;
            }

            boolean forceRecovery = cmdLine.hasOption("r");
            BookieSocketAddress bookie = null;
            if (cmdLine.hasOption("b")) {
                // A particular bookie was specified
                bookie = new BookieSocketAddress(cmdLine.getOptionValue("b"));
            }

            ClientConfiguration conf = new ClientConfiguration();
            conf.addConfiguration(bkConf);

            BookKeeperAdmin bk = null;
            try {
                bk = new BookKeeperAdmin(conf);

                long ledgerId = Long.parseLong(leftArgs[0]);

                long firstEntry = 0;
                long lastEntry = -1;

                if (leftArgs.length >= 2) {
                    firstEntry = Long.parseLong(leftArgs[1]);
                }

                if (leftArgs.length >= 3) {
                    lastEntry = Long.parseLong(leftArgs[2]);
                }

                if (forceRecovery) {
                    // Force the opening of the ledger to trigger recovery
                    LedgerHandle lh = bk.openLedger(ledgerId);
                    if (lastEntry == -1 || lastEntry > lh.getLastAddConfirmed()) {
                        lastEntry = lh.getLastAddConfirmed();
                    }
                }

                if (bookie == null) {
                    // No bookie was specified, use normal bk client
                    Iterator<LedgerEntry> entries = bk.readEntries(ledgerId, firstEntry, lastEntry).iterator();
                    while (entries.hasNext()) {
                        LedgerEntry entry = entries.next();
                        ByteBuf data = entry.getEntryBuffer();
                        System.out.println(
                                "Entry Id: " + entry.getEntryId() + ", Data: " + ByteBufUtil.prettyHexDump(data));
                        data.release();
                    }
                } else {
                    // Use BookieClient to target a specific bookie
                    EventLoopGroup eventLoopGroup = new NioEventLoopGroup();
                    OrderedSafeExecutor executor = new OrderedSafeExecutor(2, "BookieClientScheduler");
                    BookieClient bookieClient = new BookieClient(conf, eventLoopGroup, executor);

                    for (long entryId = firstEntry; entryId < lastEntry; entryId++) {
                        CompletableFuture<Void> future = new CompletableFuture<>();
                        final long currentEntryId = entryId;
                        bookieClient.readEntry(bookie, ledgerId, entryId, (rc, ledgerId1, entryId1, buffer, ctx) -> {
                            if (rc != BKException.Code.OK) {
                                LOG.error("Failed to read entry {} -- {}", entryId1, BKException.getMessage(rc));
                                future.completeExceptionally(BKException.create(rc));
                                return;
                            }

                            System.out.println(
                                    "Entry Id: " + currentEntryId + ", Data: " + ByteBufUtil.prettyHexDump(buffer));
                            buffer.release();
                            future.complete(null);
                        }, null, 0 );

                        future.get();
                    }

                    eventLoopGroup.shutdownGracefully();
                    executor.shutdown();
                    bookieClient.close();
                }
            } catch (NumberFormatException nfe) {
                System.err.println("ERROR: invalid number " + nfe.getMessage());
                printUsage();
                return -1;
            } catch (Exception e) {
                LOG.error("Error reading entries from ledger", e);
                return -1;
            } finally {
                if (bk != null) {
                    bk.close();
                }
            }

            return 0;
        }

    }

    /**
     * Command for listing underreplicated ledgers
     */
    class ListUnderreplicatedCmd extends MyCommand {
        Options opts = new Options();

        public ListUnderreplicatedCmd() {
            super(CMD_LISTUNDERREPLICATED);
        }

        @Override
        Options getOptions() {
            return opts;
        }

        @Override
        String getDescription() {
            return "List ledgers marked as underreplicated";
        }

        @Override
        String getUsage() {
            return "listunderreplicated";
        }

        @Override
        int runCmd(CommandLine cmdLine) throws Exception {
            ZooKeeper zk = null;
            try {
                ZooKeeperWatcherBase w = new ZooKeeperWatcherBase(bkConf.getZkTimeout());
                zk = ZkUtils.createConnectedZookeeperClient(bkConf.getZkServers(), w);
                LedgerManagerFactory mFactory = LedgerManagerFactory.newLedgerManagerFactory(bkConf, zk);
                LedgerUnderreplicationManager underreplicationManager = mFactory.newLedgerUnderreplicationManager();
                Iterator<Long> iter = underreplicationManager.listLedgersToRereplicate();
                while (iter.hasNext()) {
                    System.out.println(iter.next());
                }
            } finally {
                if (zk != null) {
                    zk.close();
                }
            }

            return 0;
        }
    }

    final static int LIST_BATCH_SIZE = 1000;
    /**
     * Command to list all ledgers in the cluster
     */
    class ListLedgersCmd extends MyCommand {
        Options lOpts = new Options();

        ListLedgersCmd() {
            super(CMD_LISTLEDGERS);
            lOpts.addOption("m", "meta", false, "Print metadata");

        }

        @Override
        public int runCmd(CommandLine cmdLine) throws Exception {
            ZooKeeper zk = null;
            try {
                ZooKeeperWatcherBase w = new ZooKeeperWatcherBase(bkConf.getZkTimeout());
                zk = ZkUtils.createConnectedZookeeperClient(bkConf.getZkServers(), w);
                LedgerManagerFactory mFactory = LedgerManagerFactory.newLedgerManagerFactory(bkConf, zk);
                LedgerManager m = mFactory.newLedgerManager();
                LedgerRangeIterator iter = m.getLedgerRanges();
                if (cmdLine.hasOption("m")) {
                    List<ReadMetadataCallback> futures
                        = new ArrayList<ReadMetadataCallback>(LIST_BATCH_SIZE);
                    while (iter.hasNext()) {
                        LedgerRange r = iter.next();
                        for (Long lid : r.getLedgers()) {
                            ReadMetadataCallback cb = new ReadMetadataCallback(lid);
                            m.readLedgerMetadata(lid, cb);
                            futures.add(cb);
                        }
                        if (futures.size() >= LIST_BATCH_SIZE) {
                            while (futures.size() > 0) {
                                ReadMetadataCallback cb = futures.remove(0);
                                printLedgerMetadata(cb);
                            }
                        }
                    }
                    while (futures.size() > 0) {
                        ReadMetadataCallback cb = futures.remove(0);
                        printLedgerMetadata(cb);
                    }
                } else {
                    while (iter.hasNext()) {
                        LedgerRange r = iter.next();
                        for (Long lid : r.getLedgers()) {
                            System.out.println(Long.toString(lid));
                        }
                    }
                }
            } finally {
                if (zk != null) {
                    zk.close();
                }
            }

            return 0;
        }

        @Override
        String getDescription() {
            return "List all ledgers on the cluster (this may take a long time)";
        }

        @Override
        String getUsage() {
            return "listledgers  [-meta]";
        }

        @Override
        Options getOptions() {
            return lOpts;
        }
    }

    static void printLedgerMetadata(ReadMetadataCallback cb) throws Exception {
        LedgerMetadata md = cb.get();
        System.out.println("ledgerID: " + cb.getLedgerId());
        System.out.println(new String(md.serialize(), UTF_8));
    }

    static class ReadMetadataCallback extends AbstractFuture<LedgerMetadata>
        implements GenericCallback<LedgerMetadata> {
        final long ledgerId;

        ReadMetadataCallback(long ledgerId) {
            this.ledgerId = ledgerId;
        }

        long getLedgerId() {
            return ledgerId;
        }

        public void operationComplete(int rc, LedgerMetadata result) {
            if (rc != 0) {
                setException(BKException.create(rc));
            } else {
                set(result);
            }
        }
    }

    /**
     * Print the metadata for a ledger
     */
    class LedgerMetadataCmd extends MyCommand {
        Options lOpts = new Options();

        LedgerMetadataCmd() {
            super(CMD_LEDGERMETADATA);
            lOpts.addOption("l", "ledgerid", true, "Ledger ID");
        }

        @Override
        public int runCmd(CommandLine cmdLine) throws Exception {
            final long lid = getOptionLongValue(cmdLine, "ledgerid", -1);
            if (lid == -1) {
                System.err.println("Must specify a ledger id");
                return -1;
            }

            ZooKeeper zk = null;
            try {
                ZooKeeperWatcherBase w = new ZooKeeperWatcherBase(bkConf.getZkTimeout());
                zk = ZkUtils.createConnectedZookeeperClient(bkConf.getZkServers(), w);
                LedgerManagerFactory mFactory = LedgerManagerFactory.newLedgerManagerFactory(bkConf, zk);
                LedgerManager m = mFactory.newLedgerManager();
                ReadMetadataCallback cb = new ReadMetadataCallback(lid);
                m.readLedgerMetadata(lid, cb);
                printLedgerMetadata(cb);
            } finally {
                if (zk != null) {
                    zk.close();
                }
            }

            return 0;
        }

        @Override
        String getDescription() {
            return "Print the metadata for a ledger";
        }

        @Override
        String getUsage() {
            return "ledgermetadata -ledgerid <ledgerid>";
        }

        @Override
        Options getOptions() {
            return lOpts;
        }
    }

    /**
     * Simple test to create a ledger and write to it
     */
    class SimpleTestCmd extends MyCommand {
        Options lOpts = new Options();

        SimpleTestCmd() {
            super(CMD_SIMPLETEST);
            lOpts.addOption("e", "ensemble", true, "Ensemble size (default 3)");
            lOpts.addOption("w", "writeQuorum", true, "Write quorum size (default 2)");
            lOpts.addOption("a", "ackQuorum", true, "Ack quorum size (default 2)");
            lOpts.addOption("n", "numEntries", true, "Entries to write (default 1000)");
        }

        @Override
        public int runCmd(CommandLine cmdLine) throws Exception {
            byte[] data = new byte[100]; // test data

            int ensemble = getOptionIntValue(cmdLine, "ensemble", 3);
            int writeQuorum = getOptionIntValue(cmdLine, "writeQuorum", 2);
            int ackQuorum = getOptionIntValue(cmdLine, "ackQuorum", 2);
            int numEntries = getOptionIntValue(cmdLine, "numEntries", 1000);

            ClientConfiguration conf = new ClientConfiguration();
            conf.addConfiguration(bkConf);
            BookKeeper bk = new BookKeeper(conf);
            LedgerHandle lh = bk.createLedger(ensemble, writeQuorum, ackQuorum,
                                              BookKeeper.DigestType.MAC, new byte[0]);
            System.out.println("Ledger ID: " + lh.getId());
            long lastReport = System.nanoTime();
            for (int i = 0; i < numEntries; i++) {
                lh.addEntry(data);
                if (TimeUnit.SECONDS.convert(System.nanoTime() - lastReport,
                                             TimeUnit.NANOSECONDS) > 1) {
                    System.out.println(i + " entries written");
                    lastReport = System.nanoTime();
                }
            }

            lh.close();
            bk.close();
            System.out.println(numEntries + " entries written to ledger " + lh.getId());

            return 0;
        }

        @Override
        String getDescription() {
            return "Simple test to create a ledger and write entries to it";
        }

        @Override
        String getUsage() {
            return "simpletest   [-ensemble N] [-writeQuorum N] [-ackQuorum N] [-numEntries N]";
        }

        @Override
        Options getOptions() {
            return lOpts;
        }
    }

    /**
     * Command to run a bookie sanity test
     */
    class BookieSanityTestCmd extends MyCommand {
        Options lOpts = new Options();

        BookieSanityTestCmd() {
            super(CMD_BOOKIESANITYTEST);
            lOpts.addOption("e", "entries", true, "Total entries to be added for the test (default 10)");
            lOpts.addOption("t", "timeout", true, "Timeout for write/read operations in seconds (default 1)");
        }

        @Override
        Options getOptions() {
            return lOpts;
        }

        @Override
        String getDescription() {
            return "Sanity test for local bookie. Create ledger and write/reads entries on local bookie.";
        }

        @Override
        String getUsage() {
            return "bookiesanity [-entries N] [-timeout N]";
        }

        @Override
        int runCmd(CommandLine cmdLine) throws Exception {
            int numberOfEntries = getOptionIntValue(cmdLine, "entries", 10);
            int timeoutSecs= getOptionIntValue(cmdLine, "timeout", 1);

            ClientConfiguration conf = new ClientConfiguration();
            conf.addConfiguration(bkConf);
            conf.setEnsemblePlacementPolicy(LocalBookieEnsemblePlacementPolicy.class);
            conf.setAddEntryTimeout(timeoutSecs);
            conf.setReadEntryTimeout(timeoutSecs);

            BookKeeper bk = new BookKeeper(conf);
            LedgerHandle lh = null;
            try {
                lh = bk.createLedger(1, 1, DigestType.MAC, new byte[0]);
                LOG.info("Created ledger {}", lh.getId());

                for (int i = 0; i < numberOfEntries; i++) {
                    String content = "entry-" + i;
                    lh.addEntry(content.getBytes());
                }

                LOG.info("Written {} entries in ledger {}", numberOfEntries, lh.getId());

                // Reopen the ledger and read entries
                lh = bk.openLedger(lh.getId(), DigestType.MAC, new byte[0]);
                if (lh.getLastAddConfirmed() != (numberOfEntries - 1)) {
                    throw new Exception("Invalid last entry found on ledger. expecting: " + (numberOfEntries - 1)
                            + " -- found: " + lh.getLastAddConfirmed());
                }

                Enumeration<LedgerEntry> entries = lh.readEntries(0, numberOfEntries - 1);
                int i = 0;
                while (entries.hasMoreElements()) {
                    LedgerEntry entry = entries.nextElement();
                    String actualMsg = new String(entry.getEntry());
                    String expectedMsg = "entry-" + (i++);
                    if (!expectedMsg.equals(actualMsg)) {
                        throw new Exception("Failed validation of received message - Expected: " + expectedMsg
                                + ", Actual: " + actualMsg);
                    }
                }

                LOG.info("Read {} entries from ledger {}", entries, lh.getId());
            } catch (Exception e) {
                LOG.warn("Error in bookie sanity test", e);
                return -1;
            } finally {
                if (lh != null) {
                    bk.deleteLedger(lh.getId());
                    LOG.info("Deleted ledger {}", lh.getId());
                }

                bk.close();
            }

            LOG.info("Bookie sanity test succeeded");
            return 0;
        }
    }

    /**
     * Command to read entry log files.
     */
    class ReadLogCmd extends MyCommand {
        Options rlOpts = new Options();

        ReadLogCmd() {
            super(CMD_READLOG);
            rlOpts.addOption("m", "msg", false, "Print message body");
        }

        @Override
        public int runCmd(CommandLine cmdLine) throws Exception {
            String[] leftArgs = cmdLine.getArgs();
            if (leftArgs.length <= 0) {
                System.err.println("ERROR: missing entry log id or entry log file name");
                printUsage();
                return -1;
            }

            boolean printMsg = false;
            if (cmdLine.hasOption("m")) {
                printMsg = true;
            }
            long logId;
            try {
                logId = Long.parseLong(leftArgs[0]);
            } catch (NumberFormatException nfe) {
                // not a entry log id
                File f = new File(leftArgs[0]);
                String name = f.getName();
                if (!name.endsWith(".log")) {
                    // not a log file
                    System.err.println("ERROR: invalid entry log file name " + leftArgs[0]);
                    printUsage();
                    return -1;
                }
                String idString = name.split("\\.")[0];
                logId = Long.parseLong(idString, 16);
            }
            // scan entry log
            scanEntryLog(logId, printMsg);
            return 0;
        }

        @Override
        String getDescription() {
            return "Scan an entry file and format the entries into readable format.";
        }

        @Override
        String getUsage() {
            return "readlog      [-msg] <entry_log_id | entry_log_file_name>";
        }

        @Override
        Options getOptions() {
            return rlOpts;
        }
    }

    /**
     * Command to read journal files
     */
    class ReadJournalCmd extends MyCommand {
        Options rjOpts = new Options();

        ReadJournalCmd() {
            super(CMD_READJOURNAL);
            rjOpts.addOption("dir", false, "Journal directory (needed if more than one journal configured)");
            rjOpts.addOption("m", "msg", false, "Print message body");
        }

        @Override
        public int runCmd(CommandLine cmdLine) throws Exception {
            String[] leftArgs = cmdLine.getArgs();
            if (leftArgs.length <= 0) {
                System.err.println("ERROR: missing journal id or journal file name");
                printUsage();
                return -1;
            }

            boolean printMsg = false;
            if (cmdLine.hasOption("m")) {
                printMsg = true;
            }

            Journal journal = null;
            if (getJournals().size() > 1) {
                if (!cmdLine.hasOption("dir")) {
                    System.err.println("ERROR: missing journal directory");
                    printUsage();
                    return -1;
                }

                File journalDir = new File(cmdLine.getOptionValue("dir"));
                for (Journal j : getJournals()) {
                    if (j.getJournalDirectory().equals(journalDir)) {
                        journal = j;
                        break;
                    }
                }

                if (journal == null) {
                    System.err.println("ERROR: journal directory not found");
                    printUsage();
                    return -1;
                }
            } else {
                journal = getJournals().get(0);
            }

            long journalId;
            try {
                journalId = Long.parseLong(leftArgs[0]);
            } catch (NumberFormatException nfe) {
                // not a journal id
                File f = new File(leftArgs[0]);
                String name = f.getName();
                if (!name.endsWith(".txn")) {
                    // not a journal file
                    System.err.println("ERROR: invalid journal file name " + leftArgs[0]);
                    printUsage();
                    return -1;
                }
                String idString = name.split("\\.")[0];
                journalId = Long.parseLong(idString, 16);
            }
            // scan journal
            scanJournal(journal, journalId, printMsg);
            return 0;
        }

        @Override
        String getDescription() {
            return "Scan a journal file and format the entries into readable format.";
        }

        @Override
        String getUsage() {
            return "readjournal  [-msg] <journal_id | journal_file_name>";
        }

        @Override
        Options getOptions() {
            return rjOpts;
        }
    }

    /**
     * Command to print last log mark
     */
    class LastMarkCmd extends MyCommand {
        LastMarkCmd() {
            super(CMD_LASTMARK);
        }

        @Override
        public int runCmd(CommandLine c) throws Exception {
            printLastLogMark();
            return 0;
        }

        @Override
        String getDescription() {
            return "Print last log marker.";
        }

        @Override
        String getUsage() {
            return "lastmark";
        }

        @Override
        Options getOptions() {
            return new Options();
        }
    }

    /**
     * List available bookies
     */
    class ListBookiesCmd extends MyCommand {
        Options opts = new Options();

        ListBookiesCmd() {
            super(CMD_LISTBOOKIES);
            opts.addOption("rw", "readwrite", false, "Print readwrite bookies");
            opts.addOption("ro", "readonly", false, "Print readonly bookies");
            opts.addOption("h", "hostnames", false,
                    "Also print hostname of the bookie");
        }

        @Override
        public int runCmd(CommandLine cmdLine) throws Exception {
            boolean readwrite = cmdLine.hasOption("rw");
            boolean readonly = cmdLine.hasOption("ro");

            if ((!readwrite && !readonly) || (readwrite && readonly)) {
                LOG.error("One and only one of -readwrite and -readonly must be specified");
                printUsage();
                return 1;
            }
            ClientConfiguration clientconf = new ClientConfiguration(bkConf)
                .setZkServers(bkConf.getZkServers());
            BookKeeperAdmin bka = new BookKeeperAdmin(clientconf);

            int count = 0;
            Collection<BookieSocketAddress> bookies = new ArrayList<BookieSocketAddress>();
            if (cmdLine.hasOption("rw")) {
                Collection<BookieSocketAddress> availableBookies = bka
                        .getAvailableBookies();
                bookies.addAll(availableBookies);
            } else if (cmdLine.hasOption("ro")) {
                Collection<BookieSocketAddress> roBookies = bka
                        .getReadOnlyBookies();
                bookies.addAll(roBookies);
            }
            for (BookieSocketAddress b : bookies) {
                System.out.print(b);
                if (cmdLine.hasOption("h")) {
                    System.out.print("\t" + b.getSocketAddress().getHostName());
                }
                System.out.println("");
                count++;
            }
            if (count == 0) {
                System.err.println("No bookie exists!");
                return 1;
            }
            return 0;
        }

        @Override
        String getDescription() {
            return "List the bookies, which are running as either readwrite or readonly mode.";
        }

        @Override
        String getUsage() {
            return "listbookies  [-readwrite|-readonly] [-hostnames]";
        }

        @Override
        Options getOptions() {
            return opts;
        }
    }

    /**
     * Command to print help message
     */
    class HelpCmd extends MyCommand {
        HelpCmd() {
            super(CMD_HELP);
        }

        @Override
        public int runCmd(CommandLine cmdLine) throws Exception {
            String[] args = cmdLine.getArgs();
            if (args.length == 0) {
                printShellUsage();
                return 0;
            }
            String cmdName = args[0];
            Command cmd = commands.get(cmdName);
            if (null == cmd) {
                System.err.println("Unknown command " + cmdName);
                printShellUsage();
                return -1;
            }
            cmd.printUsage();
            return 0;
        }

        @Override
        String getDescription() {
            return "Describe the usage of this program or its subcommands.";
        }

        @Override
        String getUsage() {
            return "help         [COMMAND]";
        }

        @Override
        Options getOptions() {
            return new Options();
        }
    }

    /**
     * Command for administration of autorecovery
     */
    class AutoRecoveryCmd extends MyCommand {
        Options opts = new Options();

        public AutoRecoveryCmd() {
            super(CMD_AUTORECOVERY);
            opts.addOption("e", "enable", false,
                           "Enable auto recovery of underreplicated ledgers");
            opts.addOption("d", "disable", false,
                           "Disable auto recovery of underreplicated ledgers");
        }

        @Override
        Options getOptions() {
            return opts;
        }

        @Override
        String getDescription() {
            return "Enable or disable autorecovery in the cluster.";
        }

        @Override
        String getUsage() {
            return "autorecovery [-enable|-disable]";
        }

        @Override
        int runCmd(CommandLine cmdLine) throws Exception {
            boolean disable = cmdLine.hasOption("d");
            boolean enable = cmdLine.hasOption("e");

            if (enable && disable) {
                LOG.error("Only one of -enable and -disable can be specified");
                printUsage();
                return 1;
            }
            ZooKeeper zk = null;
            try {
                ZooKeeperWatcherBase w = new ZooKeeperWatcherBase(bkConf.getZkTimeout());
                zk = ZkUtils.createConnectedZookeeperClient(bkConf.getZkServers(), w);
                LedgerManagerFactory mFactory = LedgerManagerFactory.newLedgerManagerFactory(bkConf, zk);
                LedgerUnderreplicationManager underreplicationManager = mFactory.newLedgerUnderreplicationManager();
                if (!enable && !disable) {
                    boolean enabled = underreplicationManager.isLedgerReplicationEnabled();
                    System.out.println("Autorecovery is " + (enabled ? "enabled." : "disabled."));
                } else if (enable) {
                    if (underreplicationManager.isLedgerReplicationEnabled()) {
                        LOG.warn("Autorecovery already enabled. Doing nothing");
                    } else {
                        LOG.info("Enabling autorecovery");
                        underreplicationManager.enableLedgerReplication();
                    }
                } else {
                    if (!underreplicationManager.isLedgerReplicationEnabled()) {
                        LOG.warn("Autorecovery already disabled. Doing nothing");
                    } else {
                        LOG.info("Disabling autorecovery");
                        underreplicationManager.disableLedgerReplication();
                    }
                }
            } finally {
                if (zk != null) {
                    zk.close();
                }
            }

            return 0;
        }
    }

    /**
     * Print which node has the auditor lock
     */
    class WhoIsAuditorCmd extends MyCommand {
        Options opts = new Options();

        public WhoIsAuditorCmd() {
            super(CMD_WHOISAUDITOR);
        }

        @Override
        Options getOptions() {
            return opts;
        }

        @Override
        String getDescription() {
            return "Print the node which holds the auditor lock";
        }

        @Override
        String getUsage() {
            return "whoisauditor";
        }

        @Override
        int runCmd(CommandLine cmdLine) throws Exception {
            ZooKeeper zk = null;
            try {
                ZooKeeperWatcherBase w = new ZooKeeperWatcherBase(bkConf.getZkTimeout());
                zk = ZkUtils.createConnectedZookeeperClient(bkConf.getZkServers(), w);
                BookieSocketAddress bookieId = AuditorElector.getCurrentAuditor(bkConf, zk);
                if (bookieId == null) {
                    LOG.info("No auditor elected");
                    return -1;
                }
                LOG.info("Auditor: {}/{}:{}",
                         new Object[] {
                             bookieId.getSocketAddress().getAddress().getCanonicalHostName(),
                             bookieId.getSocketAddress().getAddress().getHostAddress(),
                             bookieId.getSocketAddress().getPort() });
            } finally {
                if (zk != null) {
                    zk.close();
                }
            }

            return 0;
        }
    }

    /**
     * Update cookie command
     */
    class UpdateCookieCmd extends MyCommand {
        Options opts = new Options();

        UpdateCookieCmd() {
            super(CMD_UPDATECOOKIE);
            opts.addOption("b", "bookieId", true, "Bookie Id");
        }

        @Override
        Options getOptions() {
            return opts;
        }

        @Override
        String getDescription() {
            return "Update bookie id in cookie";
        }

        @Override
        String getUsage() {
            return "updatecookie -bookieId <hostname|ip>";
        }

        @Override
        int runCmd(CommandLine cmdLine) throws Exception {
            final String bookieId = cmdLine.getOptionValue("bookieId");
            if (StringUtils.isBlank(bookieId)) {
                LOG.error("Invalid argument list!");
                this.printUsage();
                return -1;
            }
            if (!StringUtils.equals(bookieId, "hostname") && !StringUtils.equals(bookieId, "ip")) {
                LOG.error("Invalid option value:" + bookieId);
                this.printUsage();
                return -1;
            }
            boolean useHostName = getOptionalValue(bookieId, "hostname");
            if (!bkConf.getUseHostNameAsBookieID() && useHostName) {
                LOG.error("Expects configuration useHostNameAsBookieID=true as the option value passed is 'hostname'");
                return -1;
            } else if (bkConf.getUseHostNameAsBookieID() && !useHostName) {
                LOG.error("Expects configuration useHostNameAsBookieID=false as the option value passed is 'ip'");
                return -1;
            }
            return updateBookieIdInCookie(bookieId, useHostName);
        }

        private int updateBookieIdInCookie(final String bookieId, final boolean useHostname) throws IOException,
                InterruptedException {
            ZooKeeper zk = null;
            ZooKeeperWatcherBase w = new ZooKeeperWatcherBase(bkConf.getZkTimeout());
            try {
                zk = ZkUtils.createConnectedZookeeperClient(bkConf.getZkServers(), w);
                ServerConfiguration conf = new ServerConfiguration(bkConf);
                String newBookieId = Bookie.getBookieAddress(conf).toString();
                // read oldcookie
                Versioned<Cookie> oldCookie = null;
                try {
                    conf.setUseHostNameAsBookieID(!useHostname);
                    oldCookie = Cookie.readFromZooKeeper(zk, conf);
                } catch (KeeperException.NoNodeException nne) {
                    LOG.error("Either cookie already updated with UseHostNameAsBookieID={} or no cookie exists!",
                            useHostname, nne);
                    return -1;
                }
                Cookie newCookie = Cookie.newBuilder(oldCookie.getValue()).setBookieHost(newBookieId).build();
                boolean hasCookieUpdatedInDirs = verifyCookie(newCookie, journalDirectories[0]);
                for (File dir : ledgerDirectories) {
                    hasCookieUpdatedInDirs &= verifyCookie(newCookie, dir);
                }
                if (indexDirectories != ledgerDirectories) {
                    for (File dir : indexDirectories) {
                        hasCookieUpdatedInDirs &= verifyCookie(newCookie, dir);
                    }
                }

                if (hasCookieUpdatedInDirs) {
                    try {
                        conf.setUseHostNameAsBookieID(useHostname);
                        Cookie.readFromZooKeeper(zk, conf);
                        // since newcookie exists, just do cleanup of oldcookie and return
                        conf.setUseHostNameAsBookieID(!useHostname);
                        oldCookie.getValue().deleteFromZooKeeper(zk, conf, oldCookie.getVersion());
                        return 0;
                    } catch (KeeperException.NoNodeException nne) {
                        LOG.debug("Ignoring, cookie will be written to zookeeper");
                    }
                } else {
                    // writes newcookie to local dirs
                    for (File journalDirectory : journalDirectories) {
                        newCookie.writeToDirectory(journalDirectory);
                        LOG.info("Updated cookie file present in journalDirectory {}", journalDirectory);
                    }

                    for (File dir : ledgerDirectories) {
                        newCookie.writeToDirectory(dir);
                    }
                    LOG.info("Updated cookie file present in ledgerDirectories {}", ledgerDirectories);
                    if (ledgerDirectories != indexDirectories) {
                        for (File dir : indexDirectories) {
                            newCookie.writeToDirectory(dir);
                        }
                        LOG.info("Updated cookie file present in indexDirectories {}", indexDirectories);
                    }
                }
                // writes newcookie to zookeeper
                conf.setUseHostNameAsBookieID(useHostname);
                newCookie.writeToZooKeeper(zk, conf, Version.NEW);

                // delete oldcookie
                conf.setUseHostNameAsBookieID(!useHostname);
                oldCookie.getValue().deleteFromZooKeeper(zk, conf, oldCookie.getVersion());
            } catch (KeeperException ke) {
                LOG.error("KeeperException during cookie updation!", ke);
                return -1;
            } catch (IOException ioe) {
                LOG.error("IOException during cookie updation!", ioe);
                return -1;
            } finally {
                if (zk != null) {
                    zk.close();
                }
            }
            return 0;
        }

        private boolean verifyCookie(Cookie oldCookie, File dir) throws IOException {
            try {
                Cookie cookie = Cookie.readFromDirectory(dir);
                cookie.verify(oldCookie);
            } catch (InvalidCookieException e) {
                return false;
            }
            return true;
        }
    }

    /**
     * Update ledger command
     */
    class UpdateLedgerCmd extends MyCommand {
        private final Options opts = new Options();

        UpdateLedgerCmd() {
            super(CMD_UPDATELEDGER);
            opts.addOption("b", "bookieId", true, "Bookie Id");
            opts.addOption("s", "updatespersec", true, "Number of ledgers updating per second (default: 5 per sec)");
            opts.addOption("l", "limit", true, "Maximum number of ledgers to update (default: no limit)");
            opts.addOption("v", "verbose", true, "Print status of the ledger updation (default: false)");
            opts.addOption("p", "printprogress", true,
                    "Print messages on every configured seconds if verbose turned on (default: 10 secs)");
        }

        @Override
        Options getOptions() {
            return opts;
        }

        @Override
        String getDescription() {
            return "Update bookie id in ledgers (this may take a long time)";
        }

        @Override
        String getUsage() {
            return "updateledger -bookieId <hostname|ip> [-updatespersec N] [-limit N] [-verbose true/false] [-printprogress N]";
        }

        @Override
        int runCmd(CommandLine cmdLine) throws Exception {
            final String bookieId = cmdLine.getOptionValue("bookieId");
            if (StringUtils.isBlank(bookieId)) {
                LOG.error("Invalid argument list!");
                this.printUsage();
                return -1;
            }
            if (!StringUtils.equals(bookieId, "hostname") && !StringUtils.equals(bookieId, "ip")) {
                LOG.error("Invalid option value {} for bookieId, expected hostname/ip", bookieId);
                this.printUsage();
                return -1;
            }
            boolean useHostName = getOptionalValue(bookieId, "hostname");
            if (!bkConf.getUseHostNameAsBookieID() && useHostName) {
                LOG.error("Expects configuration useHostNameAsBookieID=true as the option value passed is 'hostname'");
                return -1;
            } else if (bkConf.getUseHostNameAsBookieID() && !useHostName) {
                LOG.error("Expects configuration useHostNameAsBookieID=false as the option value passed is 'ip'");
                return -1;
            }
            final int rate = getOptionIntValue(cmdLine, "updatespersec", 5);
            if (rate <= 0) {
                LOG.error("Invalid updatespersec {}, should be > 0", rate);
                return -1;
            }
            final int limit = getOptionIntValue(cmdLine, "limit", Integer.MIN_VALUE);
            if (limit <= 0 && limit != Integer.MIN_VALUE) {
                LOG.error("Invalid limit {}, should be > 0", limit);
                return -1;
            }
            final boolean verbose = getOptionBooleanValue(cmdLine, "verbose", false);
            final long printprogress;
            if (!verbose) {
                if (cmdLine.hasOption("printprogress")) {
                    LOG.warn("Ignoring option 'printprogress', this is applicable when 'verbose' is true");
                }
                printprogress = Integer.MIN_VALUE;
            } else {
                // defaulting to 10 seconds
                printprogress = getOptionLongValue(cmdLine, "printprogress", 10);
            }
            final ClientConfiguration conf = new ClientConfiguration();
            conf.addConfiguration(bkConf);
            final BookKeeper bk = new BookKeeper(conf);
            final BookKeeperAdmin admin = new BookKeeperAdmin(conf);
            final UpdateLedgerOp updateLedgerOp = new UpdateLedgerOp(bk, admin);
            final ServerConfiguration serverConf = new ServerConfiguration(bkConf);
            final BookieSocketAddress newBookieId = Bookie.getBookieAddress(serverConf);
            serverConf.setUseHostNameAsBookieID(!useHostName);
            final BookieSocketAddress oldBookieId = Bookie.getBookieAddress(serverConf);

            UpdateLedgerNotifier progressable = new UpdateLedgerNotifier() {
                long lastReport = System.nanoTime();

                @Override
                public void progress(long updated, long issued) {
                    if (printprogress <= 0) {
                        return; // disabled
                    }
                    if (TimeUnit.MILLISECONDS.toSeconds(MathUtils.elapsedMSec(lastReport)) >= printprogress) {
                        LOG.info("Number of ledgers issued={}, updated={}", issued, updated);
                        lastReport = MathUtils.nowInNano();
                    }
                }
            };
            try {
                updateLedgerOp.updateBookieIdInLedgers(oldBookieId, newBookieId, rate, limit, progressable);
            } catch (BKException e) {
                LOG.error("Failed to update ledger metadata", e);
                return -1;
            } catch (IOException e) {
                LOG.error("Failed to update ledger metadata", e);
                return -1;
            }
            return 0;
        }
    }

    /**
     * A facility for reporting update ledger progress.
     */
    public interface UpdateLedgerNotifier {
        void progress(long updated, long issued);
    }


    /**
     * Convert bookie indexes from InterleavedStorage to DbLedgerStorage format
     */
    class ConvertToDbStorageCmd extends MyCommand {
        Options opts = new Options();

        public ConvertToDbStorageCmd() {
            super(CMD_CONVERT_TO_DB_STORAGE);
        }

        @Override
        Options getOptions() {
            return opts;
        }

        @Override
        String getDescription() {
            return "Convert bookie indexes from InterleavedStorage to DbLedgerStorage format";
        }

        String getUsage() {
            return CMD_CONVERT_TO_DB_STORAGE;
        }

        @Override
        int runCmd(CommandLine cmdLine) throws Exception {
            LOG.info("=== Converting to DbLedgerStorage ===");
            ServerConfiguration conf = new ServerConfiguration(bkConf);
            LedgerDirsManager ledgerDirsManager = new LedgerDirsManager(bkConf,
                    bkConf.getLedgerDirs());

            InterleavedLedgerStorage interleavedStorage = new InterleavedLedgerStorage();
            DbLedgerStorage dbStorage = new DbLedgerStorage();

            CheckpointSource checkpointSource = new CheckpointSource() {
                    @Override
                    public Checkpoint newCheckpoint() {
                        return Checkpoint.MAX;
                    }

                    @Override
                    public void checkpointComplete(Checkpoint checkpoint, boolean compact)
                            throws IOException {
                    }
                };

            interleavedStorage.initialize(conf, null, ledgerDirsManager, ledgerDirsManager,
                    checkpointSource, NullStatsLogger.INSTANCE);
            dbStorage.initialize(conf, null, ledgerDirsManager, ledgerDirsManager,
                    checkpointSource, NullStatsLogger.INSTANCE);

            int convertedLedgers = 0;
            for (long ledgerId : interleavedStorage.getActiveLedgersInRange(0, Long.MAX_VALUE)) {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("Converting ledger {}", ledgerId);
                }

                FileInfo fi = getFileInfo(ledgerId);

                Iterable<SortedMap<Long, Long>> entries = getLedgerIndexEntries(ledgerId);

                long numberOfEntries = dbStorage.addLedgerToIndex(ledgerId, fi.isFenced(), fi.getMasterKey(), entries);
                if (LOG.isDebugEnabled()) {
                    LOG.debug("   -- done. fenced={} entries={}", fi.isFenced(), numberOfEntries);
                }

                // Remove index from old storage
                interleavedStorage.deleteLedger(ledgerId);

                if (++convertedLedgers % 1000 == 0) {
                    LOG.info("Converted {} ledgers", convertedLedgers);
                }
            }

            dbStorage.shutdown();
            interleavedStorage.shutdown();

            LOG.info("---- Done Converting ----");
            return 0;
        }
    }

    /**
     * Convert bookie indexes on DbLedgerStorage format from RocksDB to LevelDB
     */
    class ConvertRocksDbToLevelDbCmd extends MyCommand {
        Options opts = new Options();

        public ConvertRocksDbToLevelDbCmd() {
            super(CMD_CONVERT_ROCKSDB_TO_LEVELDB_STORAGE);
        }

        @Override
        Options getOptions() {
            return opts;
        }

        @Override
        String getDescription() {
            return "Convert RocksDB indexed back to LevelDB format";
        }

        String getUsage() {
            return CMD_CONVERT_ROCKSDB_TO_LEVELDB_STORAGE;
        }

        @Override
        int runCmd(CommandLine cmdLine) throws Exception {
            LOG.info("=== Converting RocksDB indexes to LevelDB ===");
            LedgerDirsManager ledgerDirsManager = new LedgerDirsManager(bkConf,
                    bkConf.getLedgerDirs());
            String baseDir = ledgerDirsManager.getAllLedgerDirs().get(0).toString();
            FileSystem fileSystem = FileSystems.getDefault();

            // Convert ledgers db
            LOG.info("-- Converting ledgers DB --");
            String rocksDbledgersPath = fileSystem.getPath(baseDir, "ledgers").toFile().toString();
            String levelDbLedgersPath = fileSystem.getPath(baseDir, "ledgers.ldb").toFile().toString();

            KeyValueStorage rocksDbLedgersStorage = new KeyValueStorageRocksDB(rocksDbledgersPath);
            KeyValueStorage levelDbLedgersStorage = new KeyValueStorageLevelDB(levelDbLedgersPath);

            copyDatabase(rocksDbLedgersStorage, levelDbLedgersStorage);
            rocksDbLedgersStorage.close();
            levelDbLedgersStorage.close();
            LOG.info("-- Converted ledgers DB --");

            LOG.info("-- Converting locations DB --");
            String rocksDbLocationsPath = fileSystem.getPath(baseDir, "locations").toFile().toString();
            String levelDbLocationsPath = fileSystem.getPath(baseDir, "locations.ldb").toFile().toString();

            KeyValueStorage rocksDbLocationsStorage = new KeyValueStorageRocksDB(rocksDbLocationsPath);
            KeyValueStorage levelDbLocationsStorage = new KeyValueStorageLevelDB(levelDbLocationsPath);

            copyDatabase(rocksDbLocationsStorage, levelDbLocationsStorage);
            rocksDbLocationsStorage.close();
            levelDbLocationsStorage.close();
            LOG.info("-- Converted locations DB --");

            // Rename databases and keep backup
            Files.move(fileSystem.getPath(baseDir, "ledgers"), fileSystem.getPath(baseDir, "ledgers.rocksdb.backup"));
            Files.move(fileSystem.getPath(baseDir, "ledgers.ldb"), fileSystem.getPath(baseDir, "ledgers"));

            Files.move(fileSystem.getPath(baseDir, "locations"), fileSystem.getPath(baseDir, "locations.rocksdb.backup"));
            Files.move(fileSystem.getPath(baseDir, "locations.ldb"), fileSystem.getPath(baseDir, "locations"));

            LOG.info("---- Done Converting ----");
            return 0;
        }

        private void copyDatabase(KeyValueStorage source, KeyValueStorage target) throws Exception {
            CloseableIterator<Entry<byte[], byte[]>> iterator = source.iterator();
            try {
                while (iterator.hasNext()) {
                    Entry<byte[], byte[]> entry = iterator.next();
                    target.put(entry.getKey(), entry.getValue());
                }
            } finally {
                iterator.close();
            }
        }
    }

    /**
     * Convert bookie indexes from DbLedgerStorage to InterleavedStorage format
     */
    class ConvertToInterleavedStorageCmd extends MyCommand {
        Options opts = new Options();

        public ConvertToInterleavedStorageCmd() {
            super(CMD_CONVERT_TO_INTERLEAVED_STORAGE);
        }

        @Override
        Options getOptions() {
            return opts;
        }

        @Override
        String getDescription() {
            return "Convert bookie indexes from DbLedgerStorage to InterleavedStorage format";
        }

        String getUsage() {
            return CMD_CONVERT_TO_INTERLEAVED_STORAGE;
        }

        @Override
        int runCmd(CommandLine cmdLine) throws Exception {
            LOG.info("=== Converting DbLedgerStorage ===");
            ServerConfiguration conf = new ServerConfiguration(bkConf);
            LedgerDirsManager ledgerDirsManager = new LedgerDirsManager(bkConf,
                    bkConf.getLedgerDirs());

            DbLedgerStorage dbStorage = new DbLedgerStorage();
            InterleavedLedgerStorage interleavedStorage = new InterleavedLedgerStorage();

            CheckpointSource checkpointSource = new CheckpointSource() {
                    @Override
                    public Checkpoint newCheckpoint() {
                        return Checkpoint.MAX;
                    }

                    @Override
                    public void checkpointComplete(Checkpoint checkpoint, boolean compact)
                            throws IOException {
                    }
                };

            dbStorage.initialize(conf, null, ledgerDirsManager, ledgerDirsManager,
                        checkpointSource, NullStatsLogger.INSTANCE);
            interleavedStorage.initialize(conf, null, ledgerDirsManager, ledgerDirsManager,
                    checkpointSource, NullStatsLogger.INSTANCE);
            LedgerCache interleavedLedgerCache = interleavedStorage.ledgerCache;

            EntryLocationIndex dbEntryLocationIndex = dbStorage.getEntryLocationIndex();

            int convertedLedgers = 0;
            for (long ledgerId : dbStorage.getActiveLedgersInRange(0, Long.MAX_VALUE)) {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("Converting ledger {}", ledgerId);
                }

                interleavedStorage.setMasterKey(ledgerId, dbStorage.readMasterKey(ledgerId));
                if (dbStorage.isFenced(ledgerId)) {
                    interleavedStorage.setFenced(ledgerId);
                }

                long lastEntryInLedger = dbEntryLocationIndex.getLastEntryInLedger(ledgerId);
                for (long entryId = 0; entryId <= lastEntryInLedger; entryId++) {
                    try {
                        long location = dbEntryLocationIndex.getLocation(ledgerId, entryId);
                        if (location != 0L) {
                            interleavedLedgerCache.putEntryOffset(ledgerId, entryId, location);
                        }
                    } catch (Bookie.NoEntryException e) {
                        // Ignore entry
                    }
                }

                if (++convertedLedgers % 1000 == 0) {
                    LOG.info("Converted {} ledgers", convertedLedgers);
                }
            }

            dbStorage.shutdown();

            interleavedLedgerCache.flushLedger(true);
            interleavedStorage.flush();
            interleavedStorage.shutdown();

            String baseDir = ledgerDirsManager.getAllLedgerDirs().get(0).toString();

            // Rename databases and keep backup
            Files.move(FileSystems.getDefault().getPath(baseDir, "ledgers"),
                    FileSystems.getDefault().getPath(baseDir, "ledgers.backup"));

            Files.move(FileSystems.getDefault().getPath(baseDir, "locations"),
                    FileSystems.getDefault().getPath(baseDir, "locations.backup"));

            LOG.info("---- Done Converting {} ledgers ----", convertedLedgers);
            return 0;
        }
    }

    /**
     * Rebuild DbLedgerStorage locations index
     */
    class RebuildDbLedgerLocationsIndexCmd extends MyCommand {
        Options opts = new Options();

        public RebuildDbLedgerLocationsIndexCmd() {
            super(CMD_REBUILD_DB_LEDGER_LOCATIONS_INDEX);
        }

        @Override
        Options getOptions() {
            return opts;
        }

        @Override
        String getDescription() {
            return "Rebuild DbLedgerStorage locations index by scanning the entry logs";
        }

        String getUsage() {
            return CMD_REBUILD_DB_LEDGER_LOCATIONS_INDEX;
        }

        @Override
        int runCmd(CommandLine cmdLine) throws Exception {
            LOG.info("=== Rebuilding bookie index ===");
            ServerConfiguration conf = new ServerConfiguration(bkConf);
            new LocationsIndexRebuildOp(conf).initiate();
            LOG.info("-- Done rebuilding bookie index --");
            return 0;
        }
    }

    final Map<String, MyCommand> commands = new HashMap<String, MyCommand>();
    {
        commands.put(CMD_METAFORMAT, new MetaFormatCmd());
        commands.put(CMD_BOOKIEFORMAT, new BookieFormatCmd());
        commands.put(CMD_RECOVER, new RecoverCmd());
        commands.put(CMD_LEDGER, new LedgerCmd());
        commands.put(CMD_READ_LEDGER_ENTRIES, new ReadLedgerEntriesCmd());
        commands.put(CMD_LISTLEDGERS, new ListLedgersCmd());
        commands.put(CMD_LISTUNDERREPLICATED, new ListUnderreplicatedCmd());
        commands.put(CMD_WHOISAUDITOR, new WhoIsAuditorCmd());
        commands.put(CMD_LEDGERMETADATA, new LedgerMetadataCmd());
        commands.put(CMD_SIMPLETEST, new SimpleTestCmd());
        commands.put(CMD_BOOKIESANITYTEST, new BookieSanityTestCmd());
        commands.put(CMD_READLOG, new ReadLogCmd());
        commands.put(CMD_READJOURNAL, new ReadJournalCmd());
        commands.put(CMD_LASTMARK, new LastMarkCmd());
        commands.put(CMD_AUTORECOVERY, new AutoRecoveryCmd());
        commands.put(CMD_LISTBOOKIES, new ListBookiesCmd());
        commands.put(CMD_UPDATECOOKIE, new UpdateCookieCmd());
        commands.put(CMD_UPDATELEDGER, new UpdateLedgerCmd());
        commands.put(CMD_CONVERT_TO_DB_STORAGE, new ConvertToDbStorageCmd());
        commands.put(CMD_CONVERT_TO_INTERLEAVED_STORAGE, new ConvertToInterleavedStorageCmd());
        commands.put(CMD_CONVERT_ROCKSDB_TO_LEVELDB_STORAGE, new ConvertRocksDbToLevelDbCmd());
        commands.put(CMD_REBUILD_DB_LEDGER_LOCATIONS_INDEX, new RebuildDbLedgerLocationsIndexCmd());
        commands.put(CMD_HELP, new HelpCmd());
    }

    @Override
    public void setConf(Configuration conf) throws Exception {
        bkConf.loadConf(conf);
        journalDirectories = Bookie.getCurrentDirectories(bkConf.getJournalDirs());
        ledgerDirectories = Bookie.getCurrentDirectories(bkConf.getLedgerDirs());
        if (null == bkConf.getIndexDirs()) {
            indexDirectories = ledgerDirectories;
        } else {
            indexDirectories = Bookie.getCurrentDirectories(bkConf.getIndexDirs());
        }
        formatter = EntryFormatter.newEntryFormatter(bkConf, ENTRY_FORMATTER_CLASS);
        LOG.debug("Using entry formatter {}", formatter.getClass().getName());
        pageSize = bkConf.getPageSize();
        entriesPerPage = pageSize / 8;
    }

    private void printShellUsage() {
        System.err.println("Usage: BookieShell [-conf configuration] <command>");
        System.err.println();
        List<String> commandNames = new ArrayList<String>();
        for (MyCommand c : commands.values()) {
            commandNames.add("       " + c.getUsage());
        }
        Collections.sort(commandNames);
        for (String s : commandNames) {
            System.err.println(s);
        }
    }

    @Override
    public int run(String[] args) throws Exception {
        if (args.length <= 0) {
            printShellUsage();
            return -1;
        }
        String cmdName = args[0];
        Command cmd = commands.get(cmdName);
        if (null == cmd) {
            System.err.println("ERROR: Unknown command " + cmdName);
            printShellUsage();
            return -1;
        }
        // prepare new args
        String[] newArgs = new String[args.length - 1];
        System.arraycopy(args, 1, newArgs, 0, newArgs.length);
        return cmd.runCmd(newArgs);
    }

    public static void main(String argv[]) throws Exception {
        BookieShell shell = new BookieShell();
        if (argv.length <= 0) {
            shell.printShellUsage();
            System.exit(-1);
        }

        CompositeConfiguration conf = new CompositeConfiguration();
        // load configuration
        if ("-conf".equals(argv[0])) {
            if (argv.length <= 1) {
                shell.printShellUsage();
                System.exit(-1);
            }
            conf.addConfiguration(new PropertiesConfiguration(
                                  new File(argv[1]).toURI().toURL()));

            String[] newArgv = new String[argv.length - 2];
            System.arraycopy(argv, 2, newArgv, 0, newArgv.length);
            argv = newArgv;
        }


        shell.setConf(conf);
        int res = shell.run(argv);
        System.exit(res);
    }

    ///
    /// Bookie File Operations
    ///

    /**
     * Get the ledger file of a specified ledger.
     *
     * @param ledgerId
     *          Ledger Id
     *
     * @return file object.
     */
    private File getLedgerFile(long ledgerId) {
        String ledgerName = IndexPersistenceMgr.getLedgerName(ledgerId);
        File lf = null;
        for (File d : indexDirectories) {
            lf = new File(d, ledgerName);
            if (lf.exists()) {
                break;
            }
            lf = null;
        }
        return lf;
    }

    /**
     * Get FileInfo for a specified ledger.
     *
     * @param ledgerId
     *          Ledger Id
     * @return read only file info instance
     */
    ReadOnlyFileInfo getFileInfo(long ledgerId) throws IOException {
        File ledgerFile = getLedgerFile(ledgerId);
        if (null == ledgerFile) {
            throw new FileNotFoundException("No index file found for ledger " + ledgerId + ". It may be not flushed yet.");
        }
        ReadOnlyFileInfo fi = new ReadOnlyFileInfo(ledgerFile, null);
        fi.readHeader();
        return fi;
    }

    private synchronized void initEntryLogger() throws IOException {
        if (null == entryLogger) {
            // provide read only entry logger
            entryLogger = new ReadOnlyEntryLogger(bkConf);
        }
    }

    /**
     * scan over entry log
     *
     * @param logId
     *          Entry Log Id
     * @param scanner
     *          Entry Log Scanner
     */
    protected void scanEntryLog(long logId, EntryLogScanner scanner) throws IOException {
        initEntryLogger();
        entryLogger.scanEntryLog(logId, scanner);
    }

    private synchronized List<Journal> getJournals() throws IOException {
        if (null == journals) {
            journals = Lists.newArrayListWithCapacity(bkConf.getJournalDirNames().length);
            for (File journalDir : bkConf.getJournalDirs()) {
                journals.add(new Journal(journalDir, bkConf, new LedgerDirsManager(bkConf, bkConf.getLedgerDirs())));
            }
        }
        return journals;
    }

    /**
     * Scan journal file
     *
     * @param journalId
     *          Journal File Id
     * @param scanner
     *          Journal File Scanner
     */
    protected void scanJournal(Journal journal, long journalId, JournalScanner scanner) throws IOException {
        journal.scanJournal(journalId, 0L, scanner);
    }

    ///
    /// Bookie Shell Commands
    ///

    /**
     * Read ledger meta
     *
     * @param ledgerId
     *          Ledger Id
     */
    protected void readLedgerMeta(long ledgerId) throws Exception {
        System.out.println("===== LEDGER: " + ledgerId + " =====");
        FileInfo fi = getFileInfo(ledgerId);
        byte[] masterKey = fi.getMasterKey();
        if (null == masterKey) {
            System.out.println("master key  : NULL");
        } else {
            System.out.println("master key  : " + bytes2Hex(fi.getMasterKey()));
        }
        long size = fi.size();
        if (size % 8 == 0) {
            System.out.println("size        : " + size);
        } else {
            System.out.println("size : " + size + " (not aligned with 8, may be corrupted or under flushing now)");
        }
        System.out.println("entries     : " + (size / 8));
        System.out.println("isFenced    : " + fi.isFenced());
    }

    /**
     * Read ledger index entires
     *
     * @param ledgerId
     *          Ledger Id
     * @throws IOException
     */
    protected void readLedgerIndexEntries(long ledgerId) throws IOException {
        System.out.println("===== LEDGER: " + ledgerId + " =====");
        FileInfo fi = getFileInfo(ledgerId);
        long size = fi.size();
        System.out.println("size        : " + size);
        long curSize = 0;
        long curEntry = 0;
        LedgerEntryPage lep = new LedgerEntryPage(pageSize, entriesPerPage);
        lep.usePage();
        try {
            while (curSize < size) {
                lep.setLedgerAndFirstEntry(ledgerId, curEntry);
                lep.readPage(fi);

                // process a page
                for (int i=0; i<entriesPerPage; i++) {
                    long offset = lep.getOffset(i * 8);
                    if (0 == offset) {
                        System.out.println("entry " + curEntry + "\t:\tN/A");
                    } else {
                        long entryLogId = offset >> 32L;
                        long pos = offset & 0xffffffffL;
                        System.out.println("entry " + curEntry + "\t:\t(log:" + entryLogId + ", pos: " + pos + ")");
                    }
                    ++curEntry;
                }

                curSize += pageSize;
            }
        } catch (IOException ie) {
            LOG.error("Failed to read index page : ", ie);
            if (curSize + pageSize < size) {
                System.out.println("Failed to read index page @ " + curSize + ", the index file may be corrupted : " + ie.getMessage());
            } else {
                System.out.println("Failed to read last index page @ " + curSize
                                 + ", the index file may be corrupted or last index page is not fully flushed yet : " + ie.getMessage());
            }
        }
    }

    /**
     * Get an iterable over pages of entries and locations for a ledger
     *
     * @param ledgerId
     * @return
     * @throws IOException
     */
    protected Iterable<SortedMap<Long, Long>> getLedgerIndexEntries(final long ledgerId) throws IOException {
        final FileInfo fi = getFileInfo(ledgerId);
        final long size = fi.size();

        final LedgerEntryPage lep = new LedgerEntryPage(pageSize, entriesPerPage);
        lep.usePage();

        final Iterator<SortedMap<Long, Long>> iterator = new Iterator<SortedMap<Long, Long>>() {
            long curSize = 0;
            long curEntry = 0;

            @Override
            public boolean hasNext() {
                return curSize < size;
            }

            @Override
            public SortedMap<Long, Long> next() {
                SortedMap<Long, Long> entries = Maps.newTreeMap();
                lep.setLedgerAndFirstEntry(ledgerId, curEntry);
                try {
                    lep.readPage(fi);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }

                // process a page
                for (int i = 0; i < entriesPerPage; i++) {
                    long offset = lep.getOffset(i * 8);
                    if (offset != 0) {
                        entries.put(curEntry, offset);
                    }
                    ++curEntry;
                }

                curSize += pageSize;
                return entries;
            }

            @Override
            public void remove() {
                throw new RuntimeException("Cannot remove");
            }

        };

        return new Iterable<SortedMap<Long, Long>>() {
            public Iterator<SortedMap<Long, Long>> iterator() {
                return iterator;
            }
        };
    }

    /**
     * Scan over an entry log file.
     *
     * @param logId
     *          Entry Log File id.
     * @param printMsg
     *          Whether printing the entry data.
     */
    protected void scanEntryLog(long logId, final boolean printMsg) throws Exception {
        System.out.println("Scan entry log " + logId + " (" + Long.toHexString(logId) + ".log)");
        scanEntryLog(logId, new EntryLogScanner() {
            @Override
            public boolean accept(long ledgerId) {
                return true;
            }
            @Override
            public void process(long ledgerId, long startPos, ByteBuffer entry) {
                formatEntry(startPos, entry, printMsg);
            }
        });
    }

    /**
     * Scan a journal file
     *
     * @param journalId
     *          Journal File Id
     * @param printMsg
     *          Whether printing the entry data.
     */
    protected void scanJournal(Journal journal, long journalId, final boolean printMsg) throws Exception {
        System.out.println("Scan journal " + journalId + " (" + Long.toHexString(journalId) + ".txn)");
        scanJournal(journal, journalId, new JournalScanner() {
            boolean printJournalVersion = false;
            @Override
            public void process(int journalVersion, long offset, ByteBuffer entry) throws IOException {
                if (!printJournalVersion) {
                    System.out.println("Journal Version : " + journalVersion);
                    printJournalVersion = true;
                }
                formatEntry(offset, entry, printMsg);
            }
        });
    }

    /**
     * Print last log mark
     */
    protected void printLastLogMark() throws IOException {
        for (Journal journal : getJournals()) {
            LogMark lastLogMark = journal.getLastLogMark().getCurMark();
            System.out.println(journal.getJournalDirectory() + " - LastLogMark: Journal Id - " + lastLogMark.getLogFileId() + "("
                    + Long.toHexString(lastLogMark.getLogFileId()) + ".txn), Pos - " + lastLogMark.getLogFileOffset());
        }
    }

    /**
     * Format the message into a readable format.
     *
     * @param pos
     *          File offset of the message stored in entry log file
     * @param recBuff
     *          Entry Data
     * @param printMsg
     *          Whether printing the message body
     */
    private void formatEntry(long pos, ByteBuffer recBuff, boolean printMsg) {
        long ledgerId = recBuff.getLong();
        long entryId = recBuff.getLong();
        int entrySize = recBuff.limit();

        System.out.println("--------- Lid=" + ledgerId + ", Eid=" + entryId
                         + ", ByteOffset=" + pos + ", EntrySize=" + entrySize + " ---------");
        if (entryId == Bookie.METAENTRY_ID_LEDGER_KEY) {
            int masterKeyLen = recBuff.getInt();
            byte[] masterKey = new byte[masterKeyLen];
            recBuff.get(masterKey);
            System.out.println("Type:           META");
            System.out.println("MasterKey:      " + bytes2Hex(masterKey));
            System.out.println();
            return;
        }
        if (entryId == Bookie.METAENTRY_ID_FENCE_KEY) {
            System.out.println("Type:           META");
            System.out.println("Fenced");
            System.out.println();
            return;
        }
        // process a data entry
        long lastAddConfirmed = recBuff.getLong();
        System.out.println("Type:           DATA");
        System.out.println("LastConfirmed:  " + lastAddConfirmed);
        if (!printMsg) {
            System.out.println();
            return;
        }
        // skip digest checking
        recBuff.position(32 + 8);
        System.out.println("Data:");
        System.out.println();
        try {
            byte[] ret = new byte[recBuff.remaining()];
            recBuff.get(ret);
            formatter.formatEntry(ret);
        } catch (Exception e) {
            System.out.println("N/A. Corrupted.");
        }
        System.out.println();
    }

    static String bytes2Hex(byte[] data) {
        StringBuilder sb = new StringBuilder(data.length * 2);
        Formatter formatter = new Formatter(sb);
        for (byte b : data) {
            formatter.format("%02x", b);
        }
        formatter.close();
        return sb.toString();
    }

    private static int getOptionIntValue(CommandLine cmdLine, String option, int defaultVal) {
        if (cmdLine.hasOption(option)) {
            String val = cmdLine.getOptionValue(option);
            try {
                return Integer.parseInt(val);
            } catch (NumberFormatException nfe) {
                System.err.println("ERROR: invalid value for option " + option + " : " + val);
                return defaultVal;
            }
        }
        return defaultVal;
    }

    private static long getOptionLongValue(CommandLine cmdLine, String option, long defaultVal) {
        if (cmdLine.hasOption(option)) {
            String val = cmdLine.getOptionValue(option);
            try {
                return Long.parseLong(val);
            } catch (NumberFormatException nfe) {
                System.err.println("ERROR: invalid value for option " + option + " : " + val);
                return defaultVal;
            }
        }
        return defaultVal;
    }

    private static boolean getOptionBooleanValue(CommandLine cmdLine, String option, boolean defaultVal) {
        if (cmdLine.hasOption(option)) {
            String val = cmdLine.getOptionValue(option);
            return Boolean.parseBoolean(val);
        }
        return defaultVal;
    }

    private static boolean getOptionalValue(String optValue, String optName) {
        if (StringUtils.equals(optValue, optName)) {
            return true;
        }
        return false;
    }
}
