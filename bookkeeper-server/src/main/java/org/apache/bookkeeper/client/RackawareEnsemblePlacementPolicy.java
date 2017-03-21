/*
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

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.bookkeeper.client.BKException.BKNotEnoughBookiesException;
import org.apache.bookkeeper.conf.Configurable;
import org.apache.bookkeeper.net.BookieSocketAddress;
import org.apache.bookkeeper.net.DNSToSwitchMapping;
import org.apache.bookkeeper.net.NetworkTopology;
import org.apache.bookkeeper.net.Node;
import org.apache.bookkeeper.net.NodeBase;
import org.apache.bookkeeper.net.ScriptBasedMapping;
import org.apache.bookkeeper.util.ReflectionUtils;
import org.apache.commons.configuration.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.carrotsearch.hppc.IntArrayList;
import com.carrotsearch.hppc.procedures.IntProcedure;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;

/**
 * Simple rackware ensemble placement policy.
 *
 * Make most of the class and methods as protected, so it could be extended to implement other algorithms.
 */
public class RackawareEnsemblePlacementPolicy implements EnsemblePlacementPolicy {

    static final Logger LOG = LoggerFactory.getLogger(RackawareEnsemblePlacementPolicy.class);

    public static final String REPP_DNS_RESOLVER_CLASS = "reppDnsResolverClass";

    /**
     * Notifier used by the RackawareEnsemblePlacementPolicy to get notified if a rack changes for a bookie
     */
    public static interface RackChangeNotifier {
        void registerRackChangeListener(RackawareEnsemblePlacementPolicy rackawarePolicy);
    }

    /**
     * Predicate used when choosing an ensemble.
     */
    protected static interface Predicate {
        boolean apply(BookieNode candidate, Ensemble chosenBookies);
    }

    /**
     * Ensemble used to hold the result of an ensemble selected for placement.
     */
    protected static interface Ensemble {

        /**
         * Append the new bookie node to the ensemble.
         *
         * @param node
         *          new candidate bookie node.
         */
        public void addBookie(BookieNode node);

        /**
         * @return list of addresses representing the ensemble
         */
        public ArrayList<BookieSocketAddress> toList();
    }

    protected static class TruePredicate implements Predicate {

        public static final TruePredicate instance = new TruePredicate();

        @Override
        public boolean apply(BookieNode candidate, Ensemble chosenNodes) {
            return true;
        }

    }

    protected static class EnsembleForReplacement implements Ensemble {

        public static final EnsembleForReplacement instance = new EnsembleForReplacement();
        static final ArrayList<BookieSocketAddress> EMPTY_LIST = new ArrayList<BookieSocketAddress>(0);

        @Override
        public void addBookie(BookieNode node) {
            // do nothing
        }

        @Override
        public ArrayList<BookieSocketAddress> toList() {
            return EMPTY_LIST;
        }

    }

    /**
     * A predicate checking the rack coverage for write quorum in {@link RoundRobinDistributionSchedule},
     * which ensures that a write quorum should be covered by at least two racks.
     */
    protected static class RRRackCoverageEnsemble implements Predicate, Ensemble {

        class QuorumCoverageSet {
            Set<String> racks = new HashSet<String>();
            int seenBookies = 0;

            boolean apply(BookieNode candidate) {
                if (seenBookies + 1 == writeQuorumSize) {
                    return racks.size() > (racks.contains(candidate.getNetworkLocation()) ? 1 : 0);
                }
                return true;
            }

            void addBookie(BookieNode candidate) {
                ++seenBookies;
                racks.add(candidate.getNetworkLocation());
            }
        }

        final int ensembleSize;
        final int writeQuorumSize;
        final ArrayList<BookieNode> chosenNodes;
        private final QuorumCoverageSet[] quorums;

        protected RRRackCoverageEnsemble(int ensembleSize, int writeQuorumSize) {
            this.ensembleSize = ensembleSize;
            this.writeQuorumSize = writeQuorumSize;
            this.chosenNodes = new ArrayList<BookieNode>(ensembleSize);
            this.quorums = new QuorumCoverageSet[ensembleSize];
        }

        @Override
        public boolean apply(BookieNode candidate, Ensemble ensemble) {
            if (ensemble != this) {
                return false;
            }
            // candidate position
            int candidatePos = chosenNodes.size();
            int startPos = candidatePos - writeQuorumSize + 1;
            for (int i = startPos; i <= candidatePos; i++) {
                int idx = (i + ensembleSize) % ensembleSize;
                if (null == quorums[idx]) {
                    quorums[idx] = new QuorumCoverageSet();
                }
                if (!quorums[idx].apply(candidate)) {
                    return false;
                }
            }
            return true;
        }

        @Override
        public void addBookie(BookieNode node) {
            int candidatePos = chosenNodes.size();
            int startPos = candidatePos - writeQuorumSize + 1;
            for (int i = startPos; i <= candidatePos; i++) {
                int idx = (i + ensembleSize) % ensembleSize;
                if (null == quorums[idx]) {
                    quorums[idx] = new QuorumCoverageSet();
                }
                quorums[idx].addBookie(node);
            }
            chosenNodes.add(node);
        }

        @Override
        public ArrayList<BookieSocketAddress> toList() {
            ArrayList<BookieSocketAddress> addresses = new ArrayList<BookieSocketAddress>(ensembleSize);
            for (BookieNode bn : chosenNodes) {
                addresses.add(bn.getAddr());
            }
            return addresses;
        }

        @Override
        public String toString() {
            return chosenNodes.toString();
        }

    }

    protected static class BookieNode implements Node {

        private final BookieSocketAddress addr; // identifier of a bookie node.

        private int level; // the level in topology tree
        private Node parent; // its parent in topology tree
        private String location = NetworkTopology.DEFAULT_RACK; // its network location
        private final String name;

        BookieNode(BookieSocketAddress addr, String networkLoc) {
            this.addr = addr;
            this.name = addr.toString();
            setNetworkLocation(networkLoc);
        }

        public BookieSocketAddress getAddr() {
            return addr;
        }

        @Override
        public int getLevel() {
            return level;
        }

        @Override
        public void setLevel(int level) {
            this.level = level;
        }

        @Override
        public Node getParent() {
            return parent;
        }

        @Override
        public void setParent(Node parent) {
            this.parent = parent;
        }

        @Override
        public String getName() {
            return name;
        }

        @Override
        public String getNetworkLocation() {
            return location;
        }

        @Override
        public void setNetworkLocation(String location) {
            this.location = location;
        }

        @Override
        public int hashCode() {
            return name.hashCode();
        }

        @Override
        public boolean equals(Object obj) {
            if (!(obj instanceof BookieNode)) {
                return false;
            }
            BookieNode other = (BookieNode) obj;
            return getName().equals(other.getName());
        }

        @Override
        public String toString() {
            return String.format("<Bookie:%s>", name);
        }

    }

    static class DefaultResolver implements DNSToSwitchMapping {

        @Override
        public List<String> resolve(List<BookieSocketAddress> bookieAddressList) {
            List<String> rNames = new ArrayList<String>(bookieAddressList.size());
            for (@SuppressWarnings("unused") BookieSocketAddress bookieAddress : bookieAddressList) {
                rNames.add(NetworkTopology.DEFAULT_RACK);
            }
            return rNames;
        }

        @Override
        public void reloadCachedMappings() {
            // nop
        }

    };

    // for now, we just maintain the writable bookies' topology
    private final NetworkTopology topology;
    private DNSToSwitchMapping dnsResolver;
    private final Map<BookieSocketAddress, BookieNode> knownBookies;
    private BookieNode localNode;
    private final ReentrantReadWriteLock rwLock;

    // Initialize to empty set
    protected ImmutableSet<BookieSocketAddress> readOnlyBookies = ImmutableSet.of();

    public RackawareEnsemblePlacementPolicy() {
        topology = new NetworkTopology();
        knownBookies = new ConcurrentHashMap<BookieSocketAddress, BookieNode>();

        rwLock = new ReentrantReadWriteLock();
    }

    private BookieNode createBookieNode(BookieSocketAddress addr) {
        return new BookieNode(addr, resolveNetworkLocation(addr));
    }

    @Override
    public EnsemblePlacementPolicy initialize(Configuration conf) {
        String dnsResolverName = conf.getString(REPP_DNS_RESOLVER_CLASS, ScriptBasedMapping.class.getName());
        try {
            dnsResolver = ReflectionUtils.newInstance(dnsResolverName, DNSToSwitchMapping.class);
            if (dnsResolver instanceof Configurable) {
                ((Configurable) dnsResolver).setConf(conf);
            }
            if (dnsResolver instanceof RackChangeNotifier) {
                ((RackChangeNotifier) dnsResolver).registerRackChangeListener(this);
            }
        } catch (RuntimeException re) {
            LOG.info("Failed to initialize DNS Resolver {}, used default subnet resolver.", dnsResolverName, re);
            dnsResolver = new DefaultResolver();
        }

        BookieNode bn;
        try {
            bn = createBookieNode(new BookieSocketAddress(InetAddress.getLocalHost().getHostAddress(), 0));
        } catch (UnknownHostException e) {
            LOG.error("Failed to get local host address : ", e);
            bn = null;
        }
        localNode = bn;
        LOG.info("Initialize rackaware ensemble placement policy @ {} @ {} : {}.",
                 new Object[] { localNode, null == localNode ? "Unknown" : localNode.getNetworkLocation(),
                         dnsResolver.getClass().getName() });
        return this;
    }

    @Override
    public void uninitalize() {
        // do nothing
    }

    private String resolveNetworkLocation(BookieSocketAddress addr) {
        List<BookieSocketAddress> bookieAddressList = new ArrayList<BookieSocketAddress>(1);
        bookieAddressList.add(addr);

        // resolve network addresses
        List<String> rNames = dnsResolver.resolve(bookieAddressList);
        String netLoc;
        if (null == rNames) {
            LOG.warn("Failed to resolve network location for {}, using default rack for them : {}.", bookieAddressList,
                    NetworkTopology.DEFAULT_RACK);
            netLoc = NetworkTopology.DEFAULT_RACK;
        } else {
            netLoc = rNames.get(0);
        }
        return netLoc;
    }

    @Override
    public Set<BookieSocketAddress> onClusterChanged(Set<BookieSocketAddress> writableBookies,
            Set<BookieSocketAddress> readOnlyBookies) {
        rwLock.writeLock().lock();
        try {
            ImmutableSet<BookieSocketAddress> joinedBookies, leftBookies, deadBookies;
            Set<BookieSocketAddress> oldBookieSet = knownBookies.keySet();
            // left bookies : bookies in known bookies, but not in new writable bookie cluster.
            leftBookies = Sets.difference(oldBookieSet, writableBookies).immutableCopy();
            // joined bookies : bookies in new writable bookie cluster, but not in known bookies
            joinedBookies = Sets.difference(writableBookies, oldBookieSet).immutableCopy();
            // dead bookies.
            deadBookies = Sets.difference(leftBookies, readOnlyBookies).immutableCopy();
            if (LOG.isDebugEnabled()) {
                LOG.debug(
                        "Cluster changed : left bookies are {}, joined bookies are {}, while dead bookies are {}.",
                        new Object[] { leftBookies, joinedBookies, deadBookies });
            }

            // node left
            for (BookieSocketAddress addr : leftBookies) {
                BookieNode node = knownBookies.remove(addr);
                topology.remove(node);
                if (LOG.isDebugEnabled()) {
                    LOG.debug("Cluster changed : bookie {} left from cluster.", addr);
                }
            }

            // node joined
            for (BookieSocketAddress addr : joinedBookies) {
                BookieNode node = createBookieNode(addr);
                topology.add(node);
                knownBookies.put(addr, node);
                if (LOG.isDebugEnabled()) {
                    LOG.debug("Cluster changed : bookie {} joined the cluster.", addr);
                }
            }

            if (!readOnlyBookies.isEmpty()) {
                this.readOnlyBookies = ImmutableSet.copyOf(readOnlyBookies);
            }

            return deadBookies;
        } finally {
            rwLock.writeLock().unlock();
        }
    }

    private Set<Node> convertBookiesToNodes(Set<BookieSocketAddress> excludeBookies) {
        Set<Node> nodes = new HashSet<Node>();
        for (BookieSocketAddress addr : excludeBookies) {
            BookieNode bn = knownBookies.get(addr);
            if (null == bn) {
                bn = createBookieNode(addr);
            }
            nodes.add(bn);
        }
        return nodes;
    }

    private Set<String> getNetworkLocations(Set<Node> bookieNodes) {
        Set<String> networkLocs = new HashSet<String>();
        for (Node bookieNode : bookieNodes) {
            networkLocs.add(bookieNode.getNetworkLocation());
        }
        return networkLocs;
    }

    @Override
    public ArrayList<BookieSocketAddress> newEnsemble(int ensembleSize, int writeQuorumSize,
            Set<BookieSocketAddress> excludeBookies) throws BKNotEnoughBookiesException {
        rwLock.readLock().lock();
        try {
            Set<Node> excludeNodes = convertBookiesToNodes(excludeBookies);
            RRRackCoverageEnsemble ensemble = new RRRackCoverageEnsemble(ensembleSize, writeQuorumSize);
            BookieNode prevNode = null;
            int numRacks = topology.getNumOfRacks();
            // only one rack, use the random algorithm.
            if (numRacks < 2) {
                List<BookieNode> bns = selectRandom(ensembleSize, excludeNodes,
                        EnsembleForReplacement.instance);
                ArrayList<BookieSocketAddress> addrs = new ArrayList<BookieSocketAddress>(ensembleSize);
                for (BookieNode bn : bns) {
                    addrs.add(bn.addr);
                }
                return addrs;
            }
            // pick nodes by racks, to ensure there is at least two racks per write quorum.
            prevNode = selectRandom(1, excludeNodes, ensemble).get(0);
            for (int i = 1; i < ensembleSize; i++) {
                String curRack;
                if (null == prevNode) {
                    if (null == localNode) {
                        curRack = NodeBase.ROOT;
                    } else {
                        curRack = localNode.getNetworkLocation();
                    }
                } else {
                    curRack = "~" + prevNode.getNetworkLocation();
                }
                prevNode = selectFromRack(curRack, excludeNodes, ensemble, ensemble);
            }
            return ensemble.toList();
        } finally {
            rwLock.readLock().unlock();
        }
    }

    /**
     * It is a best effort way to pick a new bookie that is on a different rack from the bookies in the ensemble
     * excluding the bookie to replace. If there are no bookies available on a different rack, we fall back to selecting
     * a random bookie from the cluster.
     */
    @Override
    public BookieSocketAddress replaceBookie(BookieSocketAddress bookieToReplace,
            Set<BookieSocketAddress> currentEnsemble, Set<BookieSocketAddress> excludeBookies)
            throws BKNotEnoughBookiesException {
        rwLock.readLock().lock();
        try {
            BookieNode bn = knownBookies.get(bookieToReplace);
            if (null == bn) {
                bn = createBookieNode(bookieToReplace);
            }

            Set<Node> ensembleNodes = convertBookiesToNodes(currentEnsemble);
            Set<Node> excludeNodes = convertBookiesToNodes(excludeBookies);

            excludeNodes.addAll(ensembleNodes);
            excludeNodes.add(bn);
            ensembleNodes.remove(bn);

            Set<String> networkLocationsToBeExcluded = getNetworkLocations(ensembleNodes);

            if (LOG.isDebugEnabled()) {
                LOG.debug("Try to choose a new bookie to replace {} from ensemble {}, excluding {}.",
                        new Object[] { bookieToReplace, ensembleNodes, excludeNodes });
            }
            // pick a candidate that should not belong to the same rack as the ensemble node racks
            BookieNode candidate = selectFromRacks(networkLocationsToBeExcluded, excludeNodes,
                    EnsembleForReplacement.instance);
            if (LOG.isDebugEnabled()) {
                LOG.debug("Bookie {} is chosen to replace bookie {}.", candidate, bn);
            }
            return candidate.addr;
        } finally {
            rwLock.readLock().unlock();
        }
    }

    protected BookieNode selectFromRack(String networkLocation, Set<Node> excludeBookies, Predicate predicate,
            Ensemble ensemble) throws BKNotEnoughBookiesException {
        // select one from local rack
        try {
            return selectRandomFromRack(networkLocation, excludeBookies, predicate, ensemble);
        } catch (BKNotEnoughBookiesException e) {
            LOG.warn("Failed to choose a bookie from {} : "
                     + "excluded {}, fallback to choose bookie randomly from the cluster.",
                    networkLocation, excludeBookies);
            // randomly choose one from whole cluster, ignore the provided predicate.
            return selectRandom(1, excludeBookies, ensemble).get(0);
        }
    }

    /**
     * It randomly selects a {@link BookieNode} that is not on the <i>excludeRacks</i> set, excluding the nodes in
     * <i>excludeBookies</i> set. If it fails to find one, it selects a random {@link BookieNode} from the whole
     * cluster.
     */
    protected BookieNode selectFromRacks(Set<String> excludeRacks, Set<Node> excludeBookies, Ensemble ensemble)
            throws BKNotEnoughBookiesException {
        List<BookieNode> knownNodes = new ArrayList<>(knownBookies.values());
        Collections.shuffle(knownNodes);

        for (BookieNode knownNode : knownNodes) {
            if (excludeBookies.contains(knownNode)) {
                continue;
            }
            if (excludeRacks.contains(knownNode.getNetworkLocation())) {
                continue;
            }
            return knownNode;
        }
        LOG.warn("Failed to choose a bookie: excluded {}, fallback to choose bookie randomly from the cluster.",
                excludeBookies);
        // randomly choose one from whole cluster
        return selectRandom(1, excludeBookies, ensemble).get(0);
    }

    protected String getRemoteRack(BookieNode node) {
        return "~" + node.getNetworkLocation();
    }

    /**
     * Choose random node under a given network path.
     *
     * @param netPath
     *          network path
     * @param excludeBookies
     *          exclude bookies
     * @param predicate
     *          predicate to check whether the target is a good target.
     * @param ensemble
     *          ensemble structure
     * @return chosen bookie.
     */
    protected BookieNode selectRandomFromRack(String netPath, Set<Node> excludeBookies, Predicate predicate,
            Ensemble ensemble) throws BKNotEnoughBookiesException {
        List<Node> leaves = new ArrayList<Node>(topology.getLeaves(netPath));
        Collections.shuffle(leaves);
        for (Node n : leaves) {
            if (excludeBookies.contains(n)) {
                continue;
            }
            if (!(n instanceof BookieNode) || !predicate.apply((BookieNode) n, ensemble)) {
                continue;
            }
            BookieNode bn = (BookieNode) n;
            // got a good candidate
            ensemble.addBookie(bn);
            // add the candidate to exclude set
            excludeBookies.add(bn);
            return bn;
        }
        throw new BKNotEnoughBookiesException();
    }

    /**
     * Choose a random node from whole cluster.
     *
     * @param numBookies
     *          number bookies to choose
     * @param excludeBookies
     *          bookies set to exclude.
     * @param ensemble
     *          ensemble to hold the bookie chosen.
     * @return the bookie node chosen.
     * @throws BKNotEnoughBookiesException
     */
    protected List<BookieNode> selectRandom(int numBookies, Set<Node> excludeBookies, Ensemble ensemble)
            throws BKNotEnoughBookiesException {
        List<BookieNode> allBookies = new ArrayList<BookieNode>(knownBookies.values());
        Collections.shuffle(allBookies);
        List<BookieNode> newBookies = new ArrayList<BookieNode>(numBookies);
        for (BookieNode bookie : allBookies) {
            if (excludeBookies.contains(bookie)) {
                continue;
            }
            ensemble.addBookie(bookie);
            excludeBookies.add(bookie);
            newBookies.add(bookie);
            --numBookies;
            if (numBookies == 0) {
                return newBookies;
            }
        }
        if (LOG.isDebugEnabled()) {
            LOG.debug("Failed to find {} bookies : excludeBookies {}, allBookies {}.", new Object[] {
                    numBookies, excludeBookies, allBookies });
        }
        throw new BKNotEnoughBookiesException();
    }

    public void onBookieRackChange(List<BookieSocketAddress> bookieAddressList) {
        rwLock.writeLock().lock();
        try {
            for (BookieSocketAddress bookieAddress : bookieAddressList) {
                BookieNode node = knownBookies.get(bookieAddress);
                if (node != null) {
                    // refresh the rack info if its a known bookie
                    topology.remove(node);
                    topology.add(createBookieNode(bookieAddress));
                }
            }
        } finally {
            rwLock.writeLock().unlock();
        }
    }

    @Override
    public IntArrayList reorderReadSequence(ArrayList<BookieSocketAddress> ensemble, IntArrayList writeSet) {
        // If all the bookies in the write set are available, simply return the original write set,
        // to avoid creating more lists
        boolean isAnyBookieUnavailable = false;
        for (int i = 0; i < ensemble.size(); i++) {
            BookieSocketAddress bookieAddr = ensemble.get(i);
            if (!knownBookies.containsKey(bookieAddr) && !readOnlyBookies.contains(bookieAddr)) {
                // Found at least one bookie not available in the ensemble
                isAnyBookieUnavailable = true;
                break;
            }
        }

        if (!isAnyBookieUnavailable) {
            return writeSet;
        }

        IntArrayList finalList = new IntArrayList(writeSet.size());
        IntArrayList readOnlyList = new IntArrayList(writeSet.size());
        IntArrayList unAvailableList = new IntArrayList(writeSet.size());
        writeSet.forEach((IntProcedure) idx -> {
            BookieSocketAddress address = ensemble.get(idx);
            if (null == knownBookies.get(address)) {
                // there isn't too much differences between readonly bookies from unavailable bookies. since there
                // is no write requests to them, so we shouldn't try reading from readonly bookie in prior to
                // writable bookies.
                if ((null == readOnlyBookies) || !readOnlyBookies.contains(address)) {
                    unAvailableList.add(idx);
                } else {
                    readOnlyList.add(idx);
                }
            } else {
                finalList.add(idx);
            }
        });
        finalList.addAll(readOnlyList);
        finalList.addAll(unAvailableList);
        return finalList;
    }
}
