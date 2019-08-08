package sk.demtec.neo4j.plugins.indexer.svc;

import java.io.IOException;
import java.util.Date;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Map.Entry;

import org.codehaus.jackson.map.ObjectMapper;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.index.Index;
import org.neo4j.helpers.collection.MapUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sk.demtec.neo4j.plugins.indexer.StringUtil;
import sk.demtec.neo4j.plugins.indexer.svc.NodeIndexer.ReindexStatus;
import sk.demtec.neo4j.plugins.indexer.svc.NodeIndexer.Status;

public class NodeIndexerService {
    private static final Logger log = LoggerFactory.getLogger(NodeIndexerService.class);
    private GraphDatabaseService graphDb;
    private static final int COMMIT_SIZE = 10000;
    private static Index<Node> nodeIndex;
    private static NodeIndexer nodeIndexer;
    private static final String NODE_FULLTEXT = "node_fulltext";
    private static final String NODE_FULLTEXT_KEY = "_text";
    private static final String NODE_FULLTEXT_NODE_INDEXER = "nodeIndexer"; // pod akym klucom sa uklada JSON s nodeIndexerom do Lucene konfiguracie
    private static final Object lockNodeIndex = new Object();
    private static final Object lockNodeIndexer = new Object();
    private static final Object lockNodeReindexRun = new Object();
    private static ReindexThred nodeReindexThred = null;
    public static boolean savingNodeReindex = false;
    // priznak pomocou ktoreho dame threadu najavo ze ma skoncit. interrupt na vlakno sposoboval ukoncenie Neo4j transakcie a nejaku jeho chybu a nedalo sa to
    // rozumne odchytit
    private static boolean nodeReindexThredInterrupt = false;
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    public NodeIndexerService(GraphDatabaseService graphDb) {
        this.graphDb = graphDb;
    }

    /**
     * Ziska konfiguraciu z cache. Vsetci pristupuju cez tuto metodu. Iba metoda save pracuje tiez priamo s indexom.
     */
    public NodeIndexer getNodeIndexer() throws IndexerException {
        synchronized (lockNodeIndexer) {
            if (nodeIndexer == null) {
                Index<Node> nodeIndex = getNodeIndex();
                String json = null;
                try (Transaction tx = graphDb.beginTx()) {
                    json = graphDb.index().getConfiguration(nodeIndex).get(NODE_FULLTEXT_NODE_INDEXER);
                    tx.success();
                }
                if (json != null) {
                    try {
                        nodeIndexer = OBJECT_MAPPER.readValue(json, NodeIndexer.class);
                    } catch (IOException e) {
                        log.error("Error", e);
                        throw new IndexerException("Error", e);
                    }
                }
            }
            return nodeIndexer;
        }
    }

    /**
     * Ulozi konfiguraciu do DB a ulozi v cache. Ukladame do Lucene konfiguracie. Lucene konfiguraciu je mozne pouzit na ulozenie vlastnych konfiguracnych
     * parametrov.
     */
    public NodeIndexer saveNodeIndexer(NodeIndexer indexer) throws IndexerException {
        synchronized (lockNodeIndexer) {
            try {
                savingNodeReindex = true;
                String json = OBJECT_MAPPER.writeValueAsString(indexer);
                // ulozime a vlozime do cahce
                Index<Node> nodeIndex = getNodeIndex();
                try (Transaction tx = graphDb.beginTx()) {
                    graphDb.index().setConfiguration(nodeIndex, NODE_FULLTEXT_NODE_INDEXER, json);
                    tx.success();
                }
                nodeIndexer = indexer;
                return getNodeIndexer();
            } catch (IOException e) {
                log.error("Error", e);
                throw new IndexerException("Error", e);
            } finally {
                savingNodeReindex = false;
            }
        }
    }

    /**
     * Ziska index pre uzly a ak neexistuje tak ho vytvori. Ulozi ho v "cache".
     */
    public Index<Node> getNodeIndex() throws IndexerException {
        synchronized (lockNodeIndex) {
            try {
                if (nodeIndex == null) {
                    try (Transaction tx = graphDb.beginTx()) {
                        nodeIndex = graphDb.index().forNodes(NODE_FULLTEXT, MapUtil.stringMap(org.neo4j.graphdb.index.IndexManager.PROVIDER,
                                "lucene", "type", "fulltext", "to_lower_case", "false"));
                        tx.success();
                    }
                }
                return nodeIndex;
            } catch (Exception e) {
                log.error("Error", e);
                throw new IndexerException("Error", e);
            }
        }
    }

    /**
     * Zmaze index.
     */
    private void deleteNodeIndex() {
        synchronized (lockNodeIndex) {
            if (nodeIndex != null) {
                try (Transaction tx = graphDb.beginTx()) {
                    nodeIndex.delete();
                    tx.success();
                }
                nodeIndex = null;
            }
        }
    }

    public void startReindexNodes() throws IndexerException {
        synchronized (lockNodeReindexRun) {
            try {
                // check if running
                NodeIndexer indexer = getNodeIndexer();
                if (indexer == null) {
                    return;
                }
                if (ReindexStatus.REINDEX_RUNNING.equals(indexer.getReindexStatus())) {
                    throw new IndexerException("Reindex is running");
                } else if (Status.DISABLED.equals(indexer.getStatus())) {
                    throw new IndexerException("Indexer is disabled");
                }
                // save start
                indexer.setReindexStatus(ReindexStatus.REINDEX_RUNNING);
                indexer.setLastRunStartDate(new Date());
                indexer.setLastRunEndDate(null);
                indexer.setLastRunIndexItemsCurrent(new Long(0));
                indexer.setLastRunIndexItemsTotal(new Long(0));
                saveNodeIndexer(indexer);
                nodeReindexThredInterrupt = false;
                nodeReindexThred = new ReindexThred(graphDb);
                nodeReindexThred.start();
            } catch (Exception e) {
                log.error("Error", e);
                // save error
                NodeIndexer indexerDTO = getNodeIndexer();
                if (indexerDTO != null) {
                    indexerDTO.setReindexStatus(ReindexStatus.REINDEX_ERROR);
                    indexerDTO.setLastRunEndDate(new Date());
                    saveNodeIndexer(indexerDTO);
                }
                throw new IndexerException("Error", e);
            }
        }
    }

    public void interruptReindexNodes() throws IndexerException {
        synchronized (lockNodeReindexRun) {
            if (nodeReindexThred != null && nodeReindexThred.isAlive()) {
                // pomocou tejto premennej dame vediet vlaknu ze by malo skoncit
                nodeReindexThredInterrupt = true;
            } else {
                // je mozne ze je stav running takze sme sa sem dostali ale v skutocnosti uz thread nebezi
                NodeIndexer indexer = getNodeIndexer();
                if (indexer != null) {
                    indexer.setReindexStatus(NodeIndexer.ReindexStatus.REINDEX_INTERRUPTED);
                    indexer.setLastRunEndDate(null);
                    saveNodeIndexer(indexer);
                }
            }
        }
    }

    public void disable() throws IndexerException {
        try {
            NodeIndexer indexer = getNodeIndexer();
            if (indexer == null) {
                return;
            }
            if (ReindexStatus.REINDEX_RUNNING.equals(indexer.getReindexStatus())) {
                throw new IndexerException("Reindex is running");
            } else if (Status.DISABLED.equals(indexer.getStatus())) {
                throw new IndexerException("Indexer is disabled");
            }
            indexer.setStatus(NodeIndexer.Status.DISABLED);
            saveNodeIndexer(indexer);
        } catch (Exception e) {
            log.error("Error", e);
            throw new IndexerException("Error", e);
        }
    }

    public void enable() throws IndexerException {
        try {
            NodeIndexer indexer = getNodeIndexer();
            if (indexer == null) {
                return;
            }
            if (!Status.DISABLED.equals(indexer.getStatus())) {
                throw new IndexerException("Indexer is not disabled");
            }
            indexer.setStatus(NodeIndexer.Status.ENABLED);
            saveNodeIndexer(indexer);
        } catch (Exception e) {
            log.error("Error", e);
            throw new IndexerException("Error", e);
        }
    }

    private class ReindexThred extends Thread {
        private GraphDatabaseService graphDb;

        public ReindexThred(GraphDatabaseService graphDb) {
            this.graphDb = graphDb;
        }

        public void run() {
            try {
                deleteNodeIndex();
                // okamzite ho vytvorime pretoze medzitym ak by sa niekto nanho pytal, ale robime to v osobitnej TX
                getNodeIndex();
                NodeIndexer indexer = getNodeIndexer();
                if (indexer == null) {
                    return;
                }
                indexer.setLastRunIndexItemsCurrent(new Long(0));
                indexer.setLastRunIndexItemsTotal(countAllNodes());
                saveNodeIndexer(indexer);
                long count = 0;
                // ak nema vyznam iterovat cez vsetky uzly potom neiterujeme
                if (indexer != null && indexer.getPropertiesFullText() != null && indexer.getPropertiesFullText().size() > 0) {
                    Transaction tx = null;
                    try {
                        tx = graphDb.beginTx();
                        // TODO [rda] - v pripade, ze sa paralelne prida uzol moze nastat problem ze sa zmeni interne poradie ako vracia Db uzly a
                        // nezaindexujeme niektory item
                        ResourceIterator<Node> nodes = graphDb.getAllNodes().iterator();
                        while (nodes.hasNext()) {
                            if (nodeReindexThredInterrupt == true) {
                                throw new InterruptedException();
                            }
                            Node node = (Node) nodes.next();
                            addToNodeIndex(node, indexer);
                            // comit at COMMIT SIZE
                            if ((++count % COMMIT_SIZE) == 0) {
                                // save current count
                                indexer = getNodeIndexer();
                                indexer.setLastRunIndexItemsCurrent(count);
                                saveNodeIndexer(indexer);
                                tx.success();
                                tx.close();
                                tx = graphDb.beginTx();
                            }
                        }
                        tx.success();
                    } finally {
                        tx.close();
                    }
                }
                // save success
                indexer = getNodeIndexer();
                indexer.setReindexStatus(NodeIndexer.ReindexStatus.REINDEX_SUCCESS);
                indexer.setLastRunIndexItemsCurrent(count);
                indexer.setLastRunIndexItemsTotal(count);
                indexer.setLastRunEndDate(new Date());
                saveNodeIndexer(indexer);
            } catch (InterruptedException ie) {
                // save interrupted
                try {
                    NodeIndexer indexer = getNodeIndexer();
                    indexer.setReindexStatus(NodeIndexer.ReindexStatus.REINDEX_INTERRUPTED);
                    indexer.setLastRunEndDate(null);
                    saveNodeIndexer(indexer);
                } catch (IndexerException e) {
                    log.error("Error", e);
                }
            } catch (Exception e) {
                log.error("Error", e);
                // save error
                try {
                    // skontrolujeme dostupnost pretoze tu sa mozme dostat aj v situacii ze je stopovana DB
                    if (graphDb.isAvailable(1)) {
                        NodeIndexer indexer = getNodeIndexer();
                        indexer.setReindexStatus(NodeIndexer.ReindexStatus.REINDEX_ERROR);
                        indexer.setLastRunEndDate(new Date());
                        saveNodeIndexer(indexer);
                    }
                } catch (IndexerException ie) {
                    log.error("Error", ie);
                }
            }
        }
    }

    /**
     * Prida uzol do node indexu. 1. Zozbiera property pre fulltext pole
     * 
     * @throws IndexerException
     */
    public void addToNodeIndex(Node node, NodeIndexer indexer) throws IndexerException {
        // 1. fulltext
        // Neo4j implementuje pridanie dokumentu do Lucene fulltext indexu tak, ze okrem nami pozadovanej property prida aj presnu (exact) property ktora ma
        // rovnaky nazov a suffix "_e". Pozri org.neo4j.index.impl.lucene.IndexType - CustomType metoda addToDocument. Tymto sa zdvojuje obsah idexu.
        StringBuffer text = new StringBuffer();
        if (indexer.getPropertiesFullText() != null) {
            for (String p : indexer.getPropertiesFullText()) {
                Object property = node.getProperty(p, null);
                if (property != null) {
                    if (property instanceof Object[]) {
                        for (Object propertyValue : (Object[]) property) {
                            text.append(StringUtil.normalizeAndLowerCase(String.valueOf(propertyValue)));
                            text.append(" ");
                        }
                    } else {
                        text.append(StringUtil.normalizeAndLowerCase(String.valueOf(property)));
                        text.append(" ");
                    }
                }
            }
            String s = text.toString().trim();
            if (s.length() > 0) {
                getNodeIndex().add(node, NODE_FULLTEXT_KEY, s);
            }
        }
    }

    /**
     * Odstrani uzol z indexu uzlov.
     * 
     * @throws IndexerException
     */
    public void removeFromNodeIndex(Node node) throws IndexerException {
        getNodeIndex().remove(node);
    }

    public NodeIndexer defaulNodeIndexer() {
        NodeIndexer indexer = new NodeIndexer();
        indexer.setStatus(Status.ENABLED);
        indexer.setReindexStatus(ReindexStatus.REINDEX_SUCCESS);
        indexer.setPropertiesFullText(new LinkedHashSet<>());
        return indexer;
    }

    /**
     * Od neo4j 3 je tento pocet zizkavany na zaklade statistik transakcii databazy. Tym padom je pocet vsetkych uzlov je vrateny okamzite a je mozne takuto
     * query bezne pouzivat.
     */
    private long countAllNodes() {
        long count = 0;
        try (Transaction tx = graphDb.beginTx()) {
            Result result = graphDb.execute("MATCH (n) RETURN COUNT(*) as total");
            while (result.hasNext()) {
                Map<String, Object> row = result.next();
                for (Entry<String, Object> column : row.entrySet()) {
                    Object value = column.getValue();
                    if (value instanceof Integer) {
                        count = (Integer) value;
                    } else {
                        count = (Long) value;
                    }
                }
            }
        }
        return count;
    }
}
