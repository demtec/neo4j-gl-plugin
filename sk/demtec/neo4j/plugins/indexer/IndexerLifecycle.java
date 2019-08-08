package sk.demtec.neo4j.plugins.indexer;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sk.demtec.neo4j.plugins.indexer.svc.IndexerException;
import sk.demtec.neo4j.plugins.indexer.svc.NodeIndexer;
import sk.demtec.neo4j.plugins.indexer.svc.NodeIndexer.ReindexStatus;
import sk.demtec.neo4j.plugins.indexer.svc.NodeIndexerService;

public class IndexerLifecycle extends LifecycleAdapter {
    private static final Logger log = LoggerFactory.getLogger(NodeIndexerService.class);
    private GraphDatabaseService graphDb;

    public IndexerLifecycle(GraphDatabaseService graphDb) {
        this.graphDb = graphDb;
    }

    /**
     * Pri spusteni pluginu (pri spusteni databazy. DB vsak este nie je available. Ak potrebujeme DB available je potrebne spravit Thread).
     */
    @Override
    public void start() {
        // registrujeme event handler
        graphDb.registerTransactionEventHandler(new IndexerTransactionEventHandler(graphDb));
        // initializujeme index - ak neexistuje bude vytvoreny. vykonavame vo vlakne aby sme pockali kym bude DB dostupna a nemozme blokovat start metodu
        // pretoze by DB nebola nikdy dostupna
        new Thread(new Runnable() {
            @Override
            public void run() {
                if (graphDb.isAvailable(5 * 60 * 1000)) {
                    log.info("Database is available");
                    try {
                        NodeIndexerService nodeIndexerService = new NodeIndexerService(graphDb);
                        NodeIndexer nodeIndexer = nodeIndexerService.getNodeIndexer();
                        if (nodeIndexer == null) {
                            // ak neexistuje konfiguracia potom vytvorime default
                            log.info("Node indexer not configured. Default configuration will be created.");
                            nodeIndexerService.saveNodeIndexer(nodeIndexerService.defaulNodeIndexer());
                        } else if (nodeIndexer.getReindexStatus() != null
                                && nodeIndexer.getReindexStatus().equals(ReindexStatus.REINDEX_RUNNING)) {
                            // pri spusteni DB je stav RUNNING cize pri stopnuti DB bezal reindex a nebol dokonceny co je nekonzistentny stav preto spustime
                            // reindex znova
                            log.info("Reindex status is '" + nodeIndexer.getReindexStatus().name()
                                    + "'. Going to interruprt reindex and start reindex");
                            nodeIndexerService.interruptReindexNodes();
                            nodeIndexerService.startReindexNodes();
                        }
                    } catch (IndexerException e) {
                        log.error("Error at init index", e);
                    }
                } else {
                    log.error("Database not available in 5 minutes");
                }
            }
        }).start();
    }

    /**
     * Pri stopnuti pluginu (stopnuti databazy. DB je uz uzavreta).
     */
    @Override
    public void stop() {
    }
}
