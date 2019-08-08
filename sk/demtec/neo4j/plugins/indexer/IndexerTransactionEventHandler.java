package sk.demtec.neo4j.plugins.indexer;

import java.util.HashSet;
import java.util.Set;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.event.PropertyEntry;
import org.neo4j.graphdb.event.TransactionData;
import org.neo4j.graphdb.event.TransactionEventHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sk.demtec.neo4j.plugins.indexer.svc.IndexerException;
import sk.demtec.neo4j.plugins.indexer.svc.NodeIndexer;
import sk.demtec.neo4j.plugins.indexer.svc.NodeIndexerService;

public class IndexerTransactionEventHandler implements TransactionEventHandler<Void> {
    private static final Logger log = LoggerFactory.getLogger(IndexerTransactionEventHandler.class);
    private GraphDatabaseService graphDb;

    public IndexerTransactionEventHandler(GraphDatabaseService graphDb) {
        this.graphDb = graphDb;
    }

    @Override
    // prichadzaju sem iba finalne data pred komitom, takze ak sa napr. v jednej transakcii vytvori uzol a zaroven sa odstrani tak potom dany uzol nie je v
    // zozname pridanych a ani v zozname odobranych pretoze realne sa nic nevykonalo
    public Void beforeCommit(TransactionData data) throws Exception {
        try {
            if (data == null) {
                return null;
            }
            // ak v tejto transakcii sa uklada reindex node tak potom nepokracujeme pretoze dalej ziskavame reindex node cez getNodeIndexer a napriek tomu,
            // ze ulozeny uzol nie je este komitnuty find uz ho najde kedze sme v tej istej tx a vznikne deadlock na metodach getNodeIndexer a
            // saveNodeIndexer
            if (NodeIndexerService.savingNodeReindex == true) {
                return null;
            }
            indexNodes(data);
        } catch (Exception e) {
            log.error("Error", e);
            throw e; // nechceme komitnut transakciu
        }
        return null;
    }

    private void indexNodes(TransactionData data) throws IndexerException {
        NodeIndexerService nodeSvc = new NodeIndexerService(graphDb);
        NodeIndexer nodeIndexer = nodeSvc.getNodeIndexer();
        if (nodeIndexer == null || nodeIndexer.getPropertiesFullText() == null || nodeIndexer.getPropertiesFullText().size() == 0) {
            return;
        }
        if (NodeIndexer.Status.DISABLED.equals(nodeIndexer.getStatus())) {
            return;
        }
        Set<Node> nodesToIndex = new HashSet<Node>();
        Set<Node> nodesToUnindex = new HashSet<Node>();
        Set<Node> nodesToReindex = new HashSet<Node>();
        // najprv ziskame pridane a odstranene pretoze budeme ich zoznam potrebovat pri ziskavani updatovanych
        for (Node node : data.createdNodes()) {
            nodesToIndex.add(node);
        }
        for (Node node : data.deletedNodes()) {
            nodesToUnindex.add(node);
        }
        // potom ziskame ktore su updatovane, ale pridame ich na updatovanie iba ak nie su v zozname na pridanie alebo odstranenie
        // pretoze ak je uzol zmazany objavi sa v data.deletedNodes ale aj property su v data.removedNodeProperties (podobne pre create)
        // navyse ak je uzol zmazany uz nemozme citat jeho properties
        if (data.assignedNodeProperties() != null) {
            for (PropertyEntry<Node> propertyEntry : data.assignedNodeProperties()) {
                Node node = propertyEntry.entity();
                // nezistujeme ci sa property naozaj zmenila alebo sa aktualizovala tou istou hodnotou aj keby sa to dalo. Je malo pravdepodobne ze
                // pride ako assigned value a bude mat tu istu hodnotu. Zda sa mi ze byt to viac spomalovalo ako pomahalo
                // najprv overime ci uz sme tento uzol nezahrnuli. je to aj ochrana proti tomu aby sme sa pytali na atributy uzla ktory je uz odstraneny co nie
                // je mozne
                if (!nodesToIndex.contains(node) && !nodesToUnindex.contains(node)) {
                    // a overime ci dana property je pre index zaujimava
                    if (nodeIndexer.getPropertiesFullText().contains(propertyEntry.key())) {
                        nodesToReindex.add(node);
                    }
                }
            }
        }
        if (data.removedNodeProperties() != null) {
            for (PropertyEntry<Node> propertyEntry : data.removedNodeProperties()) {
                Node node = propertyEntry.entity();
                // najprv overime ci uz sme tento uzaol nezahrnuli
                if (!nodesToIndex.contains(node) && !nodesToUnindex.contains(node)) {
                    // a overime ci dana property je pre index zaujimava
                    if (nodeIndexer.getPropertiesFullText().contains(propertyEntry.key())) {
                        nodesToReindex.add(node);
                    }
                }
            }
        }
        // operacie s indexom
        for (Node node : nodesToReindex) {
            // TODO [rda] - zvazit ci je efetivne odstranit cely uzol z indexu ak sa zmenila iba jedna property. Nie je to jasne pretoze ak sa meni
            // prilis vela property potom by odstranovanie z indexu pre konkretnu property mozno uz nebolo efektivne.
            nodeSvc.removeFromNodeIndex(node);
            nodeSvc.addToNodeIndex(node, nodeIndexer);
        }
        for (Node node : nodesToIndex) {
            nodeSvc.addToNodeIndex(node, nodeIndexer);
        }
        for (Node node : nodesToUnindex) {
            nodeSvc.removeFromNodeIndex(node);
        }
    }

    @Override
    public void afterCommit(TransactionData data, Void state) {
    }

    @Override
    public void afterRollback(TransactionData data, Void state) {
    }
}
