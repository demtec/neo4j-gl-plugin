package sk.demtec.neo4j.plugins.indexer;

import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Properties;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.server.plugins.Description;
import org.neo4j.server.plugins.Name;
import org.neo4j.server.plugins.Parameter;
import org.neo4j.server.plugins.PluginTarget;
import org.neo4j.server.plugins.ServerPlugin;
import org.neo4j.server.plugins.Source;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sk.demtec.neo4j.plugins.PluginException;
import sk.demtec.neo4j.plugins.indexer.svc.NodeIndexer;
import sk.demtec.neo4j.plugins.indexer.svc.NodeIndexer.ReindexStatus;
import sk.demtec.neo4j.plugins.indexer.svc.NodeIndexer.Status;
import sk.demtec.neo4j.plugins.indexer.svc.NodeIndexerService;

public class NodeIndexerPlugin extends ServerPlugin {
    private static final Logger log = LoggerFactory.getLogger(NodeIndexerPlugin.class);

    /**
     * Zmenia sa vybrane konfiguracne parametre a vycisti sa cache v ktorej je konfiguracia ulozena aby sa pri najblizsom pouzivani nacitala nova aktualnu
     * konfiguracia.
     */
    @Name("configure")
    @Description("Configure index.")
    @PluginTarget(GraphDatabaseService.class)
    public NodeIndexerResponse configure(@Source GraphDatabaseService graphDb,
            @Parameter(name = "propertiesFullText", optional = false) String[] propertiesFullText) throws PluginException {
        try {
            NodeIndexerService svc = new NodeIndexerService(graphDb);
            NodeIndexer indexer = svc.getNodeIndexer();
            if (indexer == null) {
                // este neexistuje tak vytvorime prazdne dto
                indexer = new NodeIndexer();
            } else if (NodeIndexer.ReindexStatus.REINDEX_RUNNING.equals(indexer.getReindexStatus())) {
                // check if running
                return NodeIndexerResponse.getInstance(NodeIndexerResponse.CODE_2001_REINDEX_RUNNING, "Reindex is running");
            } else if (NodeIndexer.Status.DISABLED.equals(indexer.getStatus())) {
                // disabled
                return NodeIndexerResponse.getInstance(NodeIndexerResponse.CODE_2004_INDEXER_DISABLED, "Indexer is disabled");
            }
            LinkedHashSet<String> newPropertiesFullText = null;
            if (propertiesFullText != null) {
                newPropertiesFullText = new LinkedHashSet<>();
                for (int i = 0; i < propertiesFullText.length; i++) {
                    newPropertiesFullText.add(propertiesFullText[i]);
                }
            }            
            // configure
            // zistime ci sa konfiguracia zmenila
            boolean configurationChanged = configurationChanged(indexer, newPropertiesFullText);
            if (configurationChanged == true) {
                // ak nastala skutocna zmena konfiguracie potom je potrebny reindex
                indexer.setReindexStatus(ReindexStatus.REINDEX_REQUIRED);
            }
            indexer.setStatus(Status.ENABLED);
            indexer.setPropertiesFullText(newPropertiesFullText);
            svc.saveNodeIndexer(indexer);
            if (configurationChanged == true) {
                return NodeIndexerResponse.getInstance(NodeIndexerResponse.CODE_1007_OK_CONFIGURE_REINDEX_REQUIRED, "OK");
            } else {
                return NodeIndexerResponse.getInstance(NodeIndexerResponse.CODE_1001_OK_CONFIGURE, "OK");
            }
        } catch (Exception e) {
            log.error("Error", e);
            throw new PluginException("Error", e);
        }
    }

    /**
     * Ziska sa stav indexera.
     */
    @Name("status")
    @Description("Status of indexer.")
    @PluginTarget(GraphDatabaseService.class)
    public NodeIndexerResponse status(@Source GraphDatabaseService graphDb) throws PluginException {
        try {
            NodeIndexerService svc = new NodeIndexerService(graphDb);
            NodeIndexer indexer = svc.getNodeIndexer();
            if (indexer == null) {
                return NodeIndexerResponse.getInstance(NodeIndexerResponse.CODE_2003_INDEXER_NOT_CONFIGURED, "Indexer is not cofigured");
            }
            return NodeIndexerResponse.getInstance(NodeIndexerResponse.CODE_1006_OK_STATUS, "OK", indexer);
        } catch (Exception e) {
            log.error("Error", e);
            throw new PluginException("Error", e);
        }
    }

    /**
     * Spusti sa reindexacia vsetkych uzlov. Zmaze sa index (synchronized aby nikto neriesil nic s indexom vtedy). iteruje po 10 000 (aby nebola prilis velka
     * transakcia) a pridava uzly do indexu.
     */
    @Name("start")
    @Description("Start reindex of all nodes with specified label and specified properties. Use configure to chage label and properties.")
    @PluginTarget(GraphDatabaseService.class)
    public NodeIndexerResponse start(@Source GraphDatabaseService graphDb) throws PluginException {
        try {
            // check if running
            NodeIndexerService svc = new NodeIndexerService(graphDb);
            NodeIndexer indexer = svc.getNodeIndexer();
            if (indexer == null) {
                return NodeIndexerResponse.getInstance(NodeIndexerResponse.CODE_2003_INDEXER_NOT_CONFIGURED, "Indexer is not cofigured");
            }
            if (NodeIndexer.ReindexStatus.REINDEX_RUNNING.equals(indexer.getReindexStatus())) {
                return NodeIndexerResponse.getInstance(NodeIndexerResponse.CODE_2001_REINDEX_RUNNING, "Reindex is running");
            } else if (NodeIndexer.Status.DISABLED.equals(indexer.getStatus())) {
                return NodeIndexerResponse.getInstance(NodeIndexerResponse.CODE_2004_INDEXER_DISABLED, "Indexer is disabled");
            }
            svc.startReindexNodes();
            return NodeIndexerResponse.getInstance(NodeIndexerResponse.CODE_1005_OK_START, "OK");
        } catch (Exception e) {
            log.error("Error", e);
            throw new PluginException("Error", e);
        }
    }

    /**
     * Prerusi reindexaciu.
     */
    @Name("interrupt")
    @Description("Interrupt reindex process.")
    @PluginTarget(GraphDatabaseService.class)
    public NodeIndexerResponse interrupt(@Source GraphDatabaseService graphDb) throws PluginException {
        try {
            // check if running
            NodeIndexerService svc = new NodeIndexerService(graphDb);
            NodeIndexer indexer = svc.getNodeIndexer();
            if (indexer == null) {
                return NodeIndexerResponse.getInstance(NodeIndexerResponse.CODE_2003_INDEXER_NOT_CONFIGURED, "Indexer is not cofigured");
            }
            if (!NodeIndexer.ReindexStatus.REINDEX_RUNNING.equals(indexer.getReindexStatus())) {
                return NodeIndexerResponse.getInstance(NodeIndexerResponse.CODE_2002_REINDEX_NOT_RUNNING, "Reindex is not running");
            }
            if (NodeIndexer.Status.DISABLED.equals(indexer.getStatus())) {
                // disabeld
                return NodeIndexerResponse.getInstance(NodeIndexerResponse.CODE_2004_INDEXER_DISABLED, "Indexer is disabled");
            }
            svc.interruptReindexNodes();
            return NodeIndexerResponse.getInstance(NodeIndexerResponse.CODE_1004_OK_INTERRUPT, "OK");
        } catch (Exception e) {
            log.error("Error", e);
            throw new PluginException("Error", e);
        }
    }

    /**
     * Deaktivuje sa reindexer co znemena ze sa nevykonava automaticke indexovanie dat a nie je mozne vykonat ani reindex.
     */
    @Name("disable")
    @Description("Disable reindexer which means reindexer is not automaticly indexing data.")
    @PluginTarget(GraphDatabaseService.class)
    public NodeIndexerResponse disable(@Source GraphDatabaseService graphDb) throws PluginException {
        try {
            NodeIndexerService svc = new NodeIndexerService(graphDb);
            NodeIndexer indexer = svc.getNodeIndexer();
            if (indexer == null) {
                return NodeIndexerResponse.getInstance(NodeIndexerResponse.CODE_2003_INDEXER_NOT_CONFIGURED, "Indexer is not cofigured");
            }
            if (NodeIndexer.ReindexStatus.REINDEX_RUNNING.equals(indexer.getReindexStatus())) {
                return NodeIndexerResponse.getInstance(NodeIndexerResponse.CODE_2001_REINDEX_RUNNING, "Reindex is running");
            } else if (NodeIndexer.Status.DISABLED.equals(indexer.getStatus())) {
                return NodeIndexerResponse.getInstance(NodeIndexerResponse.CODE_2004_INDEXER_DISABLED, "Indexer is disabled");
            }
            svc.disable();
            return NodeIndexerResponse.getInstance(NodeIndexerResponse.CODE_1002_OK_DISABLE, "OK");
        } catch (Exception e) {
            log.error("Error", e);
            throw new PluginException("Error", e);
        }
    }

    /**
     * Aktivuje sa reindexer.
     */
    @Name("enable")
    @Description("Enable reindexer and starts reindexing.")
    @PluginTarget(GraphDatabaseService.class)
    public NodeIndexerResponse enable(@Source GraphDatabaseService graphDb) throws PluginException {
        try {
            NodeIndexerService svc = new NodeIndexerService(graphDb);
            NodeIndexer indexer = svc.getNodeIndexer();
            if (indexer == null) {
                return NodeIndexerResponse.getInstance(NodeIndexerResponse.CODE_2003_INDEXER_NOT_CONFIGURED, "Indexer is not cofigured");
            }
            if (!NodeIndexer.Status.DISABLED.equals(indexer.getStatus())) {
                return NodeIndexerResponse.getInstance(NodeIndexerResponse.CODE_2005_INDEXER_NOT_DISABLED, "Indexer is not disabled");
            }
            svc.enable();
            return NodeIndexerResponse.getInstance(NodeIndexerResponse.CODE_1003_OK_ENABLE, "OK");
        } catch (Exception e) {
            log.error("Error", e);
            throw new PluginException("Error", e);
        }
    }

    /**
     * Ak je potrebne vytvorit index skor ako sa vytvori napr. V EventHandleri alebo v sluzbe startReindex alebo query. Napr. ak aplikacia pouzivajuca neo4j DB
     * potrebuje vyhladavat v danom indexe cez web konzolu cez cypher, tak by nastala chyba ze index neexistuje. Ale ak sa ide cez indexer plugin tak nie je
     * potrebne inicializacia.
     */
    @Name("initIndex")
    @Description("Initialize nodes index.")
    @PluginTarget(GraphDatabaseService.class)
    public NodeIndexerResponse initIndex(@Source GraphDatabaseService graphDb) throws PluginException {
        try {
            NodeIndexerService svc = new NodeIndexerService(graphDb);
            svc.getNodeIndex();
            return NodeIndexerResponse.getInstance(NodeIndexerResponse.CODE_1000_OK, "OK");
        } catch (Exception e) {
            log.error("Error", e);
            throw new PluginException("Error", e);
        }
    }
    
    /**
     * Vrati informacie o pluginue.
     */
    @Name("pluginInfo")
    @Description("Return information about plugin.")
    @PluginTarget(GraphDatabaseService.class)
    public NodeIndexerResponse pluginInfo(@Source GraphDatabaseService graphDb) throws PluginException {
        try (InputStream inputStream = getClass().getClassLoader().getResourceAsStream("plugins.properties")) {
            Properties prop = new Properties();
            prop.load(inputStream);
            String version = prop.getProperty(getClass().getName() + ".version");
            Map<String, Object> bean = new HashMap<>();
            bean.put("version", version);
            return NodeIndexerResponse.getInstance(NodeIndexerResponse.CODE_1000_OK, "OK", bean);
        } catch (IOException e) {
            log.error("Error", e);
            throw new PluginException("Error", e);
        }
    }
    
    /**
     * Konfiguracia je zmenena ak sa nerovna existujuci a novy zoznam propertiesFullText.
     */
    private boolean configurationChanged(NodeIndexer existingIndexer, LinkedHashSet<String> newPropertiesFullText) {
        if (existingIndexer == null) {
            return true;
        }
        LinkedHashSet<String> existingPropertiesFullText = existingIndexer.getPropertiesFullText();
        if (isEmpty(existingPropertiesFullText) && isEmpty(newPropertiesFullText)) {
            return false;
        } else if (isNotEmpty(existingPropertiesFullText) && isEmpty(newPropertiesFullText)) {
            return true;
        } else if (isEmpty(existingPropertiesFullText) && isNotEmpty(newPropertiesFullText)) {
            return true;
        } else {
            // obidva su neprazdne. vzajomne porovnanie oboch zoznamov
            for (String p : newPropertiesFullText) {
                if (!existingPropertiesFullText.contains(p)) {
                    return true;
                }
            }
            for (String p : existingPropertiesFullText) {
                if (!newPropertiesFullText.contains(p)) {
                    return true;
                }
            }
            return false;
        }
    }
    
    private boolean isEmpty(LinkedHashSet<String> set) {
        if (set == null || set.size() == 0) {
            return true;
        }
        return false;
    }
    
    private boolean isNotEmpty(LinkedHashSet<String> set) {
        return !isEmpty(set);
    }
}
