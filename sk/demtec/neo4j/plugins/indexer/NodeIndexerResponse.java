package sk.demtec.neo4j.plugins.indexer;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import javax.xml.bind.annotation.XmlRootElement;

import org.codehaus.jackson.JsonGenerationException;
import org.codehaus.jackson.map.JsonMappingException;
import org.neo4j.server.rest.repr.MapRepresentation;

import sk.demtec.neo4j.plugins.indexer.svc.NodeIndexer;

@XmlRootElement
public class NodeIndexerResponse extends MapRepresentation {
    public static int CODE_1000_OK = 1000;
    public static int CODE_1001_OK_CONFIGURE = 1001;
    public static int CODE_1002_OK_DISABLE = 1002;
    public static int CODE_1003_OK_ENABLE = 1003;
    public static int CODE_1004_OK_INTERRUPT = 1004;
    public static int CODE_1005_OK_START = 1005;
    public static int CODE_1006_OK_STATUS = 1006;
    public static int CODE_1007_OK_CONFIGURE_REINDEX_REQUIRED = 1007;
    public static int CODE_2001_REINDEX_RUNNING = 2001;
    public static int CODE_2002_REINDEX_NOT_RUNNING = 2002;
    public static int CODE_2003_INDEXER_NOT_CONFIGURED = 2003;
    public static int CODE_2004_INDEXER_DISABLED = 2004;
    public static int CODE_2005_INDEXER_NOT_DISABLED = 2005;
    public static int CODE_2005_BAD_REQUEST_MISSING_FIELD = 2006;

    @SuppressWarnings("rawtypes")
    public NodeIndexerResponse(Map value) {
        super(value);
    }

    public static NodeIndexerResponse getInstance(int code, String message) {
        Map<String, Object> map = new HashMap<String, Object>();
        map.put("code", code);
        map.put("message", message);
        return new NodeIndexerResponse(map);
    }

    public static NodeIndexerResponse getInstance(int code, String message, NodeIndexer indexer)
            throws JsonGenerationException, JsonMappingException, IOException {
        Map<String, Object> map = new HashMap<String, Object>();
        map.put("code", code);
        map.put("message", message);
        Map<String, Object> bean = new HashMap<>();
        Map<String, Object> cfg = new HashMap<>();
        cfg.put(NodeIndexer.PROPERTIES_FULLTEXT, indexer.getPropertiesFullText());
        bean.put("cfg", cfg);
        if (indexer.getStatus() != null) {
            bean.put(NodeIndexer.STATUS, indexer.getStatus().name());
        }
        if (indexer.getReindexStatus() != null) {
            bean.put(NodeIndexer.REINDEX_STATUS, indexer.getReindexStatus().name());
        }
        if (indexer.getLastRunStartDate() != null) {
            bean.put(NodeIndexer.LAST_RUN_START_DATE, indexer.getLastRunStartDate().getTime());
        }
        if (indexer.getLastRunEndDate() != null) {
            bean.put(NodeIndexer.LAST_RUN_END_DATE, indexer.getLastRunEndDate().getTime());
        }
        if (indexer.getLastRunIndexItemsTotal() != null) {
            bean.put(NodeIndexer.LAST_RUN_INDEX_ITEMS_TOTAL, indexer.getLastRunIndexItemsTotal().longValue());
        }
        if (indexer.getLastRunIndexItemsTotal() != null) {
            bean.put(NodeIndexer.LAST_RUN_INDEX_ITEMS_CURRENT, indexer.getLastRunIndexItemsCurrent().longValue());
        }
        map.put("bean", bean);
        return new NodeIndexerResponse(map);
    }

    public static NodeIndexerResponse getInstance(int code, String message, Map<String, Object> bean) {
        Map<String, Object> map = new HashMap<String, Object>();
        map.put("code", code);
        map.put("message", message);
        map.put("bean", bean);
        return new NodeIndexerResponse(map);
    }
}
