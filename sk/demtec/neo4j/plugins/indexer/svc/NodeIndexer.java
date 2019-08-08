package sk.demtec.neo4j.plugins.indexer.svc;

import java.util.Date;
import java.util.LinkedHashSet;

public class NodeIndexer {
    private LinkedHashSet<String> propertiesFullText; // zoznam properties ktore sa maju pridavat do fulltext pola
    private Status status;  // stav indexu
    private ReindexStatus reindexStatus; // stav reindexovania
    private Date lastRunStartDate; // zaciatok reindexovania
    private Date lastRunEndDate; // koniec reindexovania
    private Long lastRunIndexItemsTotal; // celkovy pocet poloziek na indexovanie
    private Long lastRunIndexItemsCurrent; // aktualny stav indexovania
    //// CONSTANTS
    public static final String PROPERTIES_FULLTEXT = "propertiesFullText";
    public static final String STATUS = "status";
    public static final String REINDEX_STATUS = "reindexStatus"; 
    public static final String LAST_RUN_START_DATE = "lastRunStartDate";
    public static final String LAST_RUN_END_DATE = "lastRunEndDate";
    public static final String LAST_RUN_INDEX_ITEMS_TOTAL = "lastRunIndexItemsTotal";
    public static final String LAST_RUN_INDEX_ITEMS_CURRENT = "lastRunIndexItemsCurrent";    

    public enum Status {
        ENABLED, DISABLED
    }

    public enum ReindexStatus {
        REINDEX_RUNNING, REINDEX_SUCCESS, REINDEX_ERROR, REINDEX_INTERRUPTED, REINDEX_REQUIRED
    }

    public LinkedHashSet<String> getPropertiesFullText() {
        return propertiesFullText;
    }

    public void setPropertiesFullText(LinkedHashSet<String> propertiesFullText) {
        this.propertiesFullText = propertiesFullText;
    }

    public Status getStatus() {
        return status;
    }

    public void setStatus(Status status) {
        this.status = status;
    }

    public ReindexStatus getReindexStatus() {
        return reindexStatus;
    }

    public void setReindexStatus(ReindexStatus reindexStatus) {
        this.reindexStatus = reindexStatus;
    }

    public Date getLastRunStartDate() {
        return lastRunStartDate;
    }

    public void setLastRunStartDate(Date lastRunStartDate) {
        this.lastRunStartDate = lastRunStartDate;
    }

    public Date getLastRunEndDate() {
        return lastRunEndDate;
    }

    public void setLastRunEndDate(Date lastRunEndDate) {
        this.lastRunEndDate = lastRunEndDate;
    }

    public Long getLastRunIndexItemsTotal() {
        return lastRunIndexItemsTotal;
    }

    public void setLastRunIndexItemsTotal(Long lastRunIndexItemsTotal) {
        this.lastRunIndexItemsTotal = lastRunIndexItemsTotal;
    }

    public Long getLastRunIndexItemsCurrent() {
        return lastRunIndexItemsCurrent;
    }

    public void setLastRunIndexItemsCurrent(Long lastRunIndexItemsCurrent) {
        this.lastRunIndexItemsCurrent = lastRunIndexItemsCurrent;
    }
}
