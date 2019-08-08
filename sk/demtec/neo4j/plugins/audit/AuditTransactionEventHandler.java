package sk.demtec.neo4j.plugins.audit;

import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.event.LabelEntry;
import org.neo4j.graphdb.event.PropertyEntry;
import org.neo4j.graphdb.event.TransactionData;
import org.neo4j.graphdb.event.TransactionEventHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AuditTransactionEventHandler implements TransactionEventHandler<Void> {
    private static final Logger log = LoggerFactory.getLogger(AuditTransactionEventHandler.class);

    public AuditTransactionEventHandler() {
    }

    @Override
    // prichadzaju sem iba finalne data pred komitom, takze ak sa napr. v jednej transakcii vytvori uzol a zaroven sa odstrani tak potom dany uzol nie je v
    // zozname pridanych a ani v zozname odobranych pretoze realne sa nic nevykonalo
    // BL je v beforeCommit pretoze z node potrebujeme nacitat labels a to je mozne vykonat iba v transakcii a ta uz v afterCommit nie je pritomna
    // z toho dovadu ale odchytavame vsetky chyby a iba logujeme a chybu nehadzeme a teda data sa uspesne komitnu ak je chyba v audit logu.
    public Void beforeCommit(TransactionData data) throws Exception {
        if (log.isInfoEnabled() == true) {
            try {
                boolean toLog = false; // ci je potrebne nieco zapisat do logu, necheme zapisovat prazdny JSON
                StringBuffer buffer = new StringBuffer();
                buffer.append("AUDIT DB ");
                buffer.append("{");
                buffer.append("\"nodes\":{"); // start nodes
                buffer.append("\"created\":[");
                int i = 0;
                for (Node node : data.createdNodes()) {
                    if (isNodeAuditable(node)) {
                        i++;
                        toLog = true;
                        if (i > 1) {
                            buffer.append(",");
                        }
                        buffer.append(node.getId());
                    }
                }
                buffer.append("]");
                buffer.append(",\"deleted\":[");
                i = 0;
                for (Node node : data.deletedNodes()) {
                    // nevieme pouzit isNodeAuditable(node) pretoze uzol je uz zmazany
                    i++;
                    toLog = true;
                    if (i > 1) {
                        buffer.append(",");
                    }
                    buffer.append(node.getId());
                }
                buffer.append("]");
                buffer.append(",\"property_changes\":[");
                i = 0;
                for (PropertyEntry<Node> property : data.assignedNodeProperties()) {
                    Node node = property.entity();
                    if (isNodeAuditable(node)) {
                        i++;
                        toLog = true;
                        if (i > 1) {
                            buffer.append(",");
                        }
                        buffer.append("{");
                        buffer.append("\"node\":" + node.getId() + ",");
                        buffer.append("\"key\":\"" + property.key() + "\",");
                        buffer.append("\"new_value\":" + formatPropertyValue(property.value()) + ",");
                        buffer.append("\"old_value\":" + formatPropertyValue(property.previouslyCommitedValue()));
                        buffer.append("}");
                    }
                }
                for (PropertyEntry<Node> property : data.removedNodeProperties()) {
                    Node node = property.entity();
                    if (data.isDeleted(node) == false) {
                        if (isNodeAuditable(node)) {
                            i++;
                            toLog = true;
                            if (i > 1) {
                                buffer.append(",");
                            }
                            buffer.append("{");
                            buffer.append("\"node\":" + node.getId() + ",");
                            buffer.append("\"key\":\"" + property.key() + "\",");
                            buffer.append("\"new_value\":null,");
                            buffer.append("\"old_value\":" + formatPropertyValue(property.previouslyCommitedValue()));
                            buffer.append("}");
                        }
                    }
                }
                buffer.append("]");
                buffer.append(",\"assigned_labels\":[");
                i = 0;
                for (LabelEntry label : data.assignedLabels()) {
                    Node node = label.node();
                    if (isNodeAuditable(node)) {
                        i++;
                        toLog = true;
                        if (i > 1) {
                            buffer.append(",");
                        }
                        buffer.append("{");
                        buffer.append("\"node\":" + node.getId() + ",");
                        buffer.append("\"label\":\"" + label.label() + "\"");
                        buffer.append("}");
                    }
                }
                buffer.append("]");
                buffer.append(",\"removed_labels\":[");
                i = 0;
                for (LabelEntry label : data.removedLabels()) {
                    Node node = label.node();
                    if (data.isDeleted(node) == false) {
                        if (isNodeAuditable(node)) {
                            i++;
                            toLog = true;
                            if (i > 1) {
                                buffer.append(",");
                            }
                            buffer.append("{");
                            buffer.append("\"node\":" + node.getId() + ",");
                            buffer.append("\"label\":\"" + label.label() + "\"");
                            buffer.append("}");
                        }
                    }
                }
                buffer.append("]");
                buffer.append("}"); // end nodes
                buffer.append(",\"relationships\":{"); // start rels
                buffer.append("\"created\":[");
                i = 0;
                for (Relationship rel : data.createdRelationships()) {
                    if (isRelAuditable(rel)) {
                        i++;
                        toLog = true;
                        if (i > 1) {
                            buffer.append(",");
                        }
                        buffer.append(rel.getId());
                    }
                }
                buffer.append("]");
                buffer.append(",\"deleted\":[");
                i = 0;
                for (Relationship rel : data.deletedRelationships()) {
                    // nevieme pouzit isRelAuditable(rel) pretoze rel je uz zmazane
                    i++;
                    toLog = true;
                    if (i > 1) {
                        buffer.append(",");
                    }
                    buffer.append(rel.getId());
                }
                buffer.append("]");
                buffer.append(",\"property_changes\":[");
                i = 0;
                for (PropertyEntry<Relationship> property : data.assignedRelationshipProperties()) {
                    Relationship rel = property.entity();
                    if (isRelAuditable(rel)) {
                        i++;
                        toLog = true;
                        if (i > 1) {
                            buffer.append(",");
                        }
                        buffer.append("{");
                        buffer.append("\"relationship\":" + rel.getId() + ",");
                        buffer.append("\"key\":\"" + property.key() + "\",");
                        buffer.append("\"new_value\":" + formatPropertyValue(property.value()) + ",");
                        buffer.append("\"old_value\":" + formatPropertyValue(property.previouslyCommitedValue()));
                        buffer.append("}");
                    }
                }
                for (PropertyEntry<Relationship> property : data.removedRelationshipProperties()) {
                    Relationship rel = property.entity();
                    if (data.isDeleted(property.entity()) == false) {
                        if (isRelAuditable(rel)) {
                            i++;
                            toLog = true;
                            if (i > 1) {
                                buffer.append(",");
                            }
                            buffer.append("{");
                            buffer.append("\"relationship\":" + rel.getId() + ",");
                            buffer.append("\"key\":\"" + property.key() + "\",");
                            buffer.append("\"new_value\":null,");
                            buffer.append("\"old_value\":" + formatPropertyValue(property.previouslyCommitedValue()));
                            buffer.append("}");
                        }
                    }
                }
                buffer.append("]");
                buffer.append("}"); // end rels
                buffer.append("}");
                if (toLog == true) {
                    log.info(buffer.toString());
                }
            } catch (Exception e) {
                log.error("Error", e);
            }
        }        
        return null;
    }

    @Override
    public void afterCommit(TransactionData data, Void state) {

    }

    private String formatPropertyValue(Object propertyValue) {
        if (propertyValue == null) {
            return null;
        } else if (propertyValue instanceof Long || propertyValue instanceof Integer) {
            return String.valueOf(propertyValue);
        } else {
            return "\"" + String.valueOf(propertyValue) + "\"";
        }
    }
    
    private boolean isNodeAuditable(Node node) {
        for (Label label : node.getLabels()) {
            if (label.name().startsWith("_")) {
                return false;
            }
        }
        return true;
    }
    
    private boolean isRelAuditable(Relationship rel) {
        return isNodeAuditable(rel.getStartNode()) && isNodeAuditable(rel.getEndNode());
    }

    @Override
    public void afterRollback(TransactionData data, Void state) {
    }
}
