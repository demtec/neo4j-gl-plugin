package sk.demtec.neo4j.plugins.audit;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;

public class AuditLifecycle extends LifecycleAdapter {
    private GraphDatabaseService graphDb;

    public AuditLifecycle(GraphDatabaseService graphDb) {
        this.graphDb = graphDb;
    }

    @Override
    public void start() {
        // registrujeme event handler
        graphDb.registerTransactionEventHandler(new AuditTransactionEventHandler());
    }

    @Override
    public void stop() {
    }
}
