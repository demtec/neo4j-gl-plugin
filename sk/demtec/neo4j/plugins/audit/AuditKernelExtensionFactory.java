package sk.demtec.neo4j.plugins.audit;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.kernel.extension.ExtensionType;
import org.neo4j.kernel.extension.KernelExtensionFactory;
import org.neo4j.kernel.impl.spi.KernelContext;
import org.neo4j.kernel.lifecycle.Lifecycle;

public class AuditKernelExtensionFactory extends KernelExtensionFactory<AuditKernelExtensionFactory.Dependencies> {
    public interface Dependencies {
        GraphDatabaseService getGraphDatabaseService();
    }

    public AuditKernelExtensionFactory() {
        super(ExtensionType.DATABASE, "audit");
    }

    @Override
    public Lifecycle newInstance(KernelContext context, Dependencies dependencies) {
        return new AuditLifecycle(dependencies.getGraphDatabaseService());
    }
}
