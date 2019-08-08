package sk.demtec.neo4j.plugins.indexer;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.kernel.extension.ExtensionType;
import org.neo4j.kernel.extension.KernelExtensionFactory;
import org.neo4j.kernel.impl.spi.KernelContext;
import org.neo4j.kernel.lifecycle.Lifecycle;

public class IndexerKernelExtensionFactory extends KernelExtensionFactory<IndexerKernelExtensionFactory.Dependencies> {
    public interface Dependencies {
        GraphDatabaseService getGraphDatabaseService();
    }

    public IndexerKernelExtensionFactory() {
        super(ExtensionType.DATABASE, "indexer");
    }

    @Override
    public Lifecycle newInstance(KernelContext context, Dependencies dependencies) {
        return new IndexerLifecycle(dependencies.getGraphDatabaseService());
    }
}
