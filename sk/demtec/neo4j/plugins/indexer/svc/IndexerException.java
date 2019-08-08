package sk.demtec.neo4j.plugins.indexer.svc;

public class IndexerException extends Exception {
    private static final long serialVersionUID = -5663903796809390179L;

    public IndexerException() {
        super();
    }

    public IndexerException(String message, Throwable cause) {
        super(message, cause);
    }

    public IndexerException(String message) {
        super(message);
    }

    public IndexerException(Throwable cause) {
        super(cause);
    }
}
