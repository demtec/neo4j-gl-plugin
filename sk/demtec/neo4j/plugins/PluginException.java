package sk.demtec.neo4j.plugins;

public class PluginException extends Exception {
    private static final long serialVersionUID = -5663903796809390179L;

    public PluginException() {
        super();
    }

    public PluginException(String message, Throwable cause) {
        super(message, cause);
    }

    public PluginException(String message) {
        super(message);
    }

    public PluginException(Throwable cause) {
        super(cause);
    }
}
