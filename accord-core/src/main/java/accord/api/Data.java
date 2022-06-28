package accord.api;

/**
 * The result of some (potentially partial) {@link Read} from some {@link DataStore}
 */
public interface Data
{
    /**
     * Combine the contents of the parameter with this object and return the resultant object.
     * This method may modify the current object and return itself.
     */
    Data merge(Data data);
}
