package accord.coordinate;

import accord.api.ConfigurationService;
import accord.api.Result;
import accord.local.Node;
import accord.txn.Ballot;
import accord.txn.Txn;
import accord.txn.TxnId;
import org.apache.cassandra.utils.concurrent.Future;

public class Coordinate
{
    private static Future<Result> fetchEpochOrExecute(Node node, Agreed agreed)
    {
        long executeEpoch = agreed.executeAt.epoch;
        ConfigurationService configService = node.configService();
        if (executeEpoch > configService.currentEpoch())
            return configService.fetchTopologyForEpoch(executeEpoch)
                                .flatMap(v -> fetchEpochOrExecute(node, agreed));

        return Execute.execute(node, agreed);
    }

    private static Future<Result> andThenExecute(Node node, Future<Agreed> agree)
    {
        return agree.flatMap(agreed -> fetchEpochOrExecute(node, agreed));
    }

    public static Future<Result> execute(Node node, TxnId txnId, Txn txn)
    {
        return andThenExecute(node, Agree.agree(node, txnId, txn));
    }

    public static Future<Result> recover(Node node, TxnId txnId, Txn txn)
    {
        return andThenExecute(node, new Recover(node, new Ballot(node.uniqueNow()), txnId, txn));
    }
}
