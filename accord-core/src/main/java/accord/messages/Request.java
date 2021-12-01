package accord.messages;

import accord.local.Node;
import accord.local.Node.Id;

public interface Request extends Message
{
    void process(Node on, Id from, ReplyContext replyContext);
}
