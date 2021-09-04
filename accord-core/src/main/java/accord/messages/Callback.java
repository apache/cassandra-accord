package accord.messages;

import accord.local.Node.Id;

public interface Callback<T>
{
    void onSuccess(Id from, T response);
    void onFailure(Id from, Throwable throwable);
}
