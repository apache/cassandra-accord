package accord.messages;

/**
 * meant to assist implementations map accord messages to their own messaging systems
 */
public enum MessageType
{
    SIMPLE_RSP,
    PREACCEPT_REQ,
    PREACCEPT_RSP,
    ACCEPT_REQ,
    ACCEPT_INVALIDATE_REQ,
    ACCEPT_RSP,
    GET_DEPS_REQ,
    GET_DEPS_RSP,
    COMMIT_REQ,
    COMMIT_INVALIDATE,
    APPLY_REQ,
    APPLY_RSP,
    APPLY_AND_CHECK_REQ,
    APPLY_AND_CHECK_RSP,
    READ_REQ,
    READ_RSP,
    BEGIN_RECOVER_REQ,
    BEGIN_RECOVER_RSP,
    BEGIN_INVALIDATE_REQ,
    BEGIN_INVALIDATE_RSP,
    WAIT_ON_COMMIT_REQ,
    WAIT_ON_COMMIT_RSP,
    INFORM_TXNID_REQ,
    INFORM_HOME_DURABLE_REQ,
    INFORM_ROUTE_REQ,
    CHECK_STATUS_REQ,
    CHECK_STATUS_RSP
}
