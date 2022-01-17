package accord.messages;

/**
 * meant to assist implementations map accord messages to their own messaging systems
 */
public enum MessageType
{
    PREACCEPT_REQ,
    PREACCEPT_RSP,
    ACCEPT_REQ,
    ACCEPT_RSP,
    COMMIT_REQ,
    APPLY_REQ,
    APPLY_RSP,
    APPLY_AND_CHECK_REQ,
    APPLY_AND_CHECK_RSP,
    READ_REQ,
    READ_RSP,
    RECOVER_REQ,
    RECOVER_RSP,
    WAIT_ON_COMMIT_REQ,
    WAIT_ON_COMMIT_RSP,
    INFORM_REQ,
    INFORM_RSP,
    INFORM_PERSISTED_REQ,
    INFORM_PERSISTED_RSP,
    CHECK_STATUS_REQ,
    CHECK_STATUS_RSP
}
