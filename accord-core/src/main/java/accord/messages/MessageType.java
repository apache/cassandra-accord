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
    READ_REQ,
    READ_RSP,
    RECOVER_REQ,
    RECOVER_RSP,
    WAIT_ON_COMMIT_REQ,
    WAIT_ON_COMMIT_RSP
}
