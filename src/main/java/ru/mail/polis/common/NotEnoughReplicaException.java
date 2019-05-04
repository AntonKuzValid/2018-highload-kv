package ru.mail.polis.common;

public class NotEnoughReplicaException extends RuntimeException {
    public NotEnoughReplicaException(String message) {
        super(message);
    }
}
