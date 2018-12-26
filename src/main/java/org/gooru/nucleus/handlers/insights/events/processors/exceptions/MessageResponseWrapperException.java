package org.gooru.nucleus.handlers.insights.events.processors.exceptions;

import org.gooru.nucleus.handlers.insights.events.processors.responses.MessageResponse;

public class MessageResponseWrapperException extends RuntimeException {

    /**
     * 
     */
    private static final long serialVersionUID = 442853362633755587L;
    private final MessageResponse messageResponse;

    public MessageResponseWrapperException(MessageResponse messageResponse) {
        this.messageResponse = messageResponse;
    }
    

    public MessageResponse getMessageResponse() {
        return messageResponse;
    }
}
