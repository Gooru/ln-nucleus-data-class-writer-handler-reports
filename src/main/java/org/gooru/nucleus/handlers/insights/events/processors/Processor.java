package org.gooru.nucleus.handlers.insights.events.processors;

import org.gooru.nucleus.handlers.insights.events.processors.responses.MessageResponse;

public interface Processor {

  MessageResponse process();
}
