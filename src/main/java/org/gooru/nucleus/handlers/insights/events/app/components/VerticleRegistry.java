package org.gooru.nucleus.handlers.insights.events.app.components;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * This is repository for verticles which needs to be deployed by
 * {@link DeployVerticle}
 *
 * @author Insights Team
 */
public class VerticleRegistry implements Iterable<String> {

  private static final String WRITER_VERTICLE = "org.gooru.nucleus.handlers.insights.events.bootstrap.InsightsWriteVerticle";
  private final Iterator<String> internalIterator;

  public VerticleRegistry() {
    List<String> initializers = new ArrayList<>();
    initializers.add(WRITER_VERTICLE);
    internalIterator = initializers.iterator();
  }

  @Override
  public Iterator<String> iterator() {
    return new Iterator<String>() {

      @Override
      public boolean hasNext() {
        return internalIterator.hasNext();
      }

      @Override
      public String next() {
        return internalIterator.next();
      }

    };
  }

}
