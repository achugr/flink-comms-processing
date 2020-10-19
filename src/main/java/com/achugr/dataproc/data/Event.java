package com.achugr.dataproc.data;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.List;
import java.util.Map;

/**
 * This represents event content. This is the information required
 * for analysis. This is not the event metadata, so it's not necessarily
 * that every single piece of information inside is eagerly fetched.
 * Subject might be fine to keep in the message payload, but for example
 * attachments of the event should be loaded lazily:
 * reference-based messaging https://www.slideshare.net/JiangjieQin/handle-large-messages-in-apache-kafka-58692297.
 * <p>
 * Balance between eagerly fetched and lazy contents should be found
 * according to the requirements, available resources and system limitations.
 */
public interface Event {
    @Nonnull
    EventMetadata getMetadata();

    @Nullable
    String getSubject();

    @Nullable
    String getMessage();

    @Nonnull
    Map<ParticipantType, List<String>> getParticipants();

    @Nonnull
    Map<IndexableField, Object> getIndexableFields();
}
