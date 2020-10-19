package com.achugr.dataproc.data;

import com.google.common.collect.ImmutableMap;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

//TODO once add more event types it's necessary to introduce better hierarchy
public class Email implements Event {

    private final String subject;
    private final String body;
    private final EventMetadata meta;

    public Email(@Nullable String subject, @Nullable String body, EventMetadata meta) {
        this.subject = subject;
        this.body = body;
        this.meta = meta;
    }

    @Nonnull
    @Override
    public EventMetadata getMetadata() {
        return meta;
    }

    @Nullable
    @Override
    public String getSubject() {
        return subject;
    }

    @Nullable
    @Override
    public String getMessage() {
        return body;
    }

    @Nonnull
    @Override
    public Map<ParticipantType, List<String>> getParticipants() {
        return meta.getParticipants();
    }

    @Nonnull
    @Override
    public Map<IndexableField, Object> getIndexableFields() {
        return ImmutableMap.of(
                IndexableField.SUBJECT, getSubject(),
                IndexableField.BODY, getMessage(),
                IndexableField.PARTICIPANTS, getParticipants().values().stream().flatMap(List::stream).collect(Collectors.toSet())
        );
    }

    public static EmailBuilder builder(){
        return new EmailBuilder();
    }

    public static final class EmailBuilder {
        private String subject;
        private String body;
        private EventMetadata meta;

        private EmailBuilder() {
        }

        public EmailBuilder withSubject(String subject) {
            this.subject = subject;
            return this;
        }

        public EmailBuilder withBody(String body) {
            this.body = body;
            return this;
        }

        public EmailBuilder withMeta(EventMetadata eventMetadata) {
            this.meta = eventMetadata;
            return this;
        }

        public Email build() {
            return new Email(subject, body, meta);
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;

        if (o == null || getClass() != o.getClass()) return false;

        Email email = (Email) o;

        return new EqualsBuilder()
                .append(subject, email.subject)
                .append(body, email.body)
                .append(meta, email.meta)
                .isEquals();
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder(17, 37)
                .append(subject)
                .append(body)
                .append(meta)
                .toHashCode();
    }
}
