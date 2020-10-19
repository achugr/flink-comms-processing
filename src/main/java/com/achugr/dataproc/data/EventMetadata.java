package com.achugr.dataproc.data;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;

import java.io.Serializable;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * This is a metadata of the event, we don't externalize it, so shouldn't put much here.
 */
public class EventMetadata implements Serializable {
    private final LocalDateTime eventTime;
    private final String digest;
    private final String originalName;
    private final Map<ParticipantType, List<String>> participants;

    public EventMetadata(LocalDateTime eventTime, String digest, String originalName, Map<ParticipantType, List<String>> participants) {
        this.eventTime = eventTime;
        this.digest = digest;
        this.originalName = originalName;
        this.participants = participants;
    }

    public LocalDateTime getEventTime() {
        return eventTime;
    }

    public String getDigest() {
        return digest;
    }

    public Map<ParticipantType, List<String>> getParticipants() {
        return participants;
    }

    public String getOriginalName() {
        return originalName;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;

        if (o == null || getClass() != o.getClass()) return false;

        EventMetadata eventMetadata = (EventMetadata) o;

        return new EqualsBuilder()
                .append(eventTime, eventMetadata.eventTime)
                .append(digest, eventMetadata.digest)
                .append(originalName, eventMetadata.originalName)
                .append(participants, eventMetadata.participants)
                .isEquals();
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder(17, 37)
                .append(eventTime)
                .append(digest)
                .append(originalName)
                .append(participants)
                .toHashCode();
    }

    public static final class Builder {
        private LocalDateTime localDateTime;
        private String digest;
        private String originalFileName;
        private Map<ParticipantType, List<String>> participants;

        private Builder() {
            participants = new HashMap<>();
        }

        public static Builder event() {
            return new Builder();
        }

        public static Builder of(EventMetadata eventMetadata) {
            return event()
                    .withLocalDateTime(eventMetadata.getEventTime())
                    .withDigest(eventMetadata.getDigest())
                    .withOriginalFileName(eventMetadata.getOriginalName())
                    .withParticipant(eventMetadata.getParticipants());
        }

        public Builder withLocalDateTime(LocalDateTime localDateTime) {
            this.localDateTime = localDateTime;
            return this;
        }

        public Builder withDigest(String digest) {
            this.digest = digest;
            return this;
        }

        public Builder withOriginalFileName(String originalFileName) {
            this.originalFileName = originalFileName;
            return this;
        }

        public Builder withParticipant(ParticipantType type, Set<String> persons) {
            this.participants.computeIfAbsent(type, k -> new ArrayList<>()).addAll(persons);
            return this;
        }

        public Builder withParticipant(Map<ParticipantType, List<String>> participant) {
            this.participants.putAll(participant);
            return this;
        }

        public EventMetadata build() {
            return new EventMetadata(localDateTime, digest, originalFileName, participants);
        }
    }
}
