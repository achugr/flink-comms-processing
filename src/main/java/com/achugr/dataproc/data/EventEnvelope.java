package com.achugr.dataproc.data;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * This is a container for an event - message representing event during the processing.
 */
public class EventEnvelope {
    private final EventSource eventSource;
    private final Event event;
    private final EnvelopeContext context;
    private final List<ProcError> errors;

    public EventEnvelope(EventSource eventSource, Event event) {
        this.eventSource = eventSource;
        this.event = event;
        this.context = null;
        this.errors = null;
    }

    public EventEnvelope(EventSource eventSource, Event event, EnvelopeContext context, List<ProcError> errors) {
        this.eventSource = eventSource;
        this.event = event;
        this.context = context;
        this.errors = errors;
    }

    @Nonnull
    public EventSource getEventSource() {
        return eventSource;
    }

    @Nonnull
    public Event getEvent() {
        return event;
    }

    @Nonnull
    public EnvelopeContext getContext() {
        return context != null ? context : EnvelopeContext.empty();
    }

    @Nullable
    public String getContext(String key) {
        return context != null ? context.get(key) : null;
    }

    public List<ProcError> getErrors() {
        return errors;
    }

    public EventMetadata getEventMetadata() {
        return event.getMetadata();
    }

    public boolean isProcessable() {
        return getErrors().stream().noneMatch(procError -> procError.getType() == ProcError.Type.TERMINAL);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;

        if (o == null || getClass() != o.getClass()) return false;

        EventEnvelope that = (EventEnvelope) o;

        return new EqualsBuilder()
                .append(eventSource, that.eventSource)
                .append(event, that.event)
                .append(context, that.context)
                .append(errors, that.errors)
                .isEquals();
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder(17, 37)
                .append(eventSource)
                .append(event)
                .append(context)
                .append(errors)
                .toHashCode();
    }

    public static final class Builder {
        private EventSource eventSource;
        private Map<String, String> context;
        private Event event;
        private List<ProcError> errors;

        private Builder() {
            this.context = new HashMap<>();
            this.errors = new ArrayList<>();
        }

        public static Builder envelop() {
            return new Builder();
        }

        public static Builder of(EventEnvelope eventEnvelope) {
            return envelop()
                    .withEventSource(eventEnvelope.getEventSource())
                    .withErrors(eventEnvelope.getErrors())
                    .withEvent(eventEnvelope.getEvent())
                    .withContext(eventEnvelope.getContext().getContext());
        }

        public Builder withEventSource(EventSource eventSource) {
            this.eventSource = eventSource;
            return this;
        }

        public Builder withContext(String key, String value) {
            this.context.put(key, value);
            return this;
        }

        public Builder withContext(Map<String, String> context) {
            this.context.putAll(context);
            return this;
        }

        public Builder withError(ProcError error) {
            this.errors.add(error);
            return this;
        }

        public Builder withErrors(List<ProcError> errors) {
            this.errors.addAll(errors);
            return this;
        }

        public Builder withEvent(Event content) {
            this.event = content;
            return this;
        }

        public EventEnvelope build() {
            return new EventEnvelope(eventSource, event, new EnvelopeContext(context), errors);
        }
    }
}
