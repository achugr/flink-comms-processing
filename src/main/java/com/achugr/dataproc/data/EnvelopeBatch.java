package com.achugr.dataproc.data;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.List;

/**
 * This represents batch of events.
 * We group them by participant and date. If events amount is huge we could
 * become a message with huge payload, to avoid this we could collect just references to the messages,
 * or decrease a batch size, depending on the logic that uses the batch.
 */
public class EnvelopeBatch {
    private final List<EventEnvelope> events;
    private final EnvelopeContext context;

    public EnvelopeBatch(@Nonnull List<EventEnvelope> events, @Nullable EnvelopeContext context) {
        this.events = events;
        this.context = context;
    }

    @Nonnull
    public List<EventEnvelope> getEvents() {
        return events;
    }

    @Nonnull
    public EnvelopeContext getContext() {
        return context != null ? context : EnvelopeContext.empty();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;

        if (o == null || getClass() != o.getClass()) return false;

        EnvelopeBatch that = (EnvelopeBatch) o;

        return new EqualsBuilder()
                .append(events, that.events)
                .append(context, that.context)
                .isEquals();
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder(17, 37)
                .append(events)
                .append(context)
                .toHashCode();
    }
}
