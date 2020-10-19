package com.achugr.dataproc.data;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Collections;
import java.util.Map;

/**
 * Envelope context is used for the meta information of the envelope. It should contain information
 * evaluated on previous steps that is necessary on the next ones.
 */
public class EnvelopeContext {

    public static final String PARTICIPANT = "participant";

    private final Map<String, String> context;

    public EnvelopeContext(Map<String, String> context) {
        this.context = context;
    }

    public static EnvelopeContext empty() {
        return new EnvelopeContext(null);
    }

    @Nullable
    public String get(String key) {
        return context.get(key);
    }

    @Nonnull
    public Map<String, String> getContext() {
        return context != null ? context : Collections.emptyMap();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;

        if (o == null || getClass() != o.getClass()) return false;

        EnvelopeContext that = (EnvelopeContext) o;

        return new EqualsBuilder()
                .append(context, that.context)
                .isEquals();
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder(17, 37)
                .append(context)
                .toHashCode();
    }
}
