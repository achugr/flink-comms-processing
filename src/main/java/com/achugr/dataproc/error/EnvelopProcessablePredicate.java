package com.achugr.dataproc.error;

import com.achugr.dataproc.data.EventEnvelope;

import java.io.Serializable;
import java.util.function.Predicate;

public class EnvelopProcessablePredicate implements Predicate<EventEnvelope>, Serializable {

    @Override
    public boolean test(EventEnvelope eventEnvelope) {
        return eventEnvelope.isProcessable();
    }
}
