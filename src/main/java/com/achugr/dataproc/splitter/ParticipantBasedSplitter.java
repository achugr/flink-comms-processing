package com.achugr.dataproc.splitter;

import com.achugr.dataproc.data.EnvelopeContext;
import com.achugr.dataproc.data.EventEnvelope;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;

import java.util.List;

/**
 * Participant based splitter is used to produce copies of an event by amount of participants,
 * this way we have a single copy of event 'representing' every participant for further grouping.
 */
public class ParticipantBasedSplitter implements FlatMapFunction<EventEnvelope, EventEnvelope> {
    @Override
    public void flatMap(EventEnvelope event, Collector<EventEnvelope> out) {
        event.getEventMetadata().getParticipants().values().stream()
                .flatMap(List::stream)
                .distinct()
                .forEach(participant -> {
                            EventEnvelope eventEnvelope = EventEnvelope.Builder.of(event)
                                    .withContext(EnvelopeContext.PARTICIPANT, participant)
                                    .build();
                            out.collect(eventEnvelope);
                        }
                );
    }
}
