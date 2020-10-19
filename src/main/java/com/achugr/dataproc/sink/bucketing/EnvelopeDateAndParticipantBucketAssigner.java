package com.achugr.dataproc.sink.bucketing;

import com.achugr.dataproc.data.EnvelopeContext;
import com.achugr.dataproc.data.EventEnvelope;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.streaming.api.functions.sink.filesystem.BucketAssigner;
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.SimpleVersionedStringSerializer;

import javax.annotation.Nonnull;
import java.nio.file.Paths;
import java.time.DayOfWeek;
import java.time.LocalDate;

/**
 * This is an extension used by out of the box s3 sink, it defines that archive should be placed
 * into folder like /DATE/PARTICIPANT/
 */
public class EnvelopeDateAndParticipantBucketAssigner implements BucketAssigner<EventEnvelope, String> {

    @Override
    public String getBucketId(EventEnvelope element, Context context) {
        LocalDate date = element.getEventMetadata().getEventTime().toLocalDate().with(DayOfWeek.MONDAY);
        String datePart = convertToLegalForFileName(date.toString());
        String participantPart = convertToLegalForFileName(element.getContext().get(EnvelopeContext.PARTICIPANT));
        return Paths.get(datePart, participantPart).toString();
    }

    private String convertToLegalForFileName(@Nonnull String string) {
        return string.replaceAll("[^a-zA-Z0-9.-]", "_");
    }

    @Override
    public SimpleVersionedSerializer<String> getSerializer() {
        return SimpleVersionedStringSerializer.INSTANCE;
    }
}
