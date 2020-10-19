package com.achugr.dataproc.parsing;

import com.achugr.dataproc.data.Email;
import com.achugr.dataproc.data.EventEnvelope;
import com.achugr.dataproc.data.EventMetadata;
import com.achugr.dataproc.data.ParticipantType;
import com.achugr.dataproc.data.ProcError;

import javax.mail.Address;
import javax.mail.internet.MimeMessage;
import java.io.InputStream;
import java.security.DigestInputStream;
import java.security.MessageDigest;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class EmailParsingService {
    public EventEnvelope parse(InputStream is, String fileName) {
        try (DigestInputStream digestInputStream = new DigestInputStream(is, MessageDigest.getInstance("SHA-1"))) {
            MimeMessage message = new MimeMessage(null, digestInputStream);
            Map<ParticipantType, List<String>> participants = new HashMap<>();
            List<ProcError> parsingErrors = new ArrayList<>();
            parseAddressSafe(message, ParticipantType.FROM, participants, parsingErrors);
            parseAddressSafe(message, ParticipantType.TO, participants, parsingErrors);

            EventEnvelope.Builder envelopeBuilder = EventEnvelope.Builder.envelop()
                    .withErrors(parsingErrors);

            EventMetadata meta = EventMetadata.Builder.event()
                    .withLocalDateTime(message.getSentDate().toInstant().atZone(ZoneId.systemDefault()).toLocalDateTime())
                    .withDigest(Base64.getEncoder().encodeToString(digestInputStream.getMessageDigest().digest()))
                    .withParticipant(participants)
                    .withOriginalFileName(fileName)
                    .build();

            Email.EmailBuilder emailBuilder = Email.builder()
                    .withMeta(meta);
            if (message.isMimeType("text/plain")) {
                emailBuilder.withBody(message.getContent().toString());
                emailBuilder.withSubject(message.getSubject());
            }
            envelopeBuilder.withEvent(emailBuilder.build());
            return envelopeBuilder.build();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    // checked exceptions from message field access...
    private void parseAddressSafe(MimeMessage message,
                                  ParticipantType participantType,
                                  Map<ParticipantType, List<String>> participants,
                                  List<ProcError> errors) {
        try {
            Address[] addresses = null;
            switch (participantType) {
                case FROM:
                    addresses = message.getFrom();
                    break;
                case TO:
                    addresses = message.getAllRecipients();
                    break;
            }
            if (addresses != null) {
                participants.put(participantType, Arrays.stream(addresses).map(Address::toString).distinct().collect(Collectors.toList()));
            }
        } catch (Exception e) {
            errors.add(new ProcError(e.toString(), ProcError.Type.NON_TERMINAL));
        }
    }

}
