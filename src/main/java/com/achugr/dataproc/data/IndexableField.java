package com.achugr.dataproc.data;

//TODO need to maintain consistency with ES mapping file
public enum IndexableField {
    SUBJECT("subject"),
    BODY("body"),
    PARTICIPANTS("participants");

    private final String name;

    IndexableField(String name) {
        this.name = name;
    }

    public String getName() {
        return name;
    }
}
