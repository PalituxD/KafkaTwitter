package org.palituxd.kafkapoc.model;

import lombok.Builder;

import java.io.Serializable;

@Builder
public class CustomObject implements Serializable {

    /**
     *
     */
    private static final long serialVersionUID = 1L;

    private String id;
    private String name;

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    @Override
    public String toString() {
        return "CustomObject{" +
                "id='" + id + '\'' +
                ", name='" + name + '\'' +
                '}';
    }
}