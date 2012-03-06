package com.aconex.scrutineer;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

import org.apache.commons.lang.builder.CompareToBuilder;

public class IdAndVersion implements Comparable<IdAndVersion> {

    private final String id;
    private final String version;

    public IdAndVersion(String id, String version) {
        this.id = id;
        this.version = version;
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof IdAndVersion)) {
            return false;
        }
        IdAndVersion other = (IdAndVersion) obj;
        return this.compareTo(other) == 0;
    }

    @Override
    public int hashCode() {
        return toString().hashCode();
    }

    @Override
    public String toString() {
        return id + ":" + version;
    }

    public int compareTo(IdAndVersion other) {
        return new CompareToBuilder().append(id, other.id).append(version, other.version).toComparison();
    }

    public String getId() {
        return id;
    }

    public String getVersion() {
        return version;
    }

    public void writeToStream(ObjectOutputStream objectOutputStream) throws IOException {
        objectOutputStream.writeUTF(id);
        objectOutputStream.writeUTF(version);
    }

    public static IdAndVersion readFromStream(ObjectInputStream inputStream) throws IOException {
        return new IdAndVersion(inputStream.readUTF(), inputStream.readUTF());
    }
}
