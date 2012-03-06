package com.aconex.scrutineer;

import org.hamcrest.Description;
import org.hamcrest.Factory;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeMatcher;

public class HasIdAndVersionMatcher extends TypeSafeMatcher<IdAndVersion> {

    private final String id;

    private final String version;

    public HasIdAndVersionMatcher(String id, String version) {
        this.id = id;
        this.version = version;
    }

    @Override
    public boolean matchesSafely(IdAndVersion idAndVersion) {
        return idAndVersion.getId().endsWith(id) && idAndVersion.getVersion().equals(version);
    }

    @Override
    public void describeTo(Description description) {
        description.appendText(" has id and version "+id+":"+version);
    }

    @Factory
    public static <T>Matcher<IdAndVersion> hasIdAndVersion(String id, String version) {
        return new HasIdAndVersionMatcher(id,version);
    }
}
