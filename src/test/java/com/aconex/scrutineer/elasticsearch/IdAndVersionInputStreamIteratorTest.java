package com.aconex.scrutineer.elasticsearch;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Mockito.when;

import java.io.IOException;

import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import com.aconex.scrutineer.IdAndVersion;

public class IdAndVersionInputStreamIteratorTest {

    private static final String ID = "12";
    private static final String VERSION = "77";
    @Mock
    private IdAndVersionDataReader idAndVersionDataReader;

    @Before
    public void setup() throws IOException {
        MockitoAnnotations.initMocks(this);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void shouldThrowUnsupportedExceptionOnRemove() {
        IdAndVersionInputStreamIterator idAndVersionInputStreamIterator = new IdAndVersionInputStreamIterator(idAndVersionDataReader);
        idAndVersionInputStreamIterator.remove();
    }

    @Test
    public void hasNextShouldReturnFalseForAnEmptyStream() throws IOException {
        when(idAndVersionDataReader.readNext()).thenReturn(null);
        IdAndVersionInputStreamIterator idAndVersionInputStreamIterator = new IdAndVersionInputStreamIterator(idAndVersionDataReader);
        assertThat(idAndVersionInputStreamIterator.hasNext(), is(false));
    }

    @Test
    public void hasNextShouldReturnTrueForNonEmptyStream() throws IOException {
        when(idAndVersionDataReader.readNext()).thenReturn(new IdAndVersion(ID, VERSION));
        IdAndVersionInputStreamIterator idAndVersionInputStreamIterator = new IdAndVersionInputStreamIterator(idAndVersionDataReader);
        assertThat(idAndVersionInputStreamIterator.hasNext(), is(true));
    }

    @Test
    public void shouldGetTheNextItem() throws IOException {
        IdAndVersion idAndVersion = new IdAndVersion(ID, VERSION);
        when(idAndVersionDataReader.readNext()).thenReturn(idAndVersion);
        IdAndVersionInputStreamIterator idAndVersionInputStreamIterator = new IdAndVersionInputStreamIterator(idAndVersionDataReader);
        assertThat(idAndVersionInputStreamIterator.next(), is(idAndVersion));
    }

    @Test
    public void hasNextShouldReturnFalseAtEndOfTheStream() throws IOException {
        IdAndVersion idAndVersion = new IdAndVersion(ID, VERSION);
        when(idAndVersionDataReader.readNext()).thenReturn(idAndVersion).thenReturn(null);
        IdAndVersionInputStreamIterator idAndVersionInputStreamIterator = new IdAndVersionInputStreamIterator(idAndVersionDataReader);
        assertThat(idAndVersionInputStreamIterator.next(), is(idAndVersion));
        assertThat(idAndVersionInputStreamIterator.hasNext(), is(false));
    }

}
