package com.aconex.scrutineer.elasticsearch;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.initMocks;

import java.io.EOFException;
import java.io.IOException;
import java.io.ObjectInputStream;

import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;

import com.aconex.scrutineer.IdAndVersion;

public class IdAndVersionDataReaderTest {

    private static final String ID = "77";
    private static final String VERSION = "77";
    @Mock
    private ObjectInputStream objectInputStream;
    private IdAndVersionDataReader idAndVersionDataReader;


    @Before
    public void setup() {
        initMocks(this);
        idAndVersionDataReader = new IdAndVersionDataReader(objectInputStream);
    }

    @Test
    public void shouldGiveAndEstimateOfSize() {
        assertThat(idAndVersionDataReader.estimateSizeInBytes(new IdAndVersion(ID,VERSION)),is(ID.length()*2+8));
    }

    @Test
    public void shouldReadNextIdAndVersionObjectFromStream() throws IOException {
        when(objectInputStream.readUTF()).thenReturn(ID);
        when(objectInputStream.readUTF()).thenReturn(VERSION);
        IdAndVersion idAndVersion = idAndVersionDataReader.readNext();
        assertThat(idAndVersion.getId(), is(ID));
        assertThat(idAndVersion.getVersion(), is(VERSION));
    }

    @Test
    public void shouldReturnNullOnEndOfStream() throws IOException {
        when(objectInputStream.readUTF()).thenThrow(new EOFException());
        assertThat(idAndVersionDataReader.readNext(), is(nullValue()));
    }

    @Test
    public void shouldCloseStream() throws IOException {
        idAndVersionDataReader.close();
        verify(objectInputStream).close();
    }
}
