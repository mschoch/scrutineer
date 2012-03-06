package com.aconex.scrutineer;

import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

import java.io.PrintStream;

import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

public class PrintStreamOutputVersionStreamVerifierListenerTest {

    private final IdAndVersion idAndVersion = new IdAndVersion("1", "10");

    @Mock
    private PrintStream printStream;

    @Before
    public void setup(){
        MockitoAnnotations.initMocks(this);
    }

    @Test()
    public void testOnMissingInPrimaryStream() throws Exception {
        PrintStreamOutputVersionStreamVerifierListener streamVerifierListener = new PrintStreamOutputVersionStreamVerifierListener(printStream);
        streamVerifierListener.onMissingInPrimaryStream(idAndVersion);
        verify(printStream).println("NOTINPRIMARY\t1\t10");
        verifyNoMoreInteractions(printStream);
    }

    @Test
    public void testOnMissingInSecondaryStream() throws Exception {
        PrintStreamOutputVersionStreamVerifierListener streamVerifierListener = new PrintStreamOutputVersionStreamVerifierListener(printStream);
        streamVerifierListener.onMissingInSecondaryStream(idAndVersion);
        verify(printStream).println("NOTINSECONDARY\t1\t10");
        verifyNoMoreInteractions(printStream);
    }


    @Test
    public void testOnVersionMisMatch() throws Exception {
        PrintStreamOutputVersionStreamVerifierListener streamVerifierListener = new PrintStreamOutputVersionStreamVerifierListener(printStream);
        streamVerifierListener.onVersionMisMatch(idAndVersion, new IdAndVersion("1","9"));
        verify(printStream).println("MISMATCH\t1\t10\tsecondaryVersion=9");
        verifyNoMoreInteractions(printStream);
    }
}
