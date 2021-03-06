package com.aconex.scrutineer;

import java.util.Iterator;

import org.apache.log4j.Logger;

public class IdAndVersionStreamVerifier {

    private static final Logger LOG = LogUtils.loggerForThisClass();

    //CHECKSTYLE:OFF
    @SuppressWarnings("PMD.NcssMethodCount")
    public void verify(IdAndVersionStream primaryStream, IdAndVersionStream secondayStream, IdAndVersionStreamVerifierListener idAndVersionStreamVerifierListener) {
        long numItems = 0;
        long begin = System.currentTimeMillis();

        try {
            primaryStream.open();
            secondayStream.open();
            
            Iterator<IdAndVersion> primaryIterator = primaryStream.iterator();
            Iterator<IdAndVersion> secondaryIterator = secondayStream.iterator();

            IdAndVersion primaryItem =  next(primaryIterator);
            IdAndVersion secondaryItem = next(secondaryIterator);

            while (primaryItem != null && secondaryItem != null) {
                if (primaryItem.equals(secondaryItem)) {
                    primaryItem =  next(primaryIterator);
                    secondaryItem = next(secondaryIterator);
                }
                else if (primaryItem.getId().equals(secondaryItem.getId())) {
                    idAndVersionStreamVerifierListener.onVersionMisMatch(primaryItem, secondaryItem);
                    primaryItem = next(primaryIterator);
                    secondaryItem = next(secondaryIterator);
                }
                else if (primaryItem.compareTo(secondaryItem) < 0) {
                    idAndVersionStreamVerifierListener.onMissingInSecondaryStream(primaryItem);
                    primaryItem = next(primaryIterator);
                }
                else {
                    idAndVersionStreamVerifierListener.onMissingInPrimaryStream(secondaryItem);
                    secondaryItem = next(secondaryIterator);
                }
                numItems++;
            }

            while (primaryItem != null) {
                idAndVersionStreamVerifierListener.onMissingInSecondaryStream(primaryItem);
                primaryItem = next(primaryIterator);
                numItems++;
            }

            while (secondaryItem != null) {
                idAndVersionStreamVerifierListener.onMissingInPrimaryStream(secondaryItem);
                secondaryItem = next(secondaryIterator);
                numItems++;
            }
        }
        finally {
            closeWithoutThrowingException(primaryStream);
            closeWithoutThrowingException(secondayStream);
        }
        LogUtils.infoTimeTaken(LOG, begin, numItems, "Completed verification");
    }
    //CHECKSTYLE:ON

    private IdAndVersion next(Iterator<IdAndVersion> iterator) {
        if (iterator.hasNext()) {
            return iterator.next();
        }
        else {
            return null;
        }
    }

    private void closeWithoutThrowingException(IdAndVersionStream idAndVersionStream) {
        try {
            idAndVersionStream.close();
        }
        catch(Exception e) {
            LogUtils.warn(LOG,"Unable to close IdAndVersionStream",e);
        }
    }

}
