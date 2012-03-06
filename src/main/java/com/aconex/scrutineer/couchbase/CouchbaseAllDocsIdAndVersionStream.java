package com.aconex.scrutineer.couchbase;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Iterator;

import com.aconex.scrutineer.IdAndVersion;
import com.aconex.scrutineer.IdAndVersionStream;
import com.aconex.scrutineer.elasticsearch.IteratorFactory;

public class CouchbaseAllDocsIdAndVersionStream implements IdAndVersionStream {

    private static final String COUCHBASE_ALL_DOCS_FILE = "couchbase-all-docs.dat";

    private final CouchbaseAllDocsDownloader couchbaseDownloader;
    private final IteratorFactory iteratorFactory;
    private final File file;

    public CouchbaseAllDocsIdAndVersionStream(CouchbaseAllDocsDownloader couchbaseDownloader, IteratorFactory iteratorFactory, String workingDirectory) {
        this.couchbaseDownloader = couchbaseDownloader;
        this.iteratorFactory = iteratorFactory;
        file = new File(workingDirectory, COUCHBASE_ALL_DOCS_FILE);
    }

    @Override
    public void open() {
        couchbaseDownloader.downloadTo(createOutputStream());
    }

    OutputStream createOutputStream() {
        try {
            return new BufferedOutputStream(new FileOutputStream(file));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Iterator<IdAndVersion> iterator() {
        return iteratorFactory.forFile(file);
    }

    @Override
    public void close() {
        file.delete();
    }

}
