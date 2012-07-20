package com.aconex.scrutineer.couchbase;

import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.net.URL;
import java.net.URLConnection;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;
import org.codehaus.jackson.map.ObjectMapper;

import com.aconex.scrutineer.IdAndVersion;
import com.aconex.scrutineer.LogUtils;

public class CouchbaseAllDocsDownloader {

    private static final Logger LOG = LogUtils.loggerForThisClass();
    private static final int BATCH = 1001;

    private long numItems = 0;

    private String host;
    private String bucket;

    private ObjectMapper mapper = new ObjectMapper();

    public CouchbaseAllDocsDownloader(String host, String bucket) {
        this.host = host;
        this.bucket = bucket;
    }

    public void downloadTo(OutputStream outputStream) {
        long begin = System.currentTimeMillis();
        doDownloadTo(outputStream);
        LogUtils.infoTimeTaken(LOG, begin, numItems, "Download completed");
    }

    private void doDownloadTo(OutputStream outputStream) {
        int currentOffset = 0;
        ObjectOutputStream objectOutputStream = null;
        try {
            objectOutputStream = new ObjectOutputStream(outputStream);

            int numRows = -1;
            String startKeyDocid = null;
            do {
                String extra = "";
                if(startKeyDocid != null) {
                    extra = String.format("&startkey_docid=%s", startKeyDocid);
                }
                URL url = new URL(String.format("%s/%s/_all_docs?limit=%d%s", host, bucket, BATCH, extra));
                LogUtils.info(LOG, "Requesting %s", url.toExternalForm());
                URLConnection connection = url.openConnection();
                InputStream stream = connection.getInputStream();
                Map<String,Object> result = mapper.readValue(stream, Map.class);
                if(result != null) {
                    List<Map<String,Object>> rows = (List)result.get("rows");
                    if(rows != null) {
                        numRows = rows.size();
                        for (int i=0; i < numRows; i++) {
                            Map<String,Object> row = rows.get(i);
                            String key = (String)row.get("key");
                            if(i == BATCH-1) {
                                //treat extra row differently, just set startKeyDocid
                                startKeyDocid = key;
                            } else {
                                Map<String,Object> value = (Map<String,Object>)row.get("value");
                                if(value != null) {
                                    String rev = (String)value.get("rev");
                                    new IdAndVersion(key, rev).writeToStream(objectOutputStream);
                                    numItems++;
                                }
                            }
                        }
                    }
                }
                currentOffset += BATCH;
            } while(numRows == BATCH);

        } catch(IOException e) {
            throw new RuntimeException(e);
        } finally {
            if(objectOutputStream != null) {
                try {
                    objectOutputStream.close();
                } catch (IOException e) {
                    //ignore
                }
            }
        }
    }

}
