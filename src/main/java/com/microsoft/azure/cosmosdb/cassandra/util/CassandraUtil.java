package com.microsoft.azure.cosmosdb.cassandra.util;

import com.datastax.driver.core.*;
import com.typesafe.config.Config;

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManagerFactory;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.security.*;
import java.security.cert.CertificateException;

/**
 * Cassandra utility class to handle the Cassandra Sessions
 */
public class CassandraUtil implements AutoCloseable {
    private Cluster cluster;
    private Session session;
    private final Config config;

    public CassandraUtil(Config config) {
        this.config = config;
    }

    public Session getSession()
            throws KeyStoreException, NoSuchAlgorithmException, CertificateException, IOException,
            UnrecoverableKeyException, KeyManagementException {

        if (session == null) {
            final KeyStore keyStore = loadKeyStore();

            final KeyManagerFactory kmf = KeyManagerFactory.getInstance(KeyManagerFactory
                    .getDefaultAlgorithm());
            kmf.init(keyStore, config.getString("ssl.keystore.password").toCharArray());
            final TrustManagerFactory tmf = TrustManagerFactory.getInstance(TrustManagerFactory
                    .getDefaultAlgorithm());
            tmf.init(keyStore);

            // Creates a socket factory for HttpsURLConnection using JKS contents.
            final SSLContext sc = SSLContext.getInstance("TLSv1.2");
            sc.init(kmf.getKeyManagers(), tmf.getTrustManagers(), new java.security.SecureRandom());

            SSLOptions sslOptions = RemoteEndpointAwareJdkSSLOptions.builder()
                    .withSSLContext(sc)
                    .build();

            SocketOptions options = new SocketOptions();
            options.setConnectTimeoutMillis(60000);
            options.setReadTimeoutMillis(60000);

            QueryOptions queryOptions = new QueryOptions().setConsistencyLevel(ConsistencyLevel.LOCAL_QUORUM);
            cluster = Cluster.builder()
                    .addContactPoints(config.getString("cassandra.host"))
                    .withQueryOptions(queryOptions)
                    .withPort(config.getInt("cassandra.port"))
                    .withCredentials(config.getString("cassandra.username"), config.getString("cassandra.password"))
                    .withSSL(sslOptions)
                    .withSocketOptions(options)
                    .build();
            session = cluster.connect();
        }
        return session;
    }

    /**
     * Closes the cluster and session
     */
    public void close() {
        if (session != null){
            session.close();
        }

        if (cluster != null) {
            cluster.close();
        }
    }

    private KeyStore loadKeyStore() throws KeyStoreException, IOException, CertificateException, NoSuchAlgorithmException {
        String keyStorePath = config.getString("ssl.keystore.file");
        String keyStorePass = config.getString("ssl.keystore.password");

        File keystoreFile = new File(keyStorePath);
        if (!keystoreFile.exists() || !keystoreFile.canRead()) {
            throw new RuntimeException("Unable to read keystore file " + keystoreFile);
        }

        final KeyStore keyStore = KeyStore.getInstance("JKS");
        try (final InputStream is = new FileInputStream(keystoreFile)) {
            keyStore.load(is, keyStorePass.toCharArray());
        }

        return keyStore;
    }
}