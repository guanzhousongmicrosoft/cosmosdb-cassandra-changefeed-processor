package com.microsoft.azure.cosmosdb.cassandra.util;

import com.datastax.oss.driver.api.core.CqlSession;
import com.typesafe.config.Config;

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManagerFactory;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.security.*;
import java.security.cert.CertificateException;

/**
 * Cassandra utility class to handle the Cassandra Sessions
 */
public class CassandraAPIUtil implements AutoCloseable {

    private CqlSession session;
    private Config config;
    private String cassandraHost = "127.0.0.1";
    private int cassandraPort = 10350;
    private String cassandraUsername = "localhost";
    private String region = "UK South";
    private String cassandraPassword = "defaultpassword";
    private File sslKeyStoreFile = null;
    private String sslKeyStorePassword = "changeit";


    public CassandraAPIUtil(Config config) {
        this.config = config;
    }

    /**
     * This method creates a Cassandra Session based on the the end-point details given in config.properties.
     * This method validates the SSL certificate based on ssl_keystore_file_path & ssl_keystore_password properties.
     * If ssl_keystore_file_path & ssl_keystore_password are not given then it uses 'cacerts' from JDK.
     * @return Session Cassandra Session
     */
    public CqlSession getSession()
            throws KeyStoreException, NoSuchAlgorithmException, CertificateException, IOException,
            UnrecoverableKeyException, KeyManagementException {

        // Load cassandra endpoint details from config.properties
        try {
            loadCassandraConnectionDetails();
        } catch (Exception e) {
            e.printStackTrace();
        }
        final KeyStore keyStore = KeyStore.getInstance("JKS");
        try (final InputStream is = new FileInputStream(sslKeyStoreFile)) {
            keyStore.load(is, sslKeyStorePassword.toCharArray());
        }

        final KeyManagerFactory kmf = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
        kmf.init(keyStore, sslKeyStorePassword.toCharArray());
        final TrustManagerFactory tmf = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
        tmf.init(keyStore);

        // Creates a socket factory for HttpsURLConnection using JKS contents.
        final SSLContext sc = SSLContext.getInstance("TLSv1.2");
        sc.init(kmf.getKeyManagers(), tmf.getTrustManagers(), new SecureRandom());

        this.session = CqlSession.builder()
                .withSslContext(sc)
                .addContactPoint(new InetSocketAddress(cassandraHost, cassandraPort))
                .withAuthCredentials(cassandraUsername, cassandraPassword)
                .withLocalDatacenter(region)
                .build();

        System.out.println("Creating session: " + session.getName());
        return session;
    }

    /**
     * Closes the Cassandra session
     */
    public void close() {
        session.close();
    }

    /**
     * Loads Cassandra end-point details from config.properties.
     * 
     * @throws Exception
     */
    private void loadCassandraConnectionDetails() throws Exception {
        cassandraPort = config.getInt("cassandra.port");
        cassandraUsername = config.getString("cassandra.username");
        cassandraHost = config.getString("cassandra.host");
        region = config.getString("cassandra.region");
        cassandraPassword = config.getString("cassandra.password");
        String keyStorePath = config.getString("ssl.keystore.file");
        String keyStorePass = config.getString("ssl.keystore.password");

        // If ssl_keystore_file_path, build the path using JAVA_HOME directory.
        if (keyStorePath == null || keyStorePath.isEmpty()) {
            String javaHomeDirectory = System.getenv("JAVA_HOME");
            if (javaHomeDirectory == null || javaHomeDirectory.isEmpty()) {
                throw new Exception("JAVA_HOME not set");
            }
            keyStorePath = new StringBuilder(javaHomeDirectory).append("/jre/lib/security/cacerts")
                    .toString();
        }
        sslKeyStorePassword = (keyStorePass != null && !keyStorePass.isEmpty())
                ? keyStorePass
                : sslKeyStorePassword;

        sslKeyStoreFile = new File(keyStorePath);

        if (!sslKeyStoreFile.exists() || !sslKeyStoreFile.canRead()) {
            throw new Exception(
                    String.format("Unable to access the SSL Key Store file from %s", keyStorePath));
        }
    }
}
