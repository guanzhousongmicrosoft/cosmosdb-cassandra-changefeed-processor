package com.microsoft.azure.cosmosdb.cassandra.util;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.metadata.Metadata;
import com.datastax.oss.driver.api.core.metadata.schema.KeyspaceMetadata;
import com.datastax.oss.driver.api.core.metadata.schema.TableMetadata;
import com.typesafe.config.Config;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Cassandra utility class to handle the Cassandra Sessions
 */
public class MICassandraUtil implements AutoCloseable{
    private static final Logger log = LoggerFactory.getLogger(MICassandraUtil.class.getName());

    private CqlSession session;
    private Config config;
    private String cassandraHost = "127.0.0.1";
    private int cassandraPort = 10350;
    private String cassandraUsername = "localhost";
    private String dataCenter = "datacenter-1";
    private String cassandraPassword = "defaultpassword";
    private File sslKeyStoreFile = null;
    private String sslKeyStorePassword = "changeit";

    public MICassandraUtil(Config config) {
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

        this.session = CqlSession.builder().withSslContext(sc)
                .addContactPoint(new InetSocketAddress(cassandraHost, cassandraPort)).withLocalDatacenter(dataCenter)
                .withAuthCredentials(cassandraUsername, cassandraPassword).build();

        log.info("Creating session: " + session.getName());
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
        cassandraPort = config.getInt("cassandra_MI.port");
        cassandraUsername = config.getString("cassandra_MI.username");
        cassandraHost = config.getString("cassandra_MI.host");
        dataCenter = config.getString("DC");
        cassandraPassword = config.getString("cassandra_MI.password");
        String ssl_keystore_file_path = config.getString("ssl.keystore.file");
        String ssl_keystore_password = config.getString("ssl.keystore.password");

        // If ssl_keystore_file_path, build the path using JAVA_HOME directory.
        if (ssl_keystore_file_path == null || ssl_keystore_file_path.isEmpty()) {
            String javaHomeDirectory = System.getenv("JAVA_HOME");
            if (javaHomeDirectory == null || javaHomeDirectory.isEmpty()) {
                throw new Exception("JAVA_HOME not set");
            }
            ssl_keystore_file_path = new StringBuilder(javaHomeDirectory).append("/jre/lib/security/cacerts")
                    .toString();
        }
        sslKeyStorePassword = (ssl_keystore_password != null && !ssl_keystore_password.isEmpty())
                ? ssl_keystore_password
                : sslKeyStorePassword;

        sslKeyStoreFile = new File(ssl_keystore_file_path);

        if (!sslKeyStoreFile.exists() || !sslKeyStoreFile.canRead()) {
            throw new Exception(
                    String.format("Unable to access the SSL Key Store file from %s", ssl_keystore_file_path));
        }
    }

    //Get all tables and try to create them in the MI
    public static void createKeyspaceAndTableIfNotExists(CqlSession session_api, CqlSession session_MI, String keyspace, String table){
        Metadata metadata = session_api.getMetadata();
        metadata.getKeyspace(keyspace).ifPresent(ks -> {
                    String keyspace_description = ks.describe(false);
                    log.info("Keyspace description: " + keyspace_description);
                    keyspace_description = keyspace_description.replace("CREATE KEYSPACE", "CREATE KEYSPACE IF NOT EXISTS");
                    session_MI.execute(keyspace_description);
                    log.info("Keyspace created: " + keyspace);});

        metadata.getKeyspace(keyspace).ifPresent(ks -> {
            ks.getTable(table).ifPresent(tableMetadata -> {
                if(tableMetadata.getName().asInternal().equals("change_feed_page_state")){
                    return;
                }

                String description = tableMetadata.describe(false);

                description = description.replace("CREATE TABLE", "CREATE TABLE IF NOT EXISTS");
                description = description.replaceAll("\\bAND\\s+dclocal_read_repair_chance\\s*=\\s*0\\.0\\b", "");
                description = description.replaceAll("\\bAND\\s+read_repair_chance\\s*=\\s*0\\.0\\b", "");
                log.info("Table description: " + description);

                session_MI.execute(description);
                log.info("Table created: " + table);
            });

        });
    }

    public static Set<TableIdentifier> getAllKeyspacesAndTables(CqlSession session_api){
        Set<TableIdentifier> res = new HashSet<>();
        Metadata metadata = session_api.getMetadata();
        Map<CqlIdentifier, KeyspaceMetadata> keyspaces = metadata.getKeyspaces();
        for (Map.Entry<CqlIdentifier, KeyspaceMetadata> entry : keyspaces.entrySet()) {
            log.info("Keyspace: " + entry.getKey());
            KeyspaceMetadata keyspaceMetadata = entry.getValue();
            Map<CqlIdentifier, TableMetadata> tables = keyspaceMetadata.getTables();
            for (Map.Entry<CqlIdentifier, TableMetadata> tableEntry : tables.entrySet()) {
                log.info("Table: " + tableEntry.getKey());
                res.add(new TableIdentifier(entry.getKey(), tableEntry.getKey()));
            }
        }

        return res;
    }
}
