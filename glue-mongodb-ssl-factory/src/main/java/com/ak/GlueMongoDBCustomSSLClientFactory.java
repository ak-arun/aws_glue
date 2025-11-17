package com.ak;

import com.mongodb.MongoDriverInformation;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.MongoClientSettings;
import com.mongodb.spark.sql.connector.config.MongoConfig;
import com.mongodb.spark.sql.connector.connection.MongoClientFactory;

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.TrustManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;
import java.security.KeyStore;
import java.io.FileInputStream;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public final class GlueMongoDBCustomSSLClientFactory implements MongoClientFactory {
    private final MongoConfig config;
    private final MongoDriverInformation mongoDriverInformation;
    private final String customKeystorePath;
    private final String customKeystorePassword;

    public GlueMongoDBCustomSSLClientFactory(final MongoConfig config) {
        this.config = config;
        
        // Read custom keystore configuration from connection options
        this.customKeystorePath = config.getOrDefault("customKeystorePath", "/tmp/mongodb-keystore.jks");
        this.customKeystorePassword = config.getOrDefault("customKeystorePassword", "password");
        
        this.mongoDriverInformation = MongoDriverInformation.builder()
            .driverName("mongo-spark-connector|glue-custom-ssl")
            .driverVersion("10.2.0")
            .build();
    }

    @Override
    public MongoClient create() {
        try {
            SSLContext sslContext = createCombinedSSLContext();
            
            MongoClientSettings settings = MongoClientSettings.builder()
                .applyConnectionString(config.getConnectionString())
                .applyToSslSettings(builder -> 
                    builder.context(sslContext)
                           .enabled(true)
                           .invalidHostNameAllowed(true))
                .build();
                
            return MongoClients.create(settings, mongoDriverInformation);
        } catch (Exception e) {
            throw new RuntimeException("Failed to create MongoDB client with combined SSL: " + e.getMessage(), e);
        }
    }
    
    private SSLContext createCombinedSSLContext() throws Exception {
        // Load Glue's default truststore
        KeyStore glueTrustStore = KeyStore.getInstance("JKS");
        try (FileInputStream fis = new FileInputStream("/opt/amazon/certs/InternalAndExternalAndAWSTrustStore.jks")) {
            glueTrustStore.load(fis, "amazon".toCharArray());
        }
        
        // Load custom keystore for client certificates and additional trust
        KeyStore customKeyStore = KeyStore.getInstance("JKS");
        try (FileInputStream fis = new FileInputStream(customKeystorePath)) {
            customKeyStore.load(fis, customKeystorePassword.toCharArray());
        }
        
        // Create key manager from custom keystore (for client certificates)
        KeyManagerFactory kmf = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
        kmf.init(customKeyStore, customKeystorePassword.toCharArray());
        
        // Create trust managers from both keystores
        TrustManagerFactory glueTmf = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
        glueTmf.init(glueTrustStore);
        
        TrustManagerFactory customTmf = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
        customTmf.init(customKeyStore);
        
        // Combine trust managers
        TrustManager[] combinedTrustManagers = createCombinedTrustManagers(
            glueTmf.getTrustManagers(), 
            customTmf.getTrustManagers()
        );
        
        // Create and initialize SSL context
        SSLContext sslContext = SSLContext.getInstance("TLS");
        sslContext.init(kmf.getKeyManagers(), combinedTrustManagers, null);
        
        return sslContext;
    }
    
    private TrustManager[] createCombinedTrustManagers(TrustManager[] glueTrustManagers, 
                                                      TrustManager[] customTrustManagers) {
        List<X509TrustManager> x509TrustManagers = new ArrayList<>();
        
        // Collect X509TrustManagers from both sources
        for (TrustManager tm : glueTrustManagers) {
            if (tm instanceof X509TrustManager) {
                x509TrustManagers.add((X509TrustManager) tm);
            }
        }
        
        for (TrustManager tm : customTrustManagers) {
            if (tm instanceof X509TrustManager) {
                x509TrustManagers.add((X509TrustManager) tm);
            }
        }
        
        // Create composite trust manager
        X509TrustManager compositeTrustManager = new X509TrustManager() {
            @Override
            public void checkClientTrusted(X509Certificate[] chain, String authType) 
                    throws java.security.cert.CertificateException {
                // Try each trust manager until one succeeds
                java.security.cert.CertificateException lastException = null;
                for (X509TrustManager tm : x509TrustManagers) {
                    try {
                        tm.checkClientTrusted(chain, authType);
                        return; // Success
                    } catch (java.security.cert.CertificateException e) {
                        lastException = e;
                    }
                }
                throw lastException;
            }

            @Override
            public void checkServerTrusted(X509Certificate[] chain, String authType) 
                    throws java.security.cert.CertificateException {
                // Try each trust manager until one succeeds
                java.security.cert.CertificateException lastException = null;
                for (X509TrustManager tm : x509TrustManagers) {
                    try {
                        tm.checkServerTrusted(chain, authType);
                        return; // Success
                    } catch (java.security.cert.CertificateException e) {
                        lastException = e;
                    }
                }
                throw lastException;
            }

            @Override
            public X509Certificate[] getAcceptedIssuers() {
                List<X509Certificate> certificates = new ArrayList<>();
                for (X509TrustManager tm : x509TrustManagers) {
                    certificates.addAll(Arrays.asList(tm.getAcceptedIssuers()));
                }
                return certificates.toArray(new X509Certificate[0]);
            }
        };
        
        return new TrustManager[] { compositeTrustManager };
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        final GlueMongoDBCustomSSLClientFactory that = (GlueMongoDBCustomSSLClientFactory) o;
        return config.getConnectionString().equals(that.config.getConnectionString()) &&
               customKeystorePath.equals(that.customKeystorePath);
    }

    @Override
    public int hashCode() {
        return config.getConnectionString().hashCode() + customKeystorePath.hashCode();
    }
}
