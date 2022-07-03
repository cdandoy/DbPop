package org.dandoy.dbpop;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Properties;

public abstract class DefaultBuilder<SELF extends DefaultBuilder<?, ?>, T> {
    private ConnectionBuilder connectionBuilder;
    private File directory;
    private boolean verbose;

    protected DefaultBuilder() {
    }

    public abstract T build();

    private SELF self() {
        //noinspection unchecked
        return (SELF) this;
    }

    public ConnectionBuilder getConnectionBuilder() {
        return connectionBuilder;
    }

    private SELF setConnectionBuilder(ConnectionBuilder connectionBuilder) {
        this.connectionBuilder = connectionBuilder;
        return self();
    }

    /**
     * How to connect to the database.
     *
     * @param dbUrl      The JDBC url
     * @param dbUser     The database username
     * @param dbPassword The database password
     * @return this
     */
    public SELF setConnection(String dbUrl, String dbUser, String dbPassword) {
        return setConnectionBuilder(new UrlConnectionBuilder(dbUrl, dbUser, dbPassword));
    }

    public File getDirectory() {
        return directory;
    }

    /**
     * For example:
     * <pre>
     * directory/
     *   base/
     *     AdventureWorks/
     *       HumanResources/
     *         Department.csv
     *         Employee.csv
     *         Shift.csv
     * </pre>
     *
     * @param directory The directory that holds the datasets
     * @return this
     */
    public SELF setDirectory(File directory) {
        try {
            if (directory == null) throw new RuntimeException("Directory cannot be null");
            this.directory = directory.getAbsoluteFile().getCanonicalFile();
            if (!this.directory.isDirectory()) throw new RuntimeException("Invalid dataset directory: " + this.directory);
            return self();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * @param path The directory that holds the datasets.
     * @return this
     * @see #setDirectory(File)
     */
    public SELF setDirectory(String path) {
        return setDirectory(new File(path));
    }

    public boolean isVerbose() {
        return verbose;
    }

    /**
     * Enables verbose logging on System.out
     *
     * @param verbose verbose on/off
     * @return this
     */
    public SELF setVerbose(boolean verbose) {
        this.verbose = verbose;
        return self();
    }

    protected void validate() {
        if (directory == null) findDirectory();
        if (directory == null) throw new RuntimeException("You must specify a dataset directory");
        if (!directory.isDirectory()) throw new RuntimeException("Invalid directory: " + directory);

        if (connectionBuilder == null) setupConnectionFromExternal();
        if (connectionBuilder == null) throw new RuntimeException("You must specify the database connection");
        try (Connection connection = connectionBuilder.createConnection()) {
            connection.getMetaData();
        } catch (SQLException e) {
            throw new RuntimeException("Invalid database connection " + connectionBuilder);
        }
    }

    /**
     * Tries to find the dataset directory starting from the current directory.
     */
    private void findDirectory() {
        for (File dir = new File("."); dir != null; dir = dir.getParentFile()) {
            File datasetsDirectory = new File(dir, "src/test/resources/datasets");
            if (datasetsDirectory.isDirectory()) {
                try {
                    this.directory = datasetsDirectory.getAbsoluteFile().getCanonicalFile();
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
        }
    }

    /**
     * Loads the properties from ~/dbpop.properties
     */
    private void setupConnectionFromExternal() {
        String userHome = System.getProperty("user.home");
        if (userHome == null) throw new RuntimeException("Cannot find your home directory");
        File propertyFile = new File(userHome, "dbpop.properties");
        if (propertyFile.exists()) {
            Properties properties = new Properties();
            try (BufferedReader bufferedReader = Files.newBufferedReader(propertyFile.toPath(), StandardCharsets.UTF_8)) {
                properties.load(bufferedReader);
                String jdbcurl = properties.getProperty("jdbcurl");
                String username = properties.getProperty("username");
                String password = properties.getProperty("password");
                if (jdbcurl == null) throw new RuntimeException("jdbcurl not set in " + propertyFile);
                if (username == null) throw new RuntimeException("username not set in " + propertyFile);
                if (password == null) throw new RuntimeException("password not set in " + propertyFile);

                setConnectionBuilder(new UrlConnectionBuilder(jdbcurl, username, password));

                setVerbose(Boolean.parseBoolean(properties.getProperty("verbose", "false")));
                if (this.verbose) {
                    System.out.println("Properties loaded from " + propertyFile);
                }
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        } else {
            throw new RuntimeException("Could not find connection properties");
        }
    }
}
