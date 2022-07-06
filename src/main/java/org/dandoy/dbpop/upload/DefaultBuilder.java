package org.dandoy.dbpop.upload;

import lombok.extern.slf4j.Slf4j;
import org.dandoy.dbpop.cli.DatabaseOptions;
import org.dandoy.dbpop.database.ConnectionBuilder;
import org.dandoy.dbpop.database.UrlConnectionBuilder;
import org.dandoy.dbpop.utils.Env;

import java.io.File;
import java.io.IOException;

@Slf4j
public abstract class DefaultBuilder<SELF extends DefaultBuilder<?, ?>, T> {
    private Env env;
    private String dbUrl;
    private String dbUser;
    private String dbPassword;
    private File directory;
    private Boolean verbose;

    protected DefaultBuilder() {
        env = Env.createEnv();
    }

    DefaultBuilder(File directory) {
        env = Env.createEnv(directory);
    }

    public abstract T build();

    @SuppressWarnings("unchecked")
    private SELF self() {
        return (SELF) this;
    }

    public SELF setEnvironment(String environment) {
        env = env.environment(environment);
        return self();
    }

    public String getDbUrl() {
        return dbUrl == null ? env.getString("jdbcurl") : dbUrl;
    }

    public String getDbUser() {
        return dbUser == null ? env.getString("username") : dbUser;
    }

    public String getDbPassword() {
        return dbPassword == null ? env.getString("password") : dbPassword;
    }

    public boolean isVerbose() {
        return verbose!= null ? verbose : Boolean.parseBoolean(env.getString("verbose", "false"));
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


    public ConnectionBuilder getConnectionBuilder() {
        return new UrlConnectionBuilder(
                getDbUrl(),
                getDbUser(),
                getDbPassword()
        );
    }

    public File getDirectory() {
        if (directory != null) return directory;
        return findDirectory();
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
            if (directory != null) {
                this.directory = directory.getAbsoluteFile().getCanonicalFile();
                if (!this.directory.isDirectory()) throw new RuntimeException("Invalid dataset directory: " + this.directory);
            } else {
                this.directory = null;
            }
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

    /**
     * Tries to find the dataset directory starting from the current directory.
     */
    private static File findDirectory() {
        for (File dir = new File("."); dir != null; dir = dir.getParentFile()) {
            File datasetsDirectory = new File(dir, "src/test/resources/datasets");
            if (datasetsDirectory.isDirectory()) {
                try {
                    return datasetsDirectory.getAbsoluteFile().getCanonicalFile();
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
        }
        throw new RuntimeException("Datasets directory not set");
    }

    public SELF setConnection(DatabaseOptions databaseOptions) {
        dbUrl = databaseOptions.dbUrl;
        dbUser = databaseOptions.dbUser;
        dbPassword = databaseOptions.dbPassword;
        return self();
    }
}
