package org.dandoy.dbpop;

public class FeatureFlags {
    /**
     * Binary data must be base64 encoded.
     * What's the best way to identify that data is encoded?
     * - Column is binary: I would prefer that meta information to be in the file
     * - Header: There could be a special character in the header to describe the encoding, for example, the header for staff.picture could be "picture*b64"
     * - Comment: rfc4180 does not define comments, but Common-csv supports them.
     */
    public static final boolean HANDLE_BINARY = false;
}
