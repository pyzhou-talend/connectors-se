/*
 * Copyright (C) 2006-2019 Talend Inc. - www.talend.com
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */
package org.talend.component.common.service.http;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.charset.UnsupportedCharsetException;
import java.security.MessageDigest;
import java.security.Principal;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.Formatter;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.StringTokenizer;

/**
 * Digest authentication scheme as defined in RFC 2617.
 * Both MD5 (default) and MD5-sess are supported.
 * Currently only qop=auth or no qop is supported. qop=auth-int
 * is unsupported. If auth and auth-int are provided, auth is
 * used.
 * <p>
 * Since the digest username is included as clear text in the generated
 * Authentication header, the charset of the username must be compatible
 * with the HTTP element charset used by the connection.
 * </p>
 *
 * @since 4.0
 */
public class DigestScheme {

    /**
     * Hexa values used when creating 32 character long digest in HTTP DigestScheme
     * in case of authentication.
     *
     * @see #formatHex(byte[])
     */

    private static final char[] HEXADECIMAL = { '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'a', 'b', 'c', 'd', 'e', 'f' };

    private static final int QOP_UNKNOWN = -1;

    private static final int QOP_MISSING = 0;

    private static final int QOP_AUTH_INT = 1;

    private static final int QOP_AUTH = 2;

    private final Map<String, String> paramMap;

    private boolean complete;

    // private transient ByteArrayBuilder buffer;
    private transient ByteArrayBuilder buffer;

    private String lastNonce;

    private long nounceCount;

    private String cnonce;

    private byte[] a1;

    private byte[] a2;

    private String username;

    private char[] password;

    public DigestScheme() {
        this.paramMap = new HashMap<>();
        this.complete = false;
    }

    /*
     * public void initPreemptive(final String username, final String password, final String cnonce, final String realm) {
     * this.username = username;
     * this.password = password.toCharArray();
     * this.paramMap.put("cnonce", cnonce);
     * this.paramMap.put("realm", realm);
     * }
     */

    public String getName() {
        return "digest";
    }

    public boolean isConnectionBased() {
        return false;
    }

    public String getRealm() {
        return this.paramMap.get("realm");
    }

    /*
     * public void processChallenge(
     * final AuthChallenge authChallenge,
     * final HttpContext context) throws MalformedChallengeException {
     * this.paramMap.clear();
     * final List<NameValuePair> params = authChallenge.getParams();
     * if (params != null) {
     * for (final NameValuePair param : params) {
     * this.paramMap.put(param.getName().toLowerCase(Locale.ROOT), param.getValue());
     * }
     * }
     * if (this.paramMap.isEmpty()) {
     * throw new MalformedChallengeException("Missing digest auth parameters");
     * }
     * this.complete = true;
     * }
     */

    public boolean isChallengeComplete() {
        final String s = this.paramMap.get("stale");
        return !"true".equalsIgnoreCase(s) && this.complete;
    }

    /*
     * public boolean isResponseReady(
     * final HttpHost host,
     * final CredentialsProvider credentialsProvider,
     * final HttpContext context) throws AuthenticationException {
     *
     * Args.notNull(host, "Auth host");
     * Args.notNull(credentialsProvider, "CredentialsProvider");
     *
     * final Credentials credentials = credentialsProvider.getCredentials(
     * new AuthScope(host, getRealm(), getName()), context);
     * if (credentials != null) {
     * this.username = credentials.getUserPrincipal().getName();
     * this.password = credentials.getPassword();
     * return true;
     * }
     * this.username = null;
     * this.password = null;
     * return false;
     * }
     */

    public Principal getPrincipal() {
        return null;
    }

    /*
     * public String generateAuthResponse(
     * final HttpHost host,
     * final HttpRequest request,
     * final HttpContext context) throws AuthenticationException {
     *
     * if (this.paramMap.get("realm") == null) {
     * throw new AuthenticationException("missing realm");
     * }
     * if (this.paramMap.get("nonce") == null) {
     * throw new AuthenticationException("missing nonce");
     * }
     * return createDigestResponse(request);
     * }
     */

    private static MessageDigest createMessageDigest(final String digAlg) throws UnsupportedDigestAlgorithmException {
        try {
            return MessageDigest.getInstance(digAlg);
        } catch (final Exception e) {
            throw new UnsupportedDigestAlgorithmException("Unsupported algorithm in HTTP Digest authentication: " + digAlg);
        }
    }

    public String createDigestResponse(final String username, final String password, final BasicHeader authChallenge,
            final DigestAuthContext context) throws AuthenticationException {
        Map<String, NameValuePair> pairs = BasicHeaderValueParser.parseParametersAsMap(authChallenge,
                new BasicHeaderValueParser());

        pairs.entrySet().forEach(k -> this.paramMap.put(k.getKey(), k.getValue().getValue()));

        final String uri = context.getUri();
        final String method = context.getMethod();
        final String realm = Optional.ofNullable(pairs.get("realm").getValue())
                .orElseThrow(() -> new AuthenticationException("No realm value in digest authentication challenge."));
        final String nonce = Optional.ofNullable(pairs.get("nonce").getValue())
                .orElse("No nonce value in digest authentication challenge.");
        final String opaque = Optional.ofNullable(pairs.get("opaque").getValue())
                .orElse("No opaque value in digest authentication challenge.");
        String algorithm = Optional.ofNullable(pairs.get("algorithm").getValue())
                .orElse("No algorithm value in digest authentication challenge.");
        // If an algorithm is not specified, default to MD5.
        if (algorithm == null) {
            algorithm = "MD5";
        }

        final Set<String> qopset = new HashSet<>(8);
        int qop = QOP_UNKNOWN;
        final String qoplist = this.paramMap.get("qop");
        if (qoplist != null) {
            final StringTokenizer tok = new StringTokenizer(qoplist, ",");
            while (tok.hasMoreTokens()) {
                final String variant = tok.nextToken().trim();
                qopset.add(variant.toLowerCase(Locale.ROOT));
            }
            if (context.hasPayload() && qopset.contains("auth-int")) {
                qop = QOP_AUTH_INT;
            } else if (qopset.contains("auth")) {
                qop = QOP_AUTH;
            } else if (qopset.contains("auth-int")) {
                qop = QOP_AUTH_INT;
            }
        } else {
            qop = QOP_MISSING;
        }

        if (qop == QOP_UNKNOWN) {
            throw new AuthenticationException("None of the qop methods is supported: " + qoplist);
        }

        final String charsetName = this.paramMap.get("charset");
        Charset charset;
        try {
            charset = charsetName != null ? Charset.forName(charsetName) : StandardCharsets.ISO_8859_1;
        } catch (final UnsupportedCharsetException ex) {
            charset = StandardCharsets.ISO_8859_1;
        }

        String digAlg = algorithm;
        if (digAlg.equalsIgnoreCase("MD5-sess")) {
            digAlg = "MD5";
        }

        final MessageDigest digester;
        try {
            digester = createMessageDigest(digAlg);
        } catch (final UnsupportedDigestAlgorithmException ex) {
            throw new AuthenticationException("Unsuppported digest algorithm: " + digAlg);
        }

        if (nonce.equals(this.lastNonce)) {
            nounceCount++;
        } else {
            nounceCount = 1;
            cnonce = null;
            lastNonce = nonce;
        }

        final StringBuilder sb = new StringBuilder(8);
        try (final Formatter formatter = new Formatter(sb, Locale.US)) {
            formatter.format("%08x", nounceCount);
        }
        final String nc = sb.toString();

        if (cnonce == null) {
            cnonce = formatHex(createCnonce());
        }

        if (buffer == null) {
            buffer = new ByteArrayBuilder(128);
        } else {
            buffer.reset();
        }
        buffer.charset(charset);

        a1 = null;
        a2 = null;
        // 3.2.2.2: Calculating digest
        if (algorithm.equalsIgnoreCase("MD5-sess")) {
            // H( unq(username-value) ":" unq(realm-value) ":" passwd )
            // ":" unq(nonce-value)
            // ":" unq(cnonce-value)

            // calculated one per session
            buffer.append(username).append(":").append(realm).append(":").append(password);
            final String checksum = formatHex(digester.digest(this.buffer.toByteArray()));
            buffer.reset();
            buffer.append(checksum).append(":").append(nonce).append(":").append(cnonce);
            a1 = buffer.toByteArray();
        } else {
            // unq(username-value) ":" unq(realm-value) ":" passwd
            buffer.append(username).append(":").append(realm).append(":").append(password);
            a1 = buffer.toByteArray();
        }

        final String hasha1 = formatHex(digester.digest(a1));
        buffer.reset();

        if (qop == QOP_AUTH) {
            // Method ":" digest-uri-value
            a2 = buffer.append(method).append(":").append(uri).toByteArray();
        } else if (qop == QOP_AUTH_INT) {
            // Method ":" digest-uri-value ":" H(entity-body)
            /*
             * final HttpEntity entity = request instanceof ClassicHttpRequest ? ((ClassicHttpRequest) request).getEntity() :
             * null;
             * if (entity != null && !entity.isRepeatable()) {
             * // If the entity is not repeatable, try falling back onto QOP_AUTH
             * if (qopset.contains("auth")) {
             * qop = QOP_AUTH;
             * a2 = buffer.append(method).append(":").append(uri).toByteArray();
             * } else {
             * throw new AuthenticationException("Qop auth-int cannot be used with " +
             * "a non-repeatable entity");
             * }
             * } else {
             */
            final HttpEntityDigester entityDigester = new HttpEntityDigester(digester);
            try {
                if (context.hasPayload()) {
                    writeTo(context.getPayload(), entityDigester);
                }
                entityDigester.close();
            } catch (final IOException ex) {
                throw new AuthenticationException("I/O error reading entity content", ex);
            }
            a2 = buffer.append(method).append(":").append(uri).append(":").append(formatHex(entityDigester.getDigest()))
                    .toByteArray();
            // }
        } else {
            a2 = buffer.append(method).append(":").append(uri).toByteArray();
        }

        final String hasha2 = formatHex(digester.digest(a2));
        buffer.reset();

        // 3.2.2.1

        final byte[] digestInput;
        if (qop == QOP_MISSING) {
            buffer.append(hasha1).append(":").append(nonce).append(":").append(hasha2);
            digestInput = buffer.toByteArray();
        } else {
            buffer.append(hasha1).append(":").append(nonce).append(":").append(nc).append(":").append(cnonce).append(":")
                    .append(qop == QOP_AUTH_INT ? "auth-int" : "auth").append(":").append(hasha2);
            digestInput = buffer.toByteArray();
        }
        buffer.reset();

        final String digest = formatHex(digester.digest(digestInput));

        final CharArrayBuffer buffer = new CharArrayBuffer(128);
        buffer.append("Digest ");

        final List<BasicNameValuePair> params = new ArrayList<>(20);
        params.add(new BasicNameValuePair("username", username));
        params.add(new BasicNameValuePair("realm", realm));
        params.add(new BasicNameValuePair("nonce", nonce));
        params.add(new BasicNameValuePair("uri", uri));
        params.add(new BasicNameValuePair("response", digest));

        if (qop != QOP_MISSING) {
            params.add(new BasicNameValuePair("qop", qop == QOP_AUTH_INT ? "auth-int" : "auth"));
            params.add(new BasicNameValuePair("nc", nc));
            params.add(new BasicNameValuePair("cnonce", cnonce));
        }
        // algorithm cannot be null here
        params.add(new BasicNameValuePair("algorithm", algorithm));
        if (opaque != null) {
            params.add(new BasicNameValuePair("opaque", opaque));
        }

        for (int i = 0; i < params.size(); i++) {
            final BasicNameValuePair param = params.get(i);
            if (i > 0) {
                buffer.append(", ");
            }
            final String name = param.getName();
            final boolean noQuotes = ("nc".equals(name) || "qop".equals(name) || "algorithm".equals(name));
            BasicHeaderValueFormatter.INSTANCE.formatNameValuePair(buffer, param, !noQuotes);
        }
        return buffer.toString();
    }

    private String getNonce() {
        return lastNonce;
    }

    private long getNounceCount() {
        return nounceCount;
    }

    private String getCnonce() {
        return cnonce;
    }

    private String getA1() {
        return a1 != null ? new String(a1, StandardCharsets.US_ASCII) : null;
    }

    private String getA2() {
        return a2 != null ? new String(a2, StandardCharsets.US_ASCII) : null;
    }

    /**
     * Encodes the 128 bit (16 bytes) MD5 digest into a 32 characters long
     * <CODE>String</CODE> according to RFC 2617.
     *
     * @param binaryData array containing the digest
     * @return encoded MD5, or <CODE>null</CODE> if encoding failed
     */
    static String formatHex(final byte[] binaryData) {
        final int n = binaryData.length;
        final char[] buffer = new char[n * 2];
        for (int i = 0; i < n; i++) {
            final int low = (binaryData[i] & 0x0f);
            final int high = ((binaryData[i] & 0xf0) >> 4);
            buffer[i * 2] = HEXADECIMAL[high];
            buffer[(i * 2) + 1] = HEXADECIMAL[low];
        }

        return new String(buffer);
    }

    private void writeTo(final byte[] payload, final OutputStream outstream) throws IOException {
        if (outstream == null) {
            throw new IllegalArgumentException("Output stream may not be null");
        }
        InputStream instream = new ByteArrayInputStream(payload);
        try {
            int l;
            byte[] tmp = new byte[2048];
            while ((l = instream.read(tmp)) != -1) {
                outstream.write(tmp, 0, l);
            }
        } finally {
            instream.close();
        }
    }

    /**
     * Creates a random cnonce value based on the current time.
     *
     * @return The cnonce value as String.
     */
    static byte[] createCnonce() {
        final SecureRandom rnd = new SecureRandom();
        final byte[] tmp = new byte[8];
        rnd.nextBytes(tmp);
        return tmp;
    }

    @Override
    public String toString() {
        return getName() + this.paramMap.toString();
    }

    public static class MalformedChallengeException extends Exception {

        public MalformedChallengeException(final String msg) {
            super(msg);
        }

        public MalformedChallengeException(final String msg, final Exception e) {
            super(msg, e);
        }
    }

    public static class AuthenticationException extends Exception {

        public AuthenticationException(final String msg) {
            super(msg);
        }

        public AuthenticationException(final String msg, final Exception e) {
            super(msg, e);
        }
    }

    public static class UnsupportedDigestAlgorithmException extends Exception {

        public UnsupportedDigestAlgorithmException(final String msg) {
            super(msg);
        }

        public UnsupportedDigestAlgorithmException(final String msg, final Exception e) {
            super(msg, e);
        }
    }

}
