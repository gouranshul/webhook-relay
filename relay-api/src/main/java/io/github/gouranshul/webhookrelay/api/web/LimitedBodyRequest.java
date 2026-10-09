package io.github.gouranshul.webhookrelay.api.web;

import jakarta.servlet.ReadListener;
import jakarta.servlet.ServletInputStream;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletRequestWrapper;
import java.io.IOException;

/**
 * Caps how many body bytes can be read. Content-Length is checked up front, but a chunked request has none, and
 * without this cap Jackson would buffer whatever a client streams in.
 */
final class LimitedBodyRequest extends HttpServletRequestWrapper {

    /** Raised mid-read; mapped to 413 by {@link ApiExceptionHandler}. */
    static final class BodyTooLargeException extends IOException {
        BodyTooLargeException(long limit) {
            super("Request body exceeds " + limit + " bytes");
        }
    }

    private final long limit;
    private ServletInputStream stream;

    LimitedBodyRequest(HttpServletRequest request, long limit) {
        super(request);
        this.limit = limit;
    }

    @Override
    public ServletInputStream getInputStream() throws IOException {
        if (stream == null) {
            stream = new Counting(super.getInputStream(), limit);
        }
        return stream;
    }

    private static final class Counting extends ServletInputStream {
        private final ServletInputStream delegate;
        private final long limit;
        private long read;

        Counting(ServletInputStream delegate, long limit) {
            this.delegate = delegate;
            this.limit = limit;
        }

        @Override
        public int read() throws IOException {
            int b = delegate.read();
            if (b >= 0) {
                count(1);
            }
            return b;
        }

        @Override
        public int read(byte[] buffer, int offset, int length) throws IOException {
            int n = delegate.read(buffer, offset, length);
            if (n > 0) {
                count(n);
            }
            return n;
        }

        private void count(int n) throws BodyTooLargeException {
            read += n;
            if (read > limit) {
                throw new BodyTooLargeException(limit);
            }
        }

        @Override
        public boolean isFinished() {
            return delegate.isFinished();
        }

        @Override
        public boolean isReady() {
            return delegate.isReady();
        }

        @Override
        public void setReadListener(ReadListener listener) {
            delegate.setReadListener(listener);
        }
    }
}
