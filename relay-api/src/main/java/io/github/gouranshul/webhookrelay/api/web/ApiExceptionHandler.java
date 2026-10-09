package io.github.gouranshul.webhookrelay.api.web;

import io.github.gouranshul.webhookrelay.common.net.EndpointUrlPolicy.InvalidEndpointUrlException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.HttpStatusCode;
import org.springframework.http.ProblemDetail;
import org.springframework.http.ResponseEntity;
import org.springframework.http.converter.HttpMessageNotReadableException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;
import org.springframework.web.context.request.WebRequest;
import org.springframework.web.servlet.mvc.method.annotation.ResponseEntityExceptionHandler;

/** Maps domain errors to RFC 9457 problem details. Framework errors (validation, parsing) use the base class. */
@RestControllerAdvice
public class ApiExceptionHandler extends ResponseEntityExceptionHandler {

    private static final Logger log = LoggerFactory.getLogger(ApiExceptionHandler.class);

    @ExceptionHandler(ApiException.class)
    ProblemDetail handleApi(ApiException e) {
        return ProblemDetail.forStatusAndDetail(e.status(), e.getMessage());
    }

    @ExceptionHandler(InvalidEndpointUrlException.class)
    ProblemDetail handleUrl(InvalidEndpointUrlException e) {
        return ProblemDetail.forStatusAndDetail(HttpStatus.BAD_REQUEST, e.getMessage());
    }

    /** A body that exceeded the limit while streaming (no Content-Length) surfaces as an unreadable message. */
    @Override
    protected ResponseEntity<Object> handleHttpMessageNotReadable(HttpMessageNotReadableException ex, HttpHeaders headers,
            HttpStatusCode status, WebRequest request) {
        for (Throwable t = ex; t != null; t = t.getCause()) {
            if (t instanceof LimitedBodyRequest.BodyTooLargeException tooLarge) {
                return ResponseEntity.status(HttpStatus.CONTENT_TOO_LARGE)
                        .body(ProblemDetail.forStatusAndDetail(HttpStatus.CONTENT_TOO_LARGE, tooLarge.getMessage()));
            }
        }
        return super.handleHttpMessageNotReadable(ex, headers, status, request);
    }

    /** Last resort: log the cause, but never echo internals (SQL, stack traces) to the client. */
    @ExceptionHandler(Exception.class)
    ProblemDetail handleUnexpected(Exception e) {
        log.error("Unhandled error", e);
        return ProblemDetail.forStatusAndDetail(HttpStatus.INTERNAL_SERVER_ERROR, "Internal error; the request was not applied");
    }
}
