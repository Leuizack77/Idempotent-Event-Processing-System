package com.example.eventprocessor.config;

import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import org.slf4j.MDC;
import org.springframework.core.Ordered;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;
import org.springframework.web.filter.OncePerRequestFilter;

import java.io.IOException;
import java.util.UUID;

/**
 * Servlet filter establishing Mapped Diagnostic Context (MDC) for HTTP requests.
 * Extracts or generates correlation / trace IDs, populates MDC, ensures propagation
 * in response headers, and guarantees thread-local cleanup in finally.
 */
@Component
@Order(Ordered.HIGHEST_PRECEDENCE)
public class MdcFilter extends OncePerRequestFilter {

    public static final String TRACE_ID_KEY = "traceId";
    public static final String CORRELATION_ID_KEY = "correlationId";
    public static final String EVENT_ID_KEY = "eventId";
    public static final String EVENT_TYPE_KEY = "eventType";

    public static final String HEADER_CORRELATION_ID = "X-Correlation-ID";
    public static final String HEADER_TRACE_ID = "X-Trace-ID";

    @Override
    protected void doFilterInternal(
            HttpServletRequest request,
            HttpServletResponse response,
            FilterChain filterChain
    ) throws ServletException, IOException {
        String traceId = request.getHeader(HEADER_CORRELATION_ID);
        if (traceId == null || traceId.isBlank()) {
            traceId = request.getHeader(HEADER_TRACE_ID);
        }
        if (traceId == null || traceId.isBlank()) {
            traceId = "tr-" + UUID.randomUUID().toString().replace("-", "").substring(0, 8);
        } else {
            traceId = traceId.trim();
        }

        MDC.put(TRACE_ID_KEY, traceId);
        MDC.put(CORRELATION_ID_KEY, traceId);

        response.setHeader(HEADER_CORRELATION_ID, traceId);
        response.setHeader(HEADER_TRACE_ID, traceId);

        try {
            filterChain.doFilter(request, response);
        } finally {
            MDC.clear();
        }
    }
}
