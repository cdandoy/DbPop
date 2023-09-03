package org.dandoy.dbpopd.internals.problem;

import io.micronaut.context.annotation.Replaces;
import io.micronaut.core.annotation.NonNull;
import io.micronaut.http.HttpStatus;
import io.micronaut.http.exceptions.HttpStatusException;
import io.micronaut.http.server.exceptions.response.ErrorContext;
import io.micronaut.problem.ProblemErrorResponseProcessor;
import io.micronaut.problem.conf.ProblemConfiguration;
import jakarta.inject.Singleton;

@Replaces(ProblemErrorResponseProcessor.class)
@Singleton
public class ProblemErrorResponseProcessorReplacement
        extends ProblemErrorResponseProcessor {
    ProblemErrorResponseProcessorReplacement(ProblemConfiguration config) {
        super(config);
    }

    @Override
    protected boolean includeErrorMessage(@NonNull ErrorContext errorContext) {
        return errorContext.getRootCause()
                .map(t -> {
                    if (t instanceof HttpStatusException e) {
                        if (e.getStatus() != HttpStatus.INTERNAL_SERVER_ERROR) {
                            return true;
                        }
                    }
                    return false;
                })
                .orElse(false);
    }
}
