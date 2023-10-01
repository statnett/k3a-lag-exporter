package no.statnett.k3alagexporter.itest.utils;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.StreamTokenizer;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Parses Prometheus metrics. Note that this is not a complete implementation,
 * just enough to run the integration tests in this project.
 */
public final class MetricsParser {

    public record Metric(String name, double value, Map<String, String> labels) {
    }

    public List<Metric> getMetrics(final String metricsContents) {
        final List<Metric> metrics = new ArrayList<>();
        try {
            final BufferedReader reader = new BufferedReader(new StringReader(metricsContents));
            for (;;) {
                final String line = reader.readLine();
                if (line == null) {
                    break;
                }
                if (line.startsWith("#") || line.isBlank()) {
                    continue;
                }
                metrics.add(parseMetric(line));
            }
        } catch (final IOException e) {
            throw new RuntimeException(e);
        }
        return metrics;
    }

    private enum ParseState {
        EXPECTING_NAME,
        EXPECTING_LABELS,
        EXPECTING_LABEL_NAME,
        EXPECTING_EQUALS,
        EXPECTING_LABEL_VALUE,
        EXPECTING_COMMA,
        EXPECTING_VALUE
    }

    private Metric parseMetric(final String line) {
        final StreamTokenizer tokenizer = new StreamTokenizer(new StringReader(line));
        tokenizer.wordChars('_', '_');
        tokenizer.quoteChar('"');
        final Map<String, String> labels = new HashMap<>();
        String name = null;
        double value = 0.0;
        try {
            int token;
            String labelName = null;
            ParseState state = ParseState.EXPECTING_NAME;
            do {
                token = tokenizer.nextToken();
                if (state == ParseState.EXPECTING_NAME) {
                    if (token == StreamTokenizer.TT_WORD) {
                        name = tokenizer.sval;
                        state = ParseState.EXPECTING_LABELS;
                    } else {
                        throw new RuntimeException("Parse error for line " + line);
                    }
                } else if (state == ParseState.EXPECTING_LABELS) {
                    if (token == '{') {
                        state = ParseState.EXPECTING_LABEL_NAME;
                    } else {
                        throw new RuntimeException("Parse error for line " + line);
                    }
                } else if (state == ParseState.EXPECTING_LABEL_NAME) {
                    if (token == '}') {
                        state = ParseState.EXPECTING_VALUE;
                    } else if (token == StreamTokenizer.TT_WORD) {
                        labelName = tokenizer.sval;
                        state = ParseState.EXPECTING_EQUALS;
                    } else {
                        throw new RuntimeException("Parse error for line " + line);
                    }
                } else if (state == ParseState.EXPECTING_EQUALS) {
                    if (token == '=') {
                        state = ParseState.EXPECTING_LABEL_VALUE;
                    } else {
                        throw new RuntimeException("Parse error for line " + line);
                    }
                } else if (state == ParseState.EXPECTING_LABEL_VALUE) {
                    if (token == StreamTokenizer.TT_WORD || token == '"') {
                        final String labelValue = tokenizer.sval;
                        labels.put(labelName, labelValue);
                        state = ParseState.EXPECTING_COMMA;
                    } else {
                        throw new RuntimeException("Parse error for line " + line);
                    }
                } else if (state == ParseState.EXPECTING_COMMA) {
                    if (token == ',') {
                        state = ParseState.EXPECTING_LABEL_NAME;
                    } else if (token == '}') {
                        state = ParseState.EXPECTING_VALUE;
                    } else {
                        throw new RuntimeException("Parse error for line " + line);
                    }
                } else if (state == ParseState.EXPECTING_VALUE) {
                    if (token == StreamTokenizer.TT_NUMBER) {
                        value = tokenizer.nval;
                        break;
                    }
                } else {
                    throw new RuntimeException("Unhandled state " + state);
                }
            } while (token != StreamTokenizer.TT_EOF);
        } catch (final IOException e) {
            throw new RuntimeException(e);
        }
        return new Metric(name, value, labels);
    }


}
