package io.statnett.k3a.lagexporter.utils;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.function.Predicate;
import java.util.regex.Pattern;

public final class RegexStringListFilter implements Predicate<String> {

    private final List<Pattern> allowPatterns;
    private final List<Pattern> denyPatterns;

    public RegexStringListFilter(final Collection<String> allowRegexList, final Collection<String> denyRegexList) {
        this.allowPatterns = toPatterns(allowRegexList);
        this.denyPatterns = toPatterns(denyRegexList);
    }

    private static List<Pattern> toPatterns(final Collection<String> regexes) {
        if (regexes == null || regexes.isEmpty()) {
            return null;
        }
        final List<Pattern> patterns = new ArrayList<>();
        for (final String regex : regexes) {
            patterns.add(Pattern.compile(regex));
        }
        return patterns;
    }

    @Override
    public boolean test(String s) {
        return isAllowed(s);
    }

    public boolean isAllowed(final String s) {
        if (allowPatterns != null && !matchesAny(allowPatterns, s)) {
            return false;
        }
        return denyPatterns == null || !matchesAny(denyPatterns, s);
    }

    private static boolean matchesAny(final List<Pattern> patterns, final String s) {
        for (final Pattern pattern : patterns) {
            if (pattern.matcher(s).matches()) {
                return true;
            }
        }
        return false;
    }
}
