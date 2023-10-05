package no.statnett.k3alagexporter.utils;

import org.junit.jupiter.api.Test;

import java.util.Collection;
import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public final class RegexStringListFilterTest {

    @Test
    public void shouldAllowForEmptyLists() {
        assertTrue(isAllowed(Collections.emptyList(), Collections.emptyList(), "foo"));
    }

    @Test
    public void shouldAllowForNullLists() {
        assertTrue(isAllowed(null, null, "foo"));
    }

    @Test
    public void shouldBeImplicitlyAnchored() {
        assertTrue(isAllowed(null, Collections.singleton("foo"), "foobar"));
    }

    @Test
    public void shouldDenyExactMatch() {
        assertFalse(isAllowed(null, Collections.singleton("foo"), "foo"));
    }

    @Test
    public void shouldDenyWhenBothAllowedAndDenied() {
        assertFalse(isAllowed(Collections.singleton("foo"), Collections.singleton("foo"), "foo"));
    }

    @Test
    public void shouldAllowWhenAllowedAndNotDenied() {
        assertTrue(isAllowed(Collections.singleton("foo"), Collections.singleton("bar"), "foo"));
    }

    @Test
    public void shouldDenyWhenNotInAllowList() {
        assertFalse(isAllowed(Collections.singleton("foo"), null, "bar"));
    }

    @Test
    public void shouldAllowWhenRegexMatch() {
        assertTrue(isAllowed(Collections.singleton("foo.*"), null, "foobar"));
    }

    private boolean isAllowed(final Collection<String> allowRegexList, final Collection<String> denyRegexList, final String s) {
        return createFilter(allowRegexList, denyRegexList).isAllowed(s);
    }

    private RegexStringListFilter createFilter(final Collection<String> allowRegexList, final Collection<String> denyRegexList) {
        return new RegexStringListFilter(allowRegexList, denyRegexList);
    }

}
