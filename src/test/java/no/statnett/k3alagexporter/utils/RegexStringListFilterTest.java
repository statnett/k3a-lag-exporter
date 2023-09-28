package no.statnett.k3alagexporter.utils;

import org.junit.Assert;
import org.junit.Test;

import java.util.Collection;
import java.util.Collections;

public final class RegexStringListFilterTest {

    @Test
    public void shouldAllowForEmptyLists() {
        Assert.assertTrue(isAllowed(Collections.emptyList(), Collections.emptyList(), "foo"));
    }

    @Test
    public void shouldAllowForNullLists() {
        Assert.assertTrue(isAllowed(null, null, "foo"));
    }

    @Test
    public void shouldBeImplicitlyAnchored() {
        Assert.assertTrue(isAllowed(null, Collections.singleton("foo"), "foobar"));
    }

    @Test
    public void shouldDenyExactMatch() {
        Assert.assertFalse(isAllowed(null, Collections.singleton("foo"), "foo"));
    }

    @Test
    public void shouldDenyWhenBothAllowedAndDenied() {
        Assert.assertFalse(isAllowed(Collections.singleton("foo"), Collections.singleton("foo"), "foo"));
    }

    @Test
    public void shouldAllowWhenAllowedAndNotDenied() {
        Assert.assertTrue(isAllowed(Collections.singleton("foo"), Collections.singleton("bar"), "foo"));
    }

    @Test
    public void shouldDenyWhenNotInAllowList() {
        Assert.assertFalse(isAllowed(Collections.singleton("foo"), null, "bar"));
    }

    @Test
    public void shouldAllowWhenRegexMatch() {
        Assert.assertTrue(isAllowed(Collections.singleton("foo.*"), null, "foobar"));
    }

    private boolean isAllowed(final Collection<String> allowRegexList, final Collection<String> denyRegexList, final String s) {
        return createFilter(allowRegexList, denyRegexList).isAllowed(s);
    }

    private RegexStringListFilter createFilter(final Collection<String> allowRegexList, final Collection<String> denyRegexList) {
        return new RegexStringListFilter(allowRegexList, denyRegexList);
    }

}
