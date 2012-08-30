package com.bazaarvoice.zookeeper.internal;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class NamespacesTest {

    @Test
    public void testNormalizeNull() {
        assertNull(Namespaces.normalize(null));
    }

    @Test
    public void testNormalizeEmpty() {
        assertNull(Namespaces.normalize(""));
    }

    @Test
    public void testNormalizeSlash() {
        assertNull(Namespaces.normalize("/"));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testNormalizeRelative() {
        Namespaces.normalize("a");
    }

    @Test
    public void testNormalizeAbsolute() {
        assertEquals("/root", Namespaces.normalize("/root"));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testNormalizeTrailingSlash() {
        Namespaces.normalize("/root/");
    }

    @Test
    public void testNormalizeBlank() {
        assertEquals("/ ", Namespaces.normalize("/ "));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testNormalizeTab() {
        Namespaces.normalize("/\t");
    }

    @Test(expected = IllegalArgumentException.class)
    public void testNormalizeNewline() {
        Namespaces.normalize("/\n");
    }

    @Test(expected = IllegalArgumentException.class)
    public void testNormalizeSlashDot() {
        Namespaces.normalize("/.");
    }

    @Test(expected = IllegalArgumentException.class)
    public void testNormalizeSlashDotDot() {
        Namespaces.normalize("/..");
    }

    @Test
    public void testNormalizeSlashDotDotDot() {
        assertEquals("/...", Namespaces.normalize("/..."));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testNormalizeEmbeddedSlashSlash() {
        Namespaces.normalize("/abc//def");
    }

    @Test(expected = IllegalArgumentException.class)
    public void testNormalizeEmbeddedSlashDot() {
        Namespaces.normalize("/abc/./def");
    }

    @Test(expected = IllegalArgumentException.class)
    public void testNormalizeEmbeddedSlashDotDot() {
        Namespaces.normalize("/abc/../def");
    }

    @Test
    public void testNormalizeEmbeddedSlashDotDotDot() {
        assertEquals("/abc/.../def", Namespaces.normalize("/abc/.../def"));
    }
}
