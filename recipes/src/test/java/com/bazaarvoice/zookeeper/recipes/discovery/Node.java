package com.bazaarvoice.zookeeper.recipes.discovery;

import com.google.common.base.Charsets;
import com.google.common.base.Objects;

public class Node {
    private final String _name;

    public Node(String name) {
        _name = name;
    }

    public String getName() {
        return _name;
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(_name);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (!(obj instanceof Node)) return false;

        Node that = (Node) obj;
        return Objects.equal(_name, that.getName());
    }

    public static final NodeDataParser<Node> PARSER = new NodeDataParser<Node>() {
        @Override
        public Node parse(String path, byte[] data) {
            return new Node(new String(data, Charsets.UTF_8));
        }
    };

}
