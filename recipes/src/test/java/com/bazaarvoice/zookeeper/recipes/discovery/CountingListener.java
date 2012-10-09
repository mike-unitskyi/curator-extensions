package com.bazaarvoice.zookeeper.recipes.discovery;

public final class CountingListener implements NodeListener<Node> {
    private int _numAdds;
    private int _numRemoves;
    private int _numUpdates;

    @Override
    public void onNodeAdded(String path, Node node) {
        _numAdds++;
    }

    @Override
    public void onNodeRemoved(String path, Node node) {
        _numRemoves++;
    }

    @Override
    public void onNodeUpdated(String path, Node node) {
        _numUpdates++;
    }

    public int getNumAdds() {
        return _numAdds;
    }

    public int getNumRemoves() {
        return _numRemoves;
    }

    public int getNumUpdates() {
        return _numUpdates;
    }

    public int getNumEvents() {
        return _numAdds + _numRemoves + _numUpdates;
    }
}
