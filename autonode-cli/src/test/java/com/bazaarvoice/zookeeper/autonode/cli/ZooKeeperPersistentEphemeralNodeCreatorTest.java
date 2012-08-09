package com.bazaarvoice.zookeeper.autonode.cli;

import com.beust.jcommander.JCommander;
import org.junit.Test;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Created with IntelliJ IDEA.
 * User: david.han
 * Date: 8/9/12
 * Time: 2:52 PM
 * To change this template use File | Settings | File Templates.
 */
public class ZooKeeperPersistentEphemeralNodeCreatorTest {
    ZooKeeperPersistentEphemeralNodeCreator _zkNodeCreator = new ZooKeeperPersistentEphemeralNodeCreator();

    static String _zkEnsembleOption = "-z";
    static String _zkEnsembleString = "aws-c0-prod-zk1.aws,aws-c0-prod-zk2.aws,aws-c0-prod-zk3.aws";
    static String _zkNodeOption = "-n";
    static String _zkNodePath = "/test/path/to/node-1";
    static String _zkNodeData = "node data";
    static String _zkNodeDataFilename = "@nodedata.txt";
    @Test
    public void testJCommander1(){
        String[] args = {_zkEnsembleOption, _zkEnsembleString,
                _zkNodeOption, _zkNodePath+"="+_zkNodeData
        };

        new JCommander(_zkNodeCreator, args);

        assertEquals(_zkEnsembleString, _zkNodeCreator.get_zooKeeperEnsemble());
        assertTrue(_zkNodeCreator.get_nodeList().contains(_zkNodePath+"="+_zkNodeData));
    }

}
