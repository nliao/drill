package org.apache.drill;

import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import org.apache.drill.exec.exception.DrillbitStartupException;
import org.apache.drill.exec.server.SlimDrillbit;
import org.junit.Test;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;

/**
 * @author ningliao
 */
public class TestSlimDrillbit extends BaseTestQuery  {
    @Test
    public void testSelect() throws DrillbitStartupException, IOException {
        SlimDrillbit bit = new SlimDrillbit();
        String sql = "SELECT columns[1] FROM dfs.`/Library/WebServer/Documents/numbers-1.csv`";
        SocketAddress address = new InetSocketAddress("127.0.0.1", 31010);
        java.nio.channels.SocketChannel socket = java.nio.channels.SocketChannel.open(address);
        SocketChannel channel = new NioSocketChannel(socket);

        bit.runQuery(channel, sql);
    }
}
