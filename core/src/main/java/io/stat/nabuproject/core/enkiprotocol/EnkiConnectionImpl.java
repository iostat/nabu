package io.stat.nabuproject.core.enkiprotocol;

import io.netty.channel.ChannelHandlerContext;

/**
 * Created by io on 1/2/16. io is an asshole because
 * he doesn't write documentation for his code.
 *
 * @author Ilya Ostrovskiy (https://github.com/iostat/)
 */
public class EnkiConnectionImpl implements EnkiConnection {
    public EnkiConnectionImpl(ChannelHandlerContext ctx,
                              EnkiClientEventListener toNotify) {
    }


    @Override
    public void leave() {
        
    }

    @Override
    public void onDisconnected() {

    }
}
