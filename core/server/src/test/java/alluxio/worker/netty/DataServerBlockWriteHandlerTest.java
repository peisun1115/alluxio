/*
 * The Alluxio Open Foundation licenses this work under the Apache License, version 2.0
 * (the "License"). You may not use this work except in compliance with the License, which is
 * available at www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied, as more fully set forth in the License.
 *
 * See the NOTICE file distributed with this work for information regarding copyright ownership.
 */

package alluxio.worker.netty;

import alluxio.network.protocol.RPCMessage;
import alluxio.network.protocol.RPCMessageDecoder;
import alluxio.network.protocol.RPCMessageEncoder;
import alluxio.network.protocol.RPCProtoMessage;
import alluxio.proto.dataserver.Protocol;
import alluxio.worker.block.BlockWorker;
import alluxio.worker.block.io.BlockWriter;
import alluxio.worker.block.io.LocalFileBlockWriter;

import io.netty.channel.embedded.EmbeddedChannel;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.io.File;

@RunWith(PowerMockRunner.class)
@PrepareForTest({BlockWorker.class})
public final class DataServerBlockWriteHandlerTest {
  private final long mBlockId = 1L;

  @Rule
  public TemporaryFolder mTestFolder = new TemporaryFolder();

  @Test
  public void writeBlock() throws Exception {
    BlockWorker blockWorker = PowerMockito.mock(BlockWorker.class);
    PowerMockito.doNothing().when(blockWorker)
        .createBlockRemote(Mockito.anyLong(), Mockito.anyLong(), Mockito.anyString(),
            Mockito.anyLong());
    PowerMockito.doNothing().when(blockWorker)
        .requestSpace(Mockito.anyLong(), Mockito.anyLong(), Mockito.anyLong());
    File file = mTestFolder.newFile();
    BlockWriter blockWriter = new LocalFileBlockWriter(file.getPath());
    PowerMockito.when(blockWorker.getTempBlockWriterRemote(Mockito.anyLong(), Mockito.anyLong()))
        .thenReturn(blockWriter);

    EmbeddedChannel channel =
        new EmbeddedChannel(RPCMessage.createFrameDecoder(), new RPCMessageDecoder(),
            new RPCMessageEncoder(),
            new DataServerBlockReadHandler(NettyExecutors.BLOCK_READER_EXECUTOR, blockWorker,
                FileTransferType.MAPPED));
    RPCProtoMessage msg = new RPCProtoMessage(
        Protocol.WriteRequest.newBuilder().setId(mBlockId).setOffset(0).setSessionId(1L).setType(
            Protocol.RequestType.ALLUXIO_BLOCK).build(), null);
    channel.writeOutbound(msg);
    Object o = channel.readOutbound();
    Object o2 = channel.readInbound();
  }
}
