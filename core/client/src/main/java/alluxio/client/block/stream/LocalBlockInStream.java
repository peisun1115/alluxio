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

package alluxio.client.block.stream;

import alluxio.client.block.BlockStoreContext;
import alluxio.client.block.BlockWorkerClient;
import alluxio.client.file.options.InStreamOptions;
import alluxio.exception.ExceptionMessage;
import alluxio.util.io.BufferUtils;
import alluxio.wire.LockBlockResult;
import alluxio.wire.WorkerNetAddress;
import alluxio.worker.block.io.LocalFileBlockReader;

import com.google.common.io.Closer;
import io.netty.buffer.ByteBuf;
import io.netty.util.ReferenceCountUtil;

import java.io.IOException;
import java.nio.ByteBuffer;

import javax.annotation.concurrent.NotThreadSafe;

@NotThreadSafe
public final class LocalBlockInStream extends BlockInStream {
  /** The file reader to read a local block. */
  private final LocalFileBlockReader mLocalFileBlockReader;

  public LocalBlockInStream(long blockId, long blockSize, WorkerNetAddress workerNetAddress,
      BlockStoreContext context, InStreamOptions options) throws IOException {
    super(blockId, blockSize, workerNetAddress, context, options);

    try {
      mLocalFileBlockReader =
          mCloser.register(new LocalFileBlockReader(mLockBlockResult.getBlockPath()));
    } catch (IOException e) {
      mCloser.close();
      throw e;
    }
  }

  protected void destroyPacket(ByteBuf packet) {
    if (packet.nioBufferCount() > 0) {
      ByteBuffer buffer = packet.nioBuffer();
      if (buffer.isDirect()) {
        BufferUtils.cleanDirectBuffer(buffer);
      }
    }
    ReferenceCountUtil.release(packet);
  }

  protected PacketReader createPacketReader(long offset, long len) {
    return new LocalPacketReader(mLocalFileBlockReader, offset, (int) len);
  }
}
