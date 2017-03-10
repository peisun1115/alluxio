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

import alluxio.metrics.MetricsSystem;
import alluxio.network.protocol.RPCProtoMessage;
import alluxio.proto.dataserver.Protocol;
import alluxio.underfs.options.CreateOptions;
import alluxio.worker.block.BlockWorker;
import alluxio.worker.block.io.BlockWriter;

import com.codahale.metrics.Counter;
import io.netty.buffer.ByteBuf;

import java.io.IOException;
import java.util.concurrent.ExecutorService;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * This handler handles file write request. Check more information in
 * {@link DataServerUFSBlockWriteHandler}.
 */
@NotThreadSafe
public final class DataServerUFSBlockWriteHandler extends DataServerWriteHandler {

  /** The Block Worker. */
  private final BlockWorker mWorker;

  private class UfsBlockWriteRequestInternal extends WriteRequestInternal {
    public BlockWriter mBlockWriter;

    public UfsBlockWriteRequestInternal(Protocol.WriteRequest request) throws Exception {
      mBlockWriter = mWorker.getUfsBlockWriter(request.getSessionId(), request.getId(),
          CreateOptions.fromProto(request.getCreateUfsFileOptions()));
      mId = request.getId();
    }

    @Override
    public void close() throws IOException {
      // mBlockWriter is closed in the BlockWorker.
      // TODO(peis): In order to manage the life of the block writer here, we need to have better
      // cancel support in NettyPacketWriter.
    }
  }

  /**
   * Creates an instance of {@link DataServerUFSBlockWriteHandler}.
   *
   * @param executorService the executor service to run {@link PacketWriter}s
   * @param worker the block worker
   */
  public DataServerUFSBlockWriteHandler(ExecutorService executorService, BlockWorker worker) {
    super(executorService);
    mWorker = worker;
  }

  @Override
  protected boolean acceptMessage(Object object) {
    if (!super.acceptMessage(object)) {
      return false;
    }
    Protocol.WriteRequest request = ((RPCProtoMessage) object).getMessage().getMessage();
    return request.getType() == Protocol.RequestType.UFS_FILE;
  }

  /**
   * Initializes the handler if necessary.
   *
   * @param msg the block write request
   * @throws Exception if it fails to initialize
   */
  @Override
  protected void initializeRequest(RPCProtoMessage msg) throws Exception {
    super.initializeRequest(msg);
    if (mRequest == null) {
      mRequest =
          new UfsBlockWriteRequestInternal(msg.getMessage().<Protocol.WriteRequest>getMessage());
    }
  }

  @Override
  protected void writeBuf(ByteBuf buf, long pos) throws Exception {
    ((UfsBlockWriteRequestInternal) mRequest).mBlockWriter.transferFrom(buf);
  }

  @Override
  protected void incrementMetrics(long bytesWritten) {
    Metrics.BYTES_WRITTEN_UFS.inc(bytesWritten);
  }

  /**
   * Class that contains metrics for BlockDataServerHandler.
   */
  private static final class Metrics {
    private static final Counter BYTES_WRITTEN_UFS = MetricsSystem.workerCounter("BytesWrittenUFS");

    private Metrics() {
    } // prevent instantiation
  }
}
