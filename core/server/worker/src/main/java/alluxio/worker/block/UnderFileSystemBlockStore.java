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

package alluxio.worker.block;

import alluxio.exception.BlockAlreadyExistsException;
import alluxio.exception.BlockDoesNotExistException;
import alluxio.exception.ExceptionMessage;
import alluxio.underfs.options.CreateOptions;
import alluxio.worker.block.io.BlockReader;
import alluxio.worker.block.io.BlockWriter;
import alluxio.worker.block.meta.UnderFileSystemBlockMeta;
import alluxio.worker.block.options.OpenUfsBlockOptions;

import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.locks.ReentrantLock;

import javax.annotation.concurrent.GuardedBy;

/**
 * This class manages the virtual blocks in the UFS for delegated UFS reads/writes.
 *
 * The usage pattern:
 *  acquireAccess(sessionId, blockId, options)
 *  closeReaderOrWriter(sessionId, blockId)
 *  releaseAccess(sessionId, blockId)
 *
 * If the client is lost before releasing or cleaning up the session, the session cleaner will
 * clean the data.
 */
// TODO(peis): When we stop supporting the non-packet-streaming, simplify and move these logic to
// the data server since we do not need to keep track of the reader/writer in packet streaming mode.
public final class UnderFileSystemBlockStore {
  private static final Logger LOG = LoggerFactory.getLogger(UnderFileSystemBlockStore.class);

  /**
   * This lock protects mBlocks, mSessionIdToBlockIds and mBlockIdToSessionIds. For any read/write
   * operations to these maps, the lock needs to be acquired. But once you get the block
   * information from the map (e.g. mBlocks), the lock does not need to be acquired. For example,
   * the block reader/writer within the BlockInfo can be updated without acquiring this lock.
   * This is based on the assumption that one session won't open multiple readers/writers on the
   * same block. If the client do that, the client can see failures but the worker won't crash.
   */
  private final ReentrantLock mLock = new ReentrantLock();
  @GuardedBy("mLock")
  /** Maps from the {@link Key} to the {@link BlockInfo}. */
  private final Map<Key, BlockInfo> mBlocks = new HashMap<>();
  @GuardedBy("mLock")
  /** Maps from the session ID to the block IDs. */
  private final Map<Long, Set<Long>> mSessionIdToBlockIds = new HashMap<>();
  @GuardedBy("mLock")
  /** Maps from the block ID to the session IDs. */
  private final Map<Long, Set<Long>> mBlockIdToSessionIds = new HashMap<>();

  /** The Local block store. */
  private final BlockStore mLocalBlockStore;

  /**
   * Creates an instance of {@link UnderFileSystemBlockStore}.
   *
   * @param localBlockStore the local block store
   */
  public UnderFileSystemBlockStore(BlockStore localBlockStore) {
    mLocalBlockStore = localBlockStore;
  }

  /**
   * Opens a UFS block given a {@link UnderFileSystemBlockMeta} and the limit on
   * the maximum concurrency on the block. If the number of concurrent readers on this UFS block
   * exceeds a threshold, the token is not granted and this method returns false.
   *
   * @param sessionId the session ID
   * @param blockId maximum concurrency
   * @param options the options
   * @return whether the UFS block is successfully opened
   * @throws BlockAlreadyExistsException if the block already exists for a session ID
   */
  public boolean openUfsBlock(long sessionId, long blockId, OpenUfsBlockOptions options)
      throws BlockAlreadyExistsException {
    UnderFileSystemBlockMeta blockMeta = new UnderFileSystemBlockMeta(sessionId, blockId, options);
    mLock.lock();
    try {
      Key key = new Key(sessionId, blockId);
      if (mBlocks.containsKey(key)) {
        throw new BlockAlreadyExistsException(ExceptionMessage.UFS_BLOCK_ALREADY_EXISTS_FOR_SESSION,
            blockId, blockMeta.getUnderFileSystemPath(), sessionId);
      }
      Set<Long> sessionIds = mBlockIdToSessionIds.get(blockId);
      if (sessionIds != null && sessionIds.size() >= options.getMaxUfsReadConcurrency()) {
        return false;
      }
      if (sessionIds == null) {
        sessionIds = new HashSet<>();
        mBlockIdToSessionIds.put(blockId, sessionIds);
      }
      sessionIds.add(sessionId);

      mBlocks.put(key, new BlockInfo(blockMeta));

      Set<Long> blockIds = mSessionIdToBlockIds.get(sessionId);
      if (blockIds == null) {
        blockIds = new HashSet<>();
        mSessionIdToBlockIds.put(sessionId, blockIds);
      }
      blockIds.add(blockId);
    } finally {
      mLock.unlock();
    }
    return true;
  }

  /**
   * Creates a UFS block to write with options. Note: a UFS file during write only has one block
   * from Alluxio's point of view. The block ID has to be the same as file ID which is the largest
   * possible blockId corresponding to the file ID.
   *
   * @param sessionId
   * @param blockId
   * @param options
   */
  public void createUfsBlock(long sessionId, long blockId, CreateOptions options)
      throws BlockAlreadyExistsException, IOException {
    UnderFileSystemBlockMeta blockMeta = new UnderFileSystemBlockMeta(sessionId, blockId, options);
    BlockInfo blockInfo;
    mLock.lock();
    try {
      Set<Long> sessionIds = mBlockIdToSessionIds.get(blockId);
      if (sessionIds != null) {
        throw new BlockAlreadyExistsException(ExceptionMessage.UFS_BLOCK_ALREADY_EXISTS_FOR_SESSION,
            blockId, blockMeta.getUnderFileSystemPath(), sessionId);
      }
      sessionIds = new HashSet<>();
      sessionIds.add(sessionId);
      mBlockIdToSessionIds.put(blockId, sessionIds);
      blockInfo = new BlockInfo(blockMeta);
      mBlocks.put(new Key(sessionId, blockId), blockInfo);

      Set<Long> blockIds = mSessionIdToBlockIds.get(sessionId);
      if (blockIds == null) {
        blockIds = new HashSet<>();
        mSessionIdToBlockIds.put(sessionId, blockIds);
      }
      blockIds.add(blockId);
    } finally {
      mLock.unlock();
    }
    UnderFileSystemBlockWriter blockWriter = UnderFileSystemBlockWriter.create(blockMeta, options);
    blockInfo.setBlockWriter(blockWriter);
  }

  public void cancelUfsBlock(long session, long blockId) throws IOException {
    BlockInfo blockInfo = getBlockInfoOrNull(session, blockId);
    if (blockInfo != null && blockInfo.getBlockWriter() != null) {
      blockInfo.getBlockWriter().cancel();
    }
  }

  public void completeUfsBlock(long session, long blockId) throws IOException {
    BlockInfo blockInfo = getBlockInfoOrNull(session, blockId);
    if (blockInfo != null && blockInfo.getBlockWriter() != null) {
      blockInfo.getBlockWriter().close();
    }
  }

  public void closeUfsBlock(long session, long blockId) throws IOException {
    BlockInfo blockInfo = getBlockInfoOrNull(session, blockId);
    if (blockInfo != null && blockInfo.getBlockReader() != null) {
      blockInfo.getBlockReader().close();
    }
  }



  /**
   * Releases the access token of this block by removing this (sessionId, blockId) pair from the
   * store.
   *
   * @param sessionId the session ID
   * @param blockId the block ID
   */
  public void release(long sessionId, long blockId) {
    mLock.lock();
    try {
      Key key = new Key(sessionId, blockId);
      if (!mBlocks.containsKey(key)) {
        LOG.warn("Key (block ID: {}, session ID {}) is not found when releasing the UFS block.",
            blockId, sessionId);
      }
      mBlocks.remove(key);
      Set<Long> blockIds = mSessionIdToBlockIds.get(sessionId);
      if (blockIds != null) {
        blockIds.remove(blockId);
      }
      Set<Long> sessionIds = mBlockIdToSessionIds.get(blockId);
      if (sessionIds != null) {
        sessionIds.remove(sessionId);
      }
    } finally {
      mLock.unlock();
    }
  }

  /**
   * Cleans up all the block information(e.g. block reader/writer) that belongs to this session.
   *
   * @param sessionId the session ID
   */
  public void cleanupSession(long sessionId) {
    Set<Long> blockIds;
    mLock.lock();
    try {
      blockIds = mSessionIdToBlockIds.get(sessionId);
      if (blockIds == null) {
        return;
      }
    } finally {
      mLock.unlock();
    }

    for (Long blockId : blockIds) {
      try {
        // Note that we don't need to explicitly call abortBlock to cleanup the temp block
        // in Local block store because they will be cleanup by the session cleaner in the
        // Local block store.
        closeUfsBlock(sessionId, blockId);
        cancelUfsBlock(sessionId, blockId);
        release(sessionId, blockId);
      } catch (Exception e) {
        LOG.warn("Failed to cleanup UFS block {}, session {}.", blockId, sessionId);
      }
    }
  }

  /**
   * Creates a block reader that reads from UFS and optionally caches the block to the Alluxio
   * block store.
   *
   * @param sessionId the client session ID that requested this read
   * @param blockId the ID of the block to read
   * @param offset the read offset within the block (NOT the file)
   * @param noCache if set, do not try to cache the block in the Alluxio worker
   * @return the block reader instance
   * @throws BlockDoesNotExistException if the UFS block does not exist in the
   * {@link UnderFileSystemBlockStore}
   * @throws IOException if any I/O errors occur
   */
  public BlockReader getBlockReader(long sessionId, long blockId, long offset, boolean noCache)
      throws BlockDoesNotExistException, IOException {
    BlockInfo blockInfo = getBlockInfo(sessionId, blockId);
    BlockReader blockReader = blockInfo.getBlockReader();
    if (blockReader != null) {
      return blockReader;
    }
    BlockReader reader =
        UnderFileSystemBlockReader.create(blockInfo.getMeta(), offset, noCache, mLocalBlockStore);
    blockInfo.setBlockReader(reader);
    return reader;
  }

  /**
   *
   * @param sessionId
   * @param blockId
   * @return
   * @throws BlockDoesNotExistException
   */
  public BlockWriter getBlockWriter(long sessionId, long blockId)
      throws BlockDoesNotExistException {
    BlockInfo blockInfo = getBlockInfo(sessionId, blockId);
    // The block writer is created when the UFS block is created.
    return Preconditions.checkNotNull(blockInfo.getBlockWriter());
  }


  /**
   * Gets the {@link UnderFileSystemBlockMeta} for a session ID and block ID pair.
   *
   * @param sessionId the session ID
   * @param blockId the block ID
   * @return the {@link UnderFileSystemBlockMeta} instance
   * @throws BlockDoesNotExistException if the UFS block does not exist in the
   * {@link UnderFileSystemBlockStore}
   */
  private BlockInfo getBlockInfo(long sessionId, long blockId) throws BlockDoesNotExistException {
    BlockInfo blockInfo = getBlockInfoOrNull(sessionId, blockId);
    if (blockInfo == null) {
      throw new BlockDoesNotExistException(ExceptionMessage.UFS_BLOCK_DOES_NOT_EXIST_FOR_SESSION,
          blockId, sessionId);
    }
    return blockInfo;
  }

  private BlockInfo getBlockInfoOrNull(long sessionId, long blockId) {
    mLock.lock();
    try {
      return mBlocks.get(new Key(sessionId, blockId));
    } finally {
      mLock.unlock();
    }
  }

  private static class Key {
    private final long mSessionId;
    private final long mBlockId;

    /**
     * Creates an instance of the Key class.
     *
     * @param sessionId the session ID
     * @param blockId the block ID
     */
    public Key(long sessionId, long blockId) {
      mSessionId = sessionId;
      mBlockId = blockId;
    }

    /**
     * @return the block ID
     */
    public long getBlockId() {
      return mBlockId;
    }

    /**
     * @return the session ID
     */
    public long getSessionId() {
      return mSessionId;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (!(o instanceof Key)) {
        return false;
      }

      Key that = (Key) o;
      return Objects.equal(mBlockId, that.mBlockId) && Objects.equal(mSessionId, that.mSessionId);
    }

    @Override
    public int hashCode() {
      return Objects.hashCode(mBlockId, mSessionId);
    }

    @Override
    public String toString() {
      return Objects.toStringHelper(this).add("blockId", mBlockId).add("sessionId", mSessionId)
          .toString();
    }
  }

  /**
   * This class is to wrap block reader/writer and the block meta into one class. The block
   * reader/writer is not part of the {@link UnderFileSystemBlockMeta} because
   * 1. UnderFileSystemBlockMeta only keeps immutable information.
   * 2. We do not want a cyclic dependency between {@link UnderFileSystemBlockReader} and
   *    {@link UnderFileSystemBlockMeta}.
   */
  private static class BlockInfo {
    private final UnderFileSystemBlockMeta mMeta;

    // A correct client implementation should never access the following reader/writer
    // concurrently. But just to avoid crashing the server thread with runtime exception when
    // the client is mis-behaving, we access them with locks acquired.
    private BlockReader mBlockReader;
    private BlockWriter mBlockWriter;

    /**
     * Creates an instance of {@link BlockInfo}.
     *
     * @param meta the UFS block meta
     */
    public BlockInfo(UnderFileSystemBlockMeta meta) {
      mMeta = meta;
    }

    /**
     * @return the UFS block meta
     */
    public UnderFileSystemBlockMeta getMeta() {
      return mMeta;
    }

    /**
     * @return the cached the block reader if it is not closed
     */
    public synchronized BlockReader getBlockReader() {
      if (mBlockReader != null && mBlockReader.isClosed()) {
        mBlockReader = null;
      }
      return mBlockReader;
    }

    /**
     * @param blockReader the block reader to be set
     */
    public synchronized void setBlockReader(BlockReader blockReader) {
      mBlockReader = blockReader;
    }

    /**
     * @return the block writer
     */
    public synchronized BlockWriter getBlockWriter() {
      return mBlockWriter;
    }

    /**
     * @param blockWriter the block writer to be set
     */
    public synchronized void setBlockWriter(BlockWriter blockWriter) {
      mBlockWriter = blockWriter;
    }
  }
}
