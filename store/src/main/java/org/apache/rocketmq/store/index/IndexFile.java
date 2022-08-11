/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.rocketmq.store.index;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.util.List;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.store.MappedFile;

/**
 * 索引文件
 * 结构
 *   40 Byte     500w个4Byte                   2000w个20Byte
 * index header | hash slot | index Data Size | index Data
 */
public class IndexFile {
    private static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);
    /**
     * 每个hash slot 的大小
     */
    private static int hashSlotSize = 4;
    /**
     * 每个index条目的大小
     */
    private static int indexSize = 20;
    /**
     * 无效索引编号 特殊值
     */
    private static int invalidIndex = 0;
    /**
     * hash slot的数量 500w
     * slot存储对应index编号
     */
    private final int hashSlotNum;
    /**
     * 索引条目的数量 2000w
     * 因为可能存在hash冲突 所以比slot大
     */
    private final int indexNum;
    /**
     * 索引文件使用的mappedFile
     */
    private final MappedFile mappedFile;
    private final FileChannel fileChannel;
    private final MappedByteBuffer mappedByteBuffer;
    /**
     * 索引文件的索引头
     *
     *  第一条消息的存储时间                               第一条消息的偏移量          未被占用的slot数量     当前占用到的索引条目
     * beginTimestampIndex  |  endTimestampIndex  |  beginPhyoffsetIndex  |  hashSlotcountIndex  |  indexCountIndex
     */
    private final IndexHeader indexHeader;

    /**
     *
     * @param endPhyOffset 上个文件 最后一条消息 偏移量
     * @param endTimestamp 上个文件 最后一条消息 存储时间
     */
    public IndexFile(final String fileName, final int hashSlotNum, final int indexNum,
        final long endPhyOffset, final long endTimestamp) throws IOException {
        // 文件大小
        // 40 Byte + 500w * 4 Byte + 2000w * 20 Byte
        int fileTotalSize =
            IndexHeader.INDEX_HEADER_SIZE + (hashSlotNum * hashSlotSize) + (indexNum * indexSize);
        // fileName的MappedFile
        this.mappedFile = new MappedFile(fileName, fileTotalSize);
        this.fileChannel = this.mappedFile.getFileChannel();
        this.mappedByteBuffer = this.mappedFile.getMappedByteBuffer();
        // hash slot的数量 默认500w
        this.hashSlotNum = hashSlotNum;
        // 索引条目的数量 默认2000w
        this.indexNum = indexNum;

        ByteBuffer byteBuffer = this.mappedByteBuffer.slice();
        this.indexHeader = new IndexHeader(byteBuffer);

        // 设置上一个文件的最后一个消息的偏移量
        // 作为
        // 这个文件初始的偏移量
        if (endPhyOffset > 0) {
            this.indexHeader.setBeginPhyOffset(endPhyOffset);
            this.indexHeader.setEndPhyOffset(endPhyOffset);
        }

        if (endTimestamp > 0) {
            this.indexHeader.setBeginTimestamp(endTimestamp);
            this.indexHeader.setEndTimestamp(endTimestamp);
        }
    }

    public String getFileName() {
        return this.mappedFile.getFileName();
    }

    public void load() {
        // 调用indexHeader.load()
        // 设置indexHeader的一些属性
        this.indexHeader.load();
    }

    public void flush() {
        long beginTime = System.currentTimeMillis();
        // 刷盘
        if (this.mappedFile.hold()) {
            // 将当前的indexHeader的那些属性写入到ByteBuffer中
            this.indexHeader.updateByteBuffer();
            // 刷盘
            this.mappedByteBuffer.force();
            this.mappedFile.release();
            log.info("flush index file elapsed time(ms) " + (System.currentTimeMillis() - beginTime));
        }
    }

    public boolean isWriteFull() {
        return this.indexHeader.getIndexCount() >= this.indexNum;
    }

    public boolean destroy(final long intervalForcibly) {
        return this.mappedFile.destroy(intervalForcibly);
    }

    /**
     * 插入数据
     *
     * @param key  msg中的key (1. uniq_key 2. keys[]{aaa, bbb, ccc})
     * @param phyOffset 消息物理偏移量
     * @param storeTimestamp 消息存储时间
     */
    public boolean putKey(final String key, final long phyOffset, final long storeTimestamp) {
        // 判断一下索引文件是否写满了
        if (this.indexHeader.getIndexCount() < this.indexNum) {
            // 计算key的hash
            int keyHash = indexKeyHashMethod(key);
            // 取余获取hash对应的slot
            int slotPos = keyHash % this.hashSlotNum;
            // 对应slot的偏移量
            int absSlotPos = IndexHeader.INDEX_HEADER_SIZE + slotPos * hashSlotSize;

            FileLock fileLock = null;

            try {

                // fileLock = this.fileChannel.lock(absSlotPos, hashSlotSize, false);
                // 获取这个slot上的value
                // 因为可能存在hash冲突
                int slotValue = this.mappedByteBuffer.getInt(absSlotPos);
                // 因为 slot 上存储的是 index编号
                // slotValue > this.indexHeader.getIndexCount()
                // 就说明这个value不正常  可以认为没有发生hash冲突
                // 直接设置成invalidIndex
                if (slotValue <= invalidIndex || slotValue > this.indexHeader.getIndexCount()) {
                    slotValue = invalidIndex;
                }
                // 存储消息的时间与第一条消息的存储时间的差值
                long timeDiff = storeTimestamp - this.indexHeader.getBeginTimestamp();

                timeDiff = timeDiff / 1000;

                if (this.indexHeader.getBeginTimestamp() <= 0) {
                    // 第一条
                    timeDiff = 0;
                } else if (timeDiff > Integer.MAX_VALUE) {
                    // 超过最大值 设置成最大值
                    timeDiff = Integer.MAX_VALUE;
                } else if (timeDiff < 0) {
                    // 小于0 设置为 0
                    timeDiff = 0;
                }
                // 索引条目写入的位置
                // header + 500w * 4 + indexNum * 20
                int absIndexPos =
                    IndexHeader.INDEX_HEADER_SIZE + this.hashSlotNum * hashSlotSize
                        + this.indexHeader.getIndexCount() * indexSize;
                // 在指定位置写入数据
                this.mappedByteBuffer.putInt(absIndexPos, keyHash);
                this.mappedByteBuffer.putLong(absIndexPos + 4, phyOffset);
                this.mappedByteBuffer.putInt(absIndexPos + 4 + 8, (int) timeDiff);
                // 如果发生了hash冲突，那么这个slotValue就应该是这个slot上一个数据的index偏移量
                this.mappedByteBuffer.putInt(absIndexPos + 4 + 8 + 4, slotValue);

                // 向slot里写入index编号
                this.mappedByteBuffer.putInt(absSlotPos, this.indexHeader.getIndexCount());

                // 如果是第一条消息
                // 重置开始偏移量和开始时间戳
                if (this.indexHeader.getIndexCount() <= 1) {
                    this.indexHeader.setBeginPhyOffset(phyOffset);
                    this.indexHeader.setBeginTimestamp(storeTimestamp);
                }

                // 这说明没有发生hash冲突
                if (invalidIndex == slotValue) {
                    // 占用的slot + 1
                    this.indexHeader.incHashSlotCount();
                }
                this.indexHeader.incIndexCount();
                this.indexHeader.setEndPhyOffset(phyOffset);
                this.indexHeader.setEndTimestamp(storeTimestamp);

                return true;
            } catch (Exception e) {
                log.error("putKey exception, Key: " + key + " KeyHashCode: " + key.hashCode(), e);
            } finally {
                if (fileLock != null) {
                    try {
                        fileLock.release();
                    } catch (IOException e) {
                        log.error("Failed to release the lock", e);
                    }
                }
            }
        } else {
            log.warn("Over index file capacity: index count = " + this.indexHeader.getIndexCount()
                + "; index max num = " + this.indexNum);
        }

        return false;
    }

    public int indexKeyHashMethod(final String key) {
        int keyHash = key.hashCode();
        int keyHashPositive = Math.abs(keyHash);
        if (keyHashPositive < 0)
            keyHashPositive = 0;
        return keyHashPositive;
    }

    public long getBeginTimestamp() {
        return this.indexHeader.getBeginTimestamp();
    }

    public long getEndTimestamp() {
        return this.indexHeader.getEndTimestamp();
    }

    public long getEndPhyOffset() {
        return this.indexHeader.getEndPhyOffset();
    }

    public boolean isTimeMatched(final long begin, final long end) {
        boolean result = begin < this.indexHeader.getBeginTimestamp() && end > this.indexHeader.getEndTimestamp();
        result = result || (begin >= this.indexHeader.getBeginTimestamp() && begin <= this.indexHeader.getEndTimestamp());
        result = result || (end >= this.indexHeader.getBeginTimestamp() && end <= this.indexHeader.getEndTimestamp());
        return result;
    }

    /**
     * 根据key查询索引数据
     */
    public void selectPhyOffset(final List<Long> phyOffsets, final String key, final int maxNum,
        final long begin, final long end, boolean lock) {
        // 查询数据 防止mappedfile被删
        if (this.mappedFile.hold()) {

            // 获取到这个key对应的 slot 偏移量
            int keyHash = indexKeyHashMethod(key);
            int slotPos = keyHash % this.hashSlotNum;
            int absSlotPos = IndexHeader.INDEX_HEADER_SIZE + slotPos * hashSlotSize;

            FileLock fileLock = null;
            try {
                if (lock) {
                    // fileLock = this.fileChannel.lock(absSlotPos,
                    // hashSlotSize, true);
                }

                // 获取到slot的value
                int slotValue = this.mappedByteBuffer.getInt(absSlotPos);
                // if (fileLock != null) {
                // fileLock.release();
                // fileLock = null;
                // }

                // 获取slot里的value是没有被占用的
                // 或者这个索引文件里一条索引条目都没有
                if (slotValue <= invalidIndex || slotValue > this.indexHeader.getIndexCount()
                    || this.indexHeader.getIndexCount() <= 1) {

                } else { // 正常找到索引走这里
                    // 这里存在哈希冲突的一种处理
                    // prevIndexRead 指向的同一个slot哈希冲突的下一条数据
                    // 通过prevIndexRead将哈希冲突的数据组成一个链表
                    // nextIndexToRead = prevIndexRead
                    // 遍历整个链表 找到对应的key
                    for (int nextIndexToRead = slotValue; ; ) {
                        // 遍历完所有的index了
                        if (phyOffsets.size() >= maxNum) {
                            break;
                        }
                        // 索引对应的偏移量
                        int absIndexPos =
                            IndexHeader.INDEX_HEADER_SIZE + this.hashSlotNum * hashSlotSize
                                + nextIndexToRead * indexSize;

                        // 获取索引数据
                        int keyHashRead = this.mappedByteBuffer.getInt(absIndexPos);
                        long phyOffsetRead = this.mappedByteBuffer.getLong(absIndexPos + 4);
                        long timeDiff = (long) this.mappedByteBuffer.getInt(absIndexPos + 4 + 8);
                        // 哈希冲突时 下一条数据的索引位置
                        int prevIndexRead = this.mappedByteBuffer.getInt(absIndexPos + 4 + 8 + 4);

                        if (timeDiff < 0) {
                            break;
                        }

                        timeDiff *= 1000L;

                        long timeRead = this.indexHeader.getBeginTimestamp() + timeDiff;
                        // 时间范围
                        boolean timeMatched = (timeRead >= begin) && (timeRead <= end);
                        // 匹配中加入到phyOffsets集合中
                        if (keyHash == keyHashRead && timeMatched) {
                            phyOffsets.add(phyOffsetRead);
                        }
                        // 是否还有下一个相同hash的索引
                        if (prevIndexRead <= invalidIndex
                            || prevIndexRead > this.indexHeader.getIndexCount()
                            || prevIndexRead == nextIndexToRead || timeRead < begin) {
                            break;
                        }

                        nextIndexToRead = prevIndexRead;
                    }
                }
            } catch (Exception e) {
                log.error("selectPhyOffset exception ", e);
            } finally {
                if (fileLock != null) {
                    try {
                        fileLock.release();
                    } catch (IOException e) {
                        log.error("Failed to release the lock", e);
                    }
                }

                this.mappedFile.release();
            }
        }
    }
}
