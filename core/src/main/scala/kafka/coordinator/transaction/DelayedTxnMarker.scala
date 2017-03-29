/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package kafka.coordinator.transaction

import kafka.server.DelayedOperation
import org.apache.kafka.common.protocol.Errors

/**
  * Delayed transaction state change operations that are added to the purgatory without timeout (i.e. these operations should never time out)
  */
private[transaction] class DelayedTxnMarker(TxnMetadata: TransactionMetadata,
                                            responseCallback: Errors => Unit)
  extends DelayedOperation(Long.MaxValue) {

  // overridden since tryComplete already synchronizes on the existing txn metadata. This makes it safe to
  // call purgatory operations while holding the group lock.
  override def safeTryComplete(): Boolean = tryComplete()

  // whenever this function is triggered, we can always safely complete the operation;
  override def tryComplete(): Boolean = forceComplete()

  override def onExpiration(): Unit = {
    // this should never happen
    throw new IllegalStateException("Delayed txn state change operation to " + newTxnMetadata + " has timed out, this should never happen.")
  }
  override def onComplete(): Unit = coordinator.tryUpdateTxnState(transactionalId, newTxnMetadata, responseCallback)
}
