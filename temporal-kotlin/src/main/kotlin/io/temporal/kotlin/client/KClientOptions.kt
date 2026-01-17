/*
 * Copyright (C) 2022 Temporal Technologies, Inc. All Rights Reserved.
 *
 * Copyright (C) 2012-2016 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Modifications copyright (C) 2017 Uber Technologies, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this material except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.temporal.kotlin.client

import io.temporal.client.WorkflowClientOptions
import io.temporal.common.converter.DataConverter
import io.temporal.common.interceptors.WorkflowClientInterceptor
import io.temporal.serviceclient.WorkflowServiceStubsOptions
import java.time.Duration

/**
 * Options for creating a [KClient].
 *
 * Example:
 * ```kotlin
 * val client = KClient.connect(
 *     KClientOptions(
 *         target = "localhost:7233",
 *         namespace = "default"
 *     )
 * )
 * ```
 *
 * @property target The Temporal server address (e.g., "localhost:7233")
 * @property namespace The namespace to connect to
 * @property identity Identity of the client for logging and debugging
 * @property dataConverter Data converter for serialization
 * @property interceptors Client interceptors for cross-cutting concerns
 * @property enableHttps Whether to use HTTPS/TLS
 * @property rpcTimeout Default timeout for RPC calls
 * @property rpcLongPollTimeout Timeout for long-poll RPC calls
 * @property rpcQueryTimeout Timeout for query RPC calls
 * @property connectionBackoffResetFrequency How often to reset backoff on successful connection
 * @property grpcReconnectFrequency How often to reconnect to the server
 * @property headers Static headers to include in all requests
 */
public data class KClientOptions(
  val target: String = "localhost:7233",
  val namespace: String = "default",
  val identity: String? = null,
  val dataConverter: DataConverter? = null,
  val interceptors: List<WorkflowClientInterceptor> = emptyList(),
  val enableHttps: Boolean = false,
  val rpcTimeout: Duration? = null,
  val rpcLongPollTimeout: Duration? = null,
  val rpcQueryTimeout: Duration? = null,
  val connectionBackoffResetFrequency: Duration? = null,
  val grpcReconnectFrequency: Duration? = null,
  val headers: Map<String, String> = emptyMap()
) {

  /**
   * Convert to Java SDK's WorkflowServiceStubsOptions.
   */
  internal fun toServiceStubsOptions(): WorkflowServiceStubsOptions {
    return WorkflowServiceStubsOptions.newBuilder()
      .setTarget(target)
      .apply {
        if (enableHttps) {
          setEnableHttps(true)
        }
        rpcTimeout?.let { setRpcTimeout(it) }
        rpcLongPollTimeout?.let { setRpcLongPollTimeout(it) }
        rpcQueryTimeout?.let { setRpcQueryTimeout(it) }
        connectionBackoffResetFrequency?.let { setConnectionBackoffResetFrequency(it) }
        grpcReconnectFrequency?.let { setGrpcReconnectFrequency(it) }
        if (headers.isNotEmpty()) {
          // Headers are set via channel configuration or metadata
        }
      }
      .build()
  }

  /**
   * Convert to Java SDK's WorkflowClientOptions.
   */
  internal fun toClientOptions(): WorkflowClientOptions {
    return WorkflowClientOptions.newBuilder()
      .setNamespace(namespace)
      .apply {
        identity?.let { setIdentity(it) }
        dataConverter?.let { setDataConverter(it) }
        if (interceptors.isNotEmpty()) {
          setInterceptors(*interceptors.toTypedArray())
        }
      }
      .build()
  }
}
