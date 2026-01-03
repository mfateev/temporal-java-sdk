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

package io.temporal.kotlin.testing

/**
 * Annotation to specify the initial time for a workflow test.
 * Overrides the initial time configured in the extension.
 *
 * Example:
 * ```kotlin
 * @Test
 * @WorkflowInitialTime("2024-06-15T12:00:00Z")
 * fun `test with specific initial time`(workflow: MyWorkflow) {
 *     // Test runs with June 15, 2024 as initial time
 * }
 * ```
 *
 * @property value ISO-8601 formatted timestamp for the initial time.
 *     Example: "2024-01-01T00:00:00Z"
 */
@Target(AnnotationTarget.FUNCTION)
@Retention(AnnotationRetention.RUNTIME)
public annotation class WorkflowInitialTime(val value: String)
