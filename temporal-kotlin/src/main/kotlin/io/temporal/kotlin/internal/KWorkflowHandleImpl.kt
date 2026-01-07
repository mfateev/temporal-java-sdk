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

package io.temporal.kotlin.internal

// This file previously contained implementation classes for workflow handles.
// These have been moved directly into the handle classes in KWorkflowHandle.kt
// as part of the refactoring to convert interfaces to classes.
//
// The following classes have been consolidated:
// - WorkflowHandleImpl -> WorkflowHandle (open class)
// - KWorkflowHandleImpl -> KWorkflowHandle (open class)
// - KTypedWorkflowHandleImpl -> KTypedWorkflowHandle (class)
// - KUpdateHandleImpl -> KUpdateHandle (class)
//
// This file is kept for reference during the transition period and can be
// deleted once all dependent code has been updated.
