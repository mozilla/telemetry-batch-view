/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */
package com.mozilla.telemetry.pings

case class UntrustedModules(payload: Payload)

case class Payload(combined_stacks: CombinedStacks)

case class CombinedStacks(memory_map: Array[MemoryMapping], stacks: Array[Array[StackFrame]])

case class MemoryMapping(module_name: String, debug_id: String)

case class StackFrame(module_index: Long, module_offset: Long)
