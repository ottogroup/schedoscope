/**
 * Copyright 2015 Otto (GmbH & Co KG)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.schedoscope.scheduler.rest

import org.schedoscope.scheduler.service.{ QueueStatusList, RunStatus, SchedoscopeCommandStatus, TransformationStatus, TransformationStatusList, ViewStatus, ViewStatusList, ViewTransformationStatus, FieldStatus }
import spray.json.{ DefaultJsonProtocol, JsonFormat }

/**
 * JSON SerDe for Schedoscope REST web service calls.
 */
object SchedoscopeJsonDataFormat extends DefaultJsonProtocol {
  implicit val runStatusFormat = jsonFormat5(RunStatus)
  implicit val actionStatusFormat = jsonFormat5(TransformationStatus)
  implicit val actionStatusListFormat = jsonFormat2(TransformationStatusList)
  implicit val schedoscopeCommandStatusFormat = jsonFormat4(SchedoscopeCommandStatus)
  implicit val viewTransformationStatusFormat: JsonFormat[ViewTransformationStatus] = lazyFormat(jsonFormat2(ViewTransformationStatus))
  implicit val viewStatusFormat: JsonFormat[ViewStatus] = lazyFormat(jsonFormat12(ViewStatus))
  implicit val fieldStatusFormat: JsonFormat[FieldStatus] = lazyFormat(jsonFormat3(FieldStatus))
  implicit val viewStatusListFormat = jsonFormat2(ViewStatusList)
  implicit val queueStatusListFormat = jsonFormat2(QueueStatusList)
}
